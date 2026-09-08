package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.McpConfirmationManager;
import org.smap.sdal.model.MCPError;
import org.smap.sdal.model.MCPRequest;
import org.smap.sdal.model.MCPResponse;
import org.smap.sdal.model.MCPToolResult;

/*
 * Routes one MCP request to one tool.
 *
 * Replaces MCPManager, which held a static registry and a mutable "initialized" flag shared by
 * every user of the JVM.  Under the 2026-07-28 protocol there is nothing to hold: no handshake, no
 * session, and every request carries its own protocol version and capabilities.  So this class has
 * no mutable state at all, which is also what lets a Smap server sit behind a plain load balancer.
 *
 * Older revisions are still answered.  They open with an initialize handshake and then never
 * mention the protocol version again, so a request without it in _meta is an older client rather
 * than a malformed one.  The specification does say a 2026-07-28 request missing that field must be
 * refused, but there is no way to tell the two apart, and refusing both would mean refusing every
 * client that exists today.  Revisit when the twelve month deprecation window closes.
 */
public class McpDispatcher {

	private static Logger log = Logger.getLogger(McpDispatcher.class.getName());

	private final McpToolRegistry registry;
	private final McpResources resources;
	private final LogManager lm = new LogManager();

	public McpDispatcher(McpToolRegistry registry) {
		this.registry = registry;
		this.resources = new McpResources(registry);
	}

	/*
	 * Raised when the caller's token is missing the scope a tool needs.
	 *
	 * Deliberately not a JSON-RPC error.  The specification wants an HTTP 403 carrying a
	 * WWW-Authenticate challenge that names the scope, because that is what tells the client to step
	 * up rather than to give up.  Only the resource layer can set that, so it is signalled up.
	 */
	public static class ScopeRequired extends RuntimeException {
		private static final long serialVersionUID = 1L;
		public final String scope;
		public ScopeRequired(String scope) {
			super("Scope required: " + scope);
			this.scope = scope;
		}
	}

	public MCPResponse process(McpToolContext ctx, MCPRequest request) {

		if(request == null || !"2.0".equals(request.getJsonrpc())) {
			return error(null, McpProtocol.INVALID_REQUEST, "Not a JSON-RPC 2.0 request");
		}
		String method = request.getMethod();
		if(method == null) {
			return error(request.getId(), McpProtocol.INVALID_REQUEST, "A method is required");
		}

		/*
		 * A notification has no id and gets no reply.  Under a stateless protocol there is nothing
		 * for one to change, so they are accepted and dropped.
		 */
		if(request.getId() == null) {
			log.fine("MCP notification ignored: " + method);
			return null;
		}



		try {
			switch(method) {
			case "initialize":
				return ok(request.getId(), initialize(request));
			case "ping":
				return ok(request.getId(), new HashMap<String, Object>());
			case "server/discover":
				return ok(request.getId(), discover());
			case "tools/list":
				return ok(request.getId(), toolsList(ctx));
			case "tools/call":
				return toolsCall(ctx, request);
			case "resources/list":
				return ok(request.getId(), map("resources", resources.list(ctx)));
			case "resources/templates/list":
				return ok(request.getId(), map("resourceTemplates", resources.templates()));
			case "resources/read":
				return resourcesRead(ctx, request);
			case "completion/complete":
				return ok(request.getId(), complete(ctx, request));
			default:
				return error(request.getId(), McpProtocol.METHOD_NOT_FOUND, "Unknown method: " + method);
			}
		} catch (ScopeRequired e) {
			throw e;		// Handled by the resource layer as an HTTP 403
		} catch (Exception e) {
			/*
			 * The detail goes to the log, never to the client.  An exception message from deep in a
			 * manager can carry a table name, a query or a row's contents.
			 */
			String reference = Long.toHexString(System.nanoTime());
			log.log(Level.SEVERE, "MCP " + method + " failed, reference " + reference, e);
			return error(request.getId(), McpProtocol.INTERNAL_ERROR,
					"The request could not be completed. Reference " + reference);
		}
	}

	/*
	 * The handshake older revisions open with.  2026-07-28 has no equivalent - there is nothing to
	 * establish, because every request stands alone - so a client that sends this is telling us
	 * which older revision it speaks.
	 */
	private Map<String, Object> initialize(MCPRequest request) {

		String asked = null;
		Map<String, Object> params = request.getParams();
		if(params != null && params.get("protocolVersion") != null) {
			asked = params.get("protocolVersion").toString();
		}

		/*
		 * Answer in the version the client asked for when we speak it, so it does not have to
		 * downgrade.  Otherwise name ours and let the client decide whether it can continue.
		 */
		String agreed = McpProtocol.isSupported(asked) ? asked : McpProtocol.VERSION;
		if(asked != null && !agreed.equals(asked)) {
			log.info("MCP client asked for protocol " + asked + ", answering with " + agreed);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("protocolVersion", agreed);
		result.put("capabilities", capabilities());

		Map<String, Object> serverInfo = new LinkedHashMap<>();
		serverInfo.put("name", McpProtocol.SERVER_NAME);
		serverInfo.put("version", McpProtocol.VERSION);
		result.put("serverInfo", serverInfo);

		result.put("instructions", "Smap survey server. Call whoami first to see which user this "
				+ "connection acts as and what it is allowed to do. Use survey_list to find a "
				+ "survey id before reading its data.");
		return result;
	}

	private Map<String, Object> map(String key, Object value) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put(key, value);
		return m;
	}

	/*
	 * Reading a resource goes through the same access check as a tool: the survey has to be one the
	 * caller could have listed.  A resource is a different way in, not a different set of rights.
	 */
	private MCPResponse resourcesRead(McpToolContext ctx, MCPRequest request) throws Exception {

		Map<String, Object> params = request.getParams();
		String uri = params == null ? null : (String) params.get("uri");
		if(uri == null) {
			return error(request.getId(), McpProtocol.INVALID_PARAMS, "A uri is required");
		}
		try {
			McpResources.Content content = resources.read(ctx, uri);
			return ok(request.getId(), map("contents",
					java.util.Collections.singletonList(content.toMap())));
		} catch (IllegalArgumentException e) {
			// A resource that is unknown and one the caller may not see answer the same way
			return error(request.getId(), McpProtocol.INVALID_PARAMS, e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> complete(McpToolContext ctx, MCPRequest request) {

		Map<String, Object> params = request.getParams();
		String name = null;
		String value = null;
		if(params != null && params.get("argument") instanceof Map) {
			Map<String, Object> argument = (Map<String, Object>) params.get("argument");
			name = (String) argument.get("name");
			value = (String) argument.get("value");
		}

		List<String> values = resources.complete(ctx, name, value);
		Map<String, Object> completion = new LinkedHashMap<>();
		completion.put("values", values);
		completion.put("total", values.size());
		completion.put("hasMore", Boolean.FALSE);
		return map("completion", completion);
	}

	private Map<String, Object> capabilities() {
		Map<String, Object> capabilities = new LinkedHashMap<>();
		Map<String, Object> tools = new LinkedHashMap<>();
		/*
		 * The tool list does not change under a caller, and saying otherwise would have a client
		 * open a subscription stream this server does not yet answer.
		 */
		tools.put("listChanged", Boolean.FALSE);
		capabilities.put("tools", tools);

		Map<String, Object> resourceCapability = new LinkedHashMap<>();
		/*
		 * No subscriptions and no list changed notifications.  Both need a stream this server does
		 * not yet answer, and claiming them would have a client open one and wait.
		 */
		resourceCapability.put("subscribe", Boolean.FALSE);
		resourceCapability.put("listChanged", Boolean.FALSE);
		capabilities.put("resources", resourceCapability);

		capabilities.put("completions", new LinkedHashMap<String, Object>());
		return capabilities;
	}

	/*
	 * The 2026-07-28 replacement for initialize.  Optional, and asks nothing of the client, because
	 * a stateless protocol has no handshake to complete.
	 */
	private Map<String, Object> discover() {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("protocolVersion", McpProtocol.VERSION);
		result.put("capabilities", capabilities());
		return result;
	}

	private Map<String, Object> toolsList(McpToolContext ctx) {

		List<Map<String, Object>> defs = new ArrayList<>();
		for(McpTool tool : registry.visibleTo(ctx.sd, ctx)) {
			defs.add(describe(tool));
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("tools", defs);
		/*
		 * No ttlMs and no cacheScope.  This listing is filtered by the caller's groups and by the
		 * scopes on their token, so it is not the same answer for two different callers and there
		 * is nothing safe to cache in front of it.  Omitting the fields is what tells a client that.
		 */
		return result;
	}

	private Map<String, Object> describe(McpTool tool) {

		Map<String, Object> def = new LinkedHashMap<>();
		def.put("name", tool.getName());
		if(tool.getTitle() != null) {
			def.put("title", tool.getTitle());
		}
		def.put("description", tool.getDescription());
		def.put("inputSchema", tool.getInputSchema());
		if(tool.getOutputSchema() != null) {
			def.put("outputSchema", tool.getOutputSchema());
		}
		if(tool.getAnnotations() != null) {
			def.put("annotations", tool.getAnnotations());
		}
		return def;
	}

	@SuppressWarnings("unchecked")
	private MCPResponse toolsCall(McpToolContext ctx, MCPRequest request) throws Exception {

		Map<String, Object> params = request.getParams();
		if(params == null) {
			return error(request.getId(), McpProtocol.INVALID_PARAMS, "Parameters are required");
		}
		String name = (String) params.get("name");
		if(name == null || name.isEmpty()) {
			return error(request.getId(), McpProtocol.INVALID_PARAMS, "A tool name is required");
		}

		McpTool tool = registry.get(name);
		if(tool == null) {
			return error(request.getId(), McpProtocol.INVALID_PARAMS, "Unknown tool: " + name);
		}

		/*
		 * Two different refusals, and the difference matters to the client.
		 *
		 * Missing scope is recoverable: the client can step up and come back, so it gets a 403 with
		 * a challenge naming what to ask for.  Missing group is not - no amount of re-authorising
		 * will give an enumerator an administrator's rights - so the tool is reported unknown,
		 * which is consistent with it never having appeared in tools/list.
		 */
		if(!ctx.hasScope(tool.getRequiredScope())) {
			throw new ScopeRequired(tool.getRequiredScope());
		}
		if(!registry.inPermittedGroup(ctx.sd, ctx, tool)) {
			log.info("MCP tool " + name + " refused for " + ctx.user + ", not in a permitted group");
			return error(request.getId(), McpProtocol.INVALID_PARAMS, "Unknown tool: " + name);
		}

		Map<String, Object> arguments = params.get("arguments") instanceof Map
				? (Map<String, Object>) params.get("arguments")
				: new HashMap<>();

		/*
		 * A retry carrying answers to something this tool asked on an earlier call.
		 *
		 * There is no session, so the two calls are related only by what the client sends back: the
		 * same arguments, the answers, and the handle.  The handle is checked here rather than in
		 * the tool, so no tool can forget to, and it is checked against this call - the same person,
		 * unexpired, same tool, same arguments - so a confirmation shown for one thing cannot be
		 * redeemed against another.
		 */
		ctx.clientCapabilities = request.getClientCapabilities();
		ctx.inputResponses = params.get("inputResponses") instanceof Map
				? (Map<String, Object>) params.get("inputResponses")
				: null;

		Object state = params.get("requestState");
		if(state != null) {
			McpConfirmationManager cm = new McpConfirmationManager();
			McpConfirmationManager.Outcome outcome =
					cm.consume(ctx.sd, state.toString(), ctx.uId, name, arguments);
			ctx.confirmed = outcome == McpConfirmationManager.Outcome.VALID;
			if(!ctx.confirmed) {
				/*
				 * Told plainly, because every one of these is recoverable by asking again, and a
				 * client that cannot tell why it was refused will retry the same way.
				 */
				log.warning("MCP confirmation rejected for " + ctx.user + " on " + name
						+ ": " + outcome);
				return ok(request.getId(), toolResult(new MCPToolResult(
						"That confirmation is no longer valid (" + reason(outcome)
						+ "). Run the tool again to be asked afresh.", true), tool));
			}
		}

		long started = System.currentTimeMillis();
		MCPToolResult result;
		try {
			result = tool.execute(ctx, arguments);
		} catch (ScopeRequired e) {
			throw e;
		} catch (ApplicationException e) {
			/*
			 * The message of an ApplicationException is written to be read by whoever asked - an
			 * unknown question name, a value out of range - so it is handed back as it is.  A tool
			 * raises one deliberately; anything else it throws falls to the branch below and becomes
			 * a reference, because a message nobody wrote for this audience is as likely to describe
			 * the schema as the mistake.
			 */
			result = new MCPToolResult(e.getMessage(), true);
		} catch (Exception e) {
			/*
			 * A business failure is reported as a tool result rather than a protocol error, because
			 * the model can act on it: fix the argument and try again.  The detail still only goes
			 * to the log.
			 */
			String reference = Long.toHexString(System.nanoTime());
			log.log(Level.SEVERE, "MCP tool " + name + " failed, reference " + reference, e);
			result = new MCPToolResult("The tool could not complete. Reference " + reference, true);
		}

		audit(ctx, name, arguments, result, System.currentTimeMillis() - started);

		/*
		 * The tool wants to ask before it acts.  The call ends here and the client is expected to
		 * put the question to somebody and call again with the answer and the handle.
		 *
		 * The handle is stored rather than signed into the response.  What it decides is whether a
		 * thing is done or refused, which is the case the specification says must be protected from
		 * the client, and a stored row can also be spent: an approval to send two emails must not be
		 * redeemable twice, which signing alone does not give.
		 */
		if(result.isInputRequired()) {
			McpConfirmationManager cm = new McpConfirmationManager();
			String stateId = cm.create(ctx.sd, ctx.uId, ctx.clientId, name, arguments);

			Map<String, Object> out = new LinkedHashMap<>();
			out.put("inputRequests", result.getInputRequests());
			out.put("requestState", stateId);
			return inputRequired(request.getId(), out);
		}

		return ok(request.getId(), toolResult(result, tool));
	}

	/* The ordinary shape of a finished tool call */
	private Map<String, Object> toolResult(MCPToolResult result, McpTool tool) {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("content", result.getContent());
		if(result.getStructuredContent() != null) {
			out.put("structuredContent",
					asObject(tool == null ? "unknown" : tool.getName(), result.getStructuredContent()));
		}
		out.put("isError", result.isError());
		return out;
	}

	/* Why a confirmation was refused, in words a client can act on */
	private String reason(McpConfirmationManager.Outcome outcome) {
		switch(outcome) {
			case EXPIRED: return "it expired";
			case WRONG_USER: return "it was issued to somebody else";
			case WRONG_CALL: return "it was for a different call";
			default: return "it is not recognised, or has already been used";
		}
	}

	/*
	 * Structured content is always sent as a JSON object.
	 *
	 * 2026-07-28 permits any JSON value there, arrays included, but every revision before it
	 * requires an object and clients still negotiate those.  One that does rejects the entire
	 * response before the caller sees any of it, which reads as the tool being broken rather than
	 * as a disagreement about the protocol.  A tool that hands back something else is wrapped here
	 * rather than left to fail on the wire, and the wrapping is logged so it gets fixed at source.
	 */
	private Object asObject(String toolName, Object structured) {
		if(structured instanceof Map) {
			return structured;
		}
		log.warning("MCP tool " + toolName + " returned structured content that is not an object; "
				+ "wrapping it. Give the tool an outputSchema and return an object.");
		Map<String, Object> wrapped = new LinkedHashMap<>();
		wrapped.put("result", structured);
		return wrapped;
	}

	/*
	 * One line per tool call in the application log.
	 *
	 * Argument names but not argument values: the values are the caller's data, and a log that
	 * records them turns every query into a second copy of what was queried.  The names are enough
	 * to see what an agent was doing.
	 */
	private void audit(McpToolContext ctx, String name, Map<String, Object> arguments,
			MCPToolResult result, long ms) {
		try {
			StringBuilder note = new StringBuilder("tool ").append(name);
			if(!arguments.isEmpty()) {
				note.append(" (").append(String.join(", ", arguments.keySet())).append(")");
			}
			/*
			 * Which application asked, not only which person it asked as.  Without it the log says a
			 * user did something at a time they may well have been asleep, and cannot distinguish
			 * one agent from another when a person has authorised several.
			 */
			note.append(ctx.clientId == null
					? " via a token minted in the console"
					: " via client " + ctx.clientId);
			if(result.isError()) {
				note.append(" failed");
			}
			lm.writeLog(ctx.sd, 0, ctx.user, LogManager.MCP, note.toString(), (int) ms, null);
		} catch (Exception e) {
			// Never fail a call because it could not be logged
			log.log(Level.WARNING, "Writing MCP audit entry", e);
		}
	}

	/*
	 * A result that is not an answer but a question.  Same envelope, different resultType, so a
	 * client that understands 2026-07-28 knows to gather the input and call again.
	 */
	private MCPResponse inputRequired(Object id, Map<String, Object> result) {
		return respond(id, result, McpProtocol.RESULT_INPUT_REQUIRED);
	}

	private MCPResponse ok(Object id, Map<String, Object> result) {
		return respond(id, result, McpProtocol.RESULT_COMPLETE);
	}

	private MCPResponse respond(Object id, Map<String, Object> result, String resultType) {

		// Every result says what kind it is, and identifies the server that produced it
		result.put("resultType", resultType);

		Map<String, Object> serverInfo = new LinkedHashMap<>();
		serverInfo.put("name", McpProtocol.SERVER_NAME);
		serverInfo.put("version", McpProtocol.VERSION);

		Map<String, Object> meta = new LinkedHashMap<>();
		meta.put(McpProtocol.META_SERVER_INFO, serverInfo);
		result.put("_meta", meta);

		return new MCPResponse(id, result);
	}

	private MCPResponse error(Object id, int code, String message) {
		return new MCPResponse(id, new MCPError(code, message));
	}
}
