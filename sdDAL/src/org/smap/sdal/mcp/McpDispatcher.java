package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.managers.LogManager;
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
 */
public class McpDispatcher {

	private static Logger log = Logger.getLogger(McpDispatcher.class.getName());

	private final McpToolRegistry registry;
	private final LogManager lm = new LogManager();

	public McpDispatcher(McpToolRegistry registry) {
		this.registry = registry;
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

		String versionError = checkMeta(request);
		if(versionError != null) {
			return error(request.getId(), McpProtocol.INVALID_PARAMS, versionError);
		}

		try {
			switch(method) {
			case "ping":
				return ok(request.getId(), new HashMap<String, Object>());
			case "server/discover":
				return ok(request.getId(), discover());
			case "tools/list":
				return ok(request.getId(), toolsList(ctx));
			case "tools/call":
				return toolsCall(ctx, request);
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
	 * Every request carries its own protocol version and capabilities, and a request missing either
	 * is malformed rather than something to guess at.
	 */
	private String checkMeta(MCPRequest request) {

		Map<String, Object> meta = meta(request);
		Object version = meta.get(McpProtocol.META_PROTOCOL_VERSION);
		if(version == null) {
			return "Missing " + McpProtocol.META_PROTOCOL_VERSION + " in _meta";
		}
		if(!meta.containsKey(McpProtocol.META_CLIENT_CAPABILITIES)) {
			return "Missing " + McpProtocol.META_CLIENT_CAPABILITIES + " in _meta";
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> meta(MCPRequest request) {
		Map<String, Object> params = request.getParams();
		if(params != null && params.get("_meta") instanceof Map) {
			return (Map<String, Object>) params.get("_meta");
		}
		return new HashMap<>();
	}

	private Map<String, Object> discover() {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("protocolVersion", McpProtocol.VERSION);

		Map<String, Object> capabilities = new LinkedHashMap<>();
		Map<String, Object> tools = new LinkedHashMap<>();
		/*
		 * The tool list does not change under a caller, and telling a client otherwise would have
		 * it open a subscription stream this server does not yet answer.
		 */
		tools.put("listChanged", Boolean.FALSE);
		capabilities.put("tools", tools);
		result.put("capabilities", capabilities);
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
		if(!registry.permitted(ctx.sd, ctx, tool)) {
			log.info("MCP tool " + name + " refused for " + ctx.user + ", not in a permitted group");
			return error(request.getId(), McpProtocol.INVALID_PARAMS, "Unknown tool: " + name);
		}

		Map<String, Object> arguments = params.get("arguments") instanceof Map
				? (Map<String, Object>) params.get("arguments")
				: new HashMap<>();

		long started = System.currentTimeMillis();
		MCPToolResult result;
		try {
			result = tool.execute(ctx, arguments);
		} catch (ScopeRequired e) {
			throw e;
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

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("content", result.getContent());
		if(result.getStructuredContent() != null) {
			out.put("structuredContent", result.getStructuredContent());
		}
		out.put("isError", result.isError());
		return ok(request.getId(), out);
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
			if(result.isError()) {
				note.append(" failed");
			}
			lm.writeLog(ctx.sd, 0, ctx.user, LogManager.MCP, note.toString(), (int) ms, null);
		} catch (Exception e) {
			// Never fail a call because it could not be logged
			log.log(Level.WARNING, "Writing MCP audit entry", e);
		}
	}

	private MCPResponse ok(Object id, Map<String, Object> result) {

		// Every result says what kind it is, and identifies the server that produced it
		result.put("resultType", McpProtocol.RESULT_COMPLETE);

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
