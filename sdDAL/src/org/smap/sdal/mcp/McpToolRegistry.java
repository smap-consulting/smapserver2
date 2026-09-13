package org.smap.sdal.mcp;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

/*
 * The tools this server offers, and who may see each one.
 */
public class McpToolRegistry {

	private static Logger log = Logger.getLogger(McpToolRegistry.class.getName());

	/*
	 * Insertion ordered, because the specification asks for a deterministic tools/list: a client
	 * that gets the same tools in a different order each time cannot cache the list, and the model
	 * behind it loses its prompt cache too.
	 */
	private final Map<String, McpTool> tools = new LinkedHashMap<>();

	public void register(McpTool tool) {

		/*
		 * The reversibility rule, enforced at startup rather than trusted.  A mutating tool that
		 * cannot say how it is undone does not get registered, so the rule cannot be forgotten by
		 * whoever adds the fiftieth tool.
		 */
		if(tool.isMutating() && (tool.getReversal() == null || tool.getReversal().isEmpty())) {
			throw new IllegalStateException("MCP tool " + tool.getName()
					+ " changes data but does not declare how that is reversed");
		}
		if(tool.getRequiredScope() == null || !MCPScope.isKnown(tool.getRequiredScope())) {
			throw new IllegalStateException("MCP tool " + tool.getName()
					+ " does not declare a known scope");
		}

		/*
		 * A tool that only reads has nothing to confirm, so asking would be a mistake rather than
		 * caution: it would train whoever answers to click through prompts that never mattered, and
		 * the prompts that do matter are the ones that follow.  Caught at startup for the same
		 * reason the reversal is - a rule enforced only by memory is one that erodes.
		 */
		if(!tool.isMutating() && tool.getConfirmation() != McpTool.Confirmation.NONE) {
			throw new IllegalStateException("MCP tool " + tool.getName()
					+ " reads but asks for confirmation");
		}
		tools.put(tool.getName(), tool);
	}

	public McpTool get(String name) {
		return tools.get(name);
	}

	/*
	 * The tools this caller is shown.
	 *
	 * Filtered by the caller's security groups and deliberately not by their token's scopes, because
	 * the two refusals are not alike. A group is a property of the person: no amount of
	 * re-authorising will give an enumerator an administrator's rights, so a tool they can never run
	 * is better never seen. A scope is a property of the token and is meant to be escalated - only
	 * smap:read is ever advertised, and everything else is reached by asking for it.
	 *
	 * Filtering the listing by scope as well made that impossible. A tool the client cannot see is a
	 * tool it never calls, so it never receives the insufficient_scope challenge that exists to tell
	 * it what to ask for, and a session could sit on a read-only token with no way to discover that
	 * writing was available at all. The listing therefore shows what the person may do, and the call
	 * decides what this token may do.
	 */
	public List<McpTool> visibleTo(Connection sd, McpToolContext ctx) {

		List<McpTool> visible = new ArrayList<>();
		for(McpTool tool : tools.values()) {
			if(grantable(ctx, tool) && inPermittedGroup(sd, ctx, tool)) {
				visible.add(tool);
			}
		}
		return visible;
	}

	/*
	 * Whether this server could ever issue the scope this tool needs.
	 *
	 * The paragraph above is about scopes a client can escalate to, and it holds - but smap:access is
	 * not always one of them.  With mcp_allow_access off the scope is not advertised and is stripped
	 * from any request naming it, so a client shown one of those tools would call it, receive a
	 * challenge naming smap:access, go to the metadata to find out how to ask, and find it absent.
	 * That is the same dead end that advertising read alone produced for the write tools.
	 *
	 * So a tool whose scope this server will not issue is hidden, for the reason given above for
	 * groups: a tool that can never be run is better never seen.
	 */
	private boolean grantable(McpToolContext ctx, McpTool tool) {
		return MCPScope.advertised(ctx.accessAllowed).contains(tool.getRequiredScope());
	}

	/* Both questions, for the call path, which has to answer them separately to say which failed */
	public boolean permitted(Connection sd, McpToolContext ctx, McpTool tool) {
		return grantable(ctx, tool) && ctx.hasScope(tool.getRequiredScope())
				&& inPermittedGroup(sd, ctx, tool);
	}

	public boolean isGrantable(McpToolContext ctx, McpTool tool) {
		return grantable(ctx, tool);
	}

	public boolean inPermittedGroup(Connection sd, McpToolContext ctx, McpTool tool) {

		List<String> groups = tool.getRequiredGroups();
		if(groups == null || groups.isEmpty()) {
			return true;
		}
		for(String group : groups) {
			try {
				if(GeneralUtilityMethods.hasSecurityGroup(sd, ctx.user, groupId(group))) {
					return true;
				}
			} catch (Exception e) {
				// A group that cannot be checked is a group the caller does not have
				log.warning("Checking group " + group + " for " + ctx.user + ": " + e.getMessage());
			}
		}
		return false;
	}

	private int groupId(String group) {
		return org.smap.sdal.Utilities.Authorise.getGroupId(group);
	}

	public int size() {
		return tools.size();
	}
}
