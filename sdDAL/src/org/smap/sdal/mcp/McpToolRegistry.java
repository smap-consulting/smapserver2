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
		tools.put(tool.getName(), tool);
	}

	public McpTool get(String name) {
		return tools.get(name);
	}

	/*
	 * The tools this caller may actually run.
	 *
	 * Effective permission is the user's security groups intersected with the token's scopes, so a
	 * token naming a scope its holder's groups do not support shows nothing extra, and a user whose
	 * groups would allow a tool still cannot see it through a token that was not granted the scope.
	 */
	public List<McpTool> visibleTo(Connection sd, McpToolContext ctx) {

		List<McpTool> visible = new ArrayList<>();
		for(McpTool tool : tools.values()) {
			if(permitted(sd, ctx, tool)) {
				visible.add(tool);
			}
		}
		return visible;
	}

	public boolean permitted(Connection sd, McpToolContext ctx, McpTool tool) {

		if(!ctx.hasScope(tool.getRequiredScope())) {
			return false;
		}
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
