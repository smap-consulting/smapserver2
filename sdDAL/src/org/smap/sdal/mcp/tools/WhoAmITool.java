package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * Who the caller is and what this connection can do.
 *
 * Replaces the echo tool the prototype carried.  It serves the same purpose - a cheap way to prove
 * the connection works - while answering the question a model actually needs answered before it
 * plans anything: whose data am I looking at, and what am I allowed to do with it.
 */
public class WhoAmITool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "whoami";
	}

	@Override
	public String getTitle() {
		return "Who am I";
	}

	@Override
	public String getDescription() {
		return "Returns the Smap user this connection acts as, their organisation, and the "
				+ "permissions granted to it. Call this first if you are unsure what you can do.";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("user", property("string", "The Smap user ident"));
		properties.put("name", property("string", "Their display name"));
		properties.put("organisation", property("string", "The organisation this grant is bound to"));
		Map<String, Object> scopes = new LinkedHashMap<>();
		scopes.put("type", "array");
		scopes.put("items", property("string", "A granted scope"));
		properties.put("scopes", scopes);
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String name = GeneralUtilityMethods.getUserNameFromIdent(ctx.sd, ctx.user);
		String organisation = GeneralUtilityMethods.getOrganisationName(ctx.sd, ctx.oId);
		List<String> scopes = new ArrayList<>(MCPScope.parse(ctx.scope));

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("user", ctx.user);
		data.put("name", name);
		data.put("organisation", organisation);
		data.put("scopes", scopes);

		StringBuilder text = new StringBuilder();
		text.append("You are acting as ").append(ctx.user);
		if(name != null) {
			text.append(" (").append(name).append(")");
		}
		text.append(" in the organisation ").append(organisation).append(".\n");
		text.append("Permissions granted to this connection: ").append(String.join(", ", scopes));

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
