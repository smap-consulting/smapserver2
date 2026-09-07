package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.Map;

import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpProtocol;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.ServerData;

/*
 * What this server is and what limits apply to this connection.
 *
 * Deliberately narrow.  The server settings hold mail relay credentials, map keys and SharePoint
 * certificates; none of that belongs in an answer to a client, and a tool that returned the whole
 * settings object would leak them the first time somebody asked an open question.  Only the things
 * that change how a caller should behave are reported.
 */
public class ServerInfoTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "server_info";
	}

	@Override
	public String getTitle() {
		return "Server information";
	}

	@Override
	public String getDescription() {
		return "Reports the Smap version, the MCP protocol revision in use, and the limits that "
				+ "apply to this connection, such as the maximum number of rows a tool will return.";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> props = new LinkedHashMap<>();
		props.put("version", property("string", "Smap version"));
		props.put("protocol_version", property("string", "MCP revision this server implements"));
		props.put("max_rows", property("integer", "Most rows any one tool will return, 0 for no limit"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", props);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		ServerData server = new ServerManager().getServer(ctx.sd, ctx.localisation);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("version", server.version);
		data.put("protocol_version", McpProtocol.VERSION);
		data.put("max_rows", ctx.maxRows);

		StringBuilder text = new StringBuilder();
		text.append("Smap version ").append(server.version)
				.append(", MCP revision ").append(McpProtocol.VERSION).append(".\n");
		if(ctx.maxRows > 0) {
			text.append("Tools return at most ").append(ctx.maxRows).append(" rows per call.");
		} else {
			text.append("No row limit is set on this server.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
