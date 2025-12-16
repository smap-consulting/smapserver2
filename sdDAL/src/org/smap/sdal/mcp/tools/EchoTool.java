package org.smap.sdal.mcp.tools;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.smap.sdal.mcp.IMCPTool;
import org.smap.sdal.model.MCPToolDefinition;
import org.smap.sdal.model.MCPToolResult;

/**
 * Simple echo tool for testing MCP
 * Echoes back the message parameter
 */
public class EchoTool implements IMCPTool {

	@Override
	public MCPToolDefinition getDefinition() {
		Map<String, Object> inputSchema = new HashMap<>();
		inputSchema.put("type", "object");

		Map<String, Object> properties = new HashMap<>();
		Map<String, Object> messageProperty = new HashMap<>();
		messageProperty.put("type", "string");
		messageProperty.put("description", "The message to echo back");
		properties.put("message", messageProperty);

		inputSchema.put("properties", properties);
		inputSchema.put("required", new String[] { "message" });

		return new MCPToolDefinition(
			"echo",
			"Echoes back the provided message. Useful for testing the MCP server.",
			inputSchema
		);
	}

	@Override
	public MCPToolResult execute(Connection sd, String username, Map<String, Object> arguments) throws Exception {
		String message = (String) arguments.get("message");

		if (message == null || message.isEmpty()) {
			return new MCPToolResult("Error: message parameter is required", true);
		}

		return new MCPToolResult("Echo: " + message);
	}
}
