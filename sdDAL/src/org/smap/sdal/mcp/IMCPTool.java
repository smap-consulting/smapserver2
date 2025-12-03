package org.smap.sdal.mcp;

import java.sql.Connection;
import java.util.Map;

import org.smap.sdal.model.MCPToolDefinition;
import org.smap.sdal.model.MCPToolResult;

/**
 * Interface for all MCP tools
 */
public interface IMCPTool {

	/**
	 * Get the tool definition (name, description, input schema)
	 * @return Tool definition
	 */
	MCPToolDefinition getDefinition();

	/**
	 * Execute the tool with the given arguments
	 * @param sd Database connection
	 * @param username The authenticated username
	 * @param arguments Tool arguments
	 * @return Tool execution result
	 * @throws Exception if execution fails
	 */
	MCPToolResult execute(Connection sd, String username, Map<String, Object> arguments) throws Exception;
}
