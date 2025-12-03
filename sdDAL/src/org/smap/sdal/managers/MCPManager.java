package org.smap.sdal.managers;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.smap.sdal.mcp.IMCPTool;
import org.smap.sdal.mcp.MCPToolRegistry;
import org.smap.sdal.model.MCPError;
import org.smap.sdal.model.MCPRequest;
import org.smap.sdal.model.MCPResponse;
import org.smap.sdal.model.MCPToolDefinition;
import org.smap.sdal.model.MCPToolResult;

/*****************************************************************************

This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

 ******************************************************************************/

/**
 * Manager for handling Model Context Protocol (MCP) requests
 * Implements JSON-RPC 2.0 protocol with MCP-specific methods
 */
public class MCPManager {

	private static Logger log = Logger.getLogger(MCPManager.class.getName());
	private MCPToolRegistry toolRegistry;

	public MCPManager() {
		this.toolRegistry = new MCPToolRegistry();
	}

	/**
	 * Get the tool registry for registering tools
	 * @return Tool registry
	 */
	public MCPToolRegistry getToolRegistry() {
		return toolRegistry;
	}

	/**
	 * Process an MCP request
	 * @param sd Database connection
	 * @param username Authenticated username
	 * @param request The MCP/JSON-RPC request
	 * @return MCP/JSON-RPC response
	 */
	public MCPResponse processRequest(Connection sd, String username, MCPRequest request) {

		// Validate JSON-RPC version
		if (request.getJsonrpc() == null || !request.getJsonrpc().equals("2.0")) {
			return new MCPResponse(
				request.getId(),
				new MCPError(MCPError.INVALID_REQUEST, "Invalid JSON-RPC version")
			);
		}

		// Route to the appropriate method handler
		String method = request.getMethod();
		if (method == null) {
			return new MCPResponse(
				request.getId(),
				new MCPError(MCPError.INVALID_REQUEST, "Method is required")
			);
		}

		try {
			switch (method) {
				case "tools/list":
					return handleToolsList(request);

				case "tools/call":
					return handleToolsCall(sd, username, request);

				default:
					return new MCPResponse(
						request.getId(),
						new MCPError(MCPError.METHOD_NOT_FOUND, "Method not found: " + method)
					);
			}
		} catch (Exception e) {
			log.severe("Error processing MCP request: " + e.getMessage());
			e.printStackTrace();
			return new MCPResponse(
				request.getId(),
				new MCPError(MCPError.INTERNAL_ERROR, "Internal error: " + e.getMessage())
			);
		}
	}

	/**
	 * Handle tools/list request
	 * @param request The request
	 * @return Response with list of tools
	 */
	private MCPResponse handleToolsList(MCPRequest request) {
		Map<String, Object> result = new HashMap<>();
		result.put("tools", toolRegistry.getAllToolDefinitions());

		return new MCPResponse(request.getId(), result);
	}

	/**
	 * Handle tools/call request
	 * @param sd Database connection
	 * @param username Authenticated username
	 * @param request The request
	 * @return Response with tool execution result
	 */
	private MCPResponse handleToolsCall(Connection sd, String username, MCPRequest request) {
		Map<String, Object> params = request.getParams();

		if (params == null) {
			return new MCPResponse(
				request.getId(),
				new MCPError(MCPError.INVALID_PARAMS, "Parameters are required")
			);
		}

		String toolName = (String) params.get("name");
		if (toolName == null || toolName.isEmpty()) {
			return new MCPResponse(
				request.getId(),
				new MCPError(MCPError.INVALID_PARAMS, "Tool name is required")
			);
		}

		// Get the tool
		IMCPTool tool = toolRegistry.getTool(toolName);
		if (tool == null) {
			return new MCPResponse(
				request.getId(),
				new MCPError(MCPError.INVALID_PARAMS, "Unknown tool: " + toolName)
			);
		}

		// Get arguments
		@SuppressWarnings("unchecked")
		Map<String, Object> arguments = (Map<String, Object>) params.get("arguments");
		if (arguments == null) {
			arguments = new HashMap<>();
		}

		// Execute the tool
		try {
			MCPToolResult toolResult = tool.execute(sd, username, arguments);
			return new MCPResponse(request.getId(), toolResult);
		} catch (Exception e) {
			log.severe("Error executing tool " + toolName + ": " + e.getMessage());
			e.printStackTrace();

			// Return error as tool result with isError flag
			MCPToolResult errorResult = new MCPToolResult(
				"Tool execution failed: " + e.getMessage(),
				true
			);
			return new MCPResponse(request.getId(), errorResult);
		}
	}
}
