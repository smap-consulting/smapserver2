package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.model.MCPToolDefinition;

/**
 * Registry of all available MCP tools
 */
public class MCPToolRegistry {

	private Map<String, IMCPTool> tools;

	public MCPToolRegistry() {
		this.tools = new HashMap<>();
	}

	/**
	 * Register a tool
	 * @param tool The tool to register
	 */
	public void register(IMCPTool tool) {
		MCPToolDefinition def = tool.getDefinition();
		tools.put(def.getName(), tool);
	}

	/**
	 * Get a tool by name
	 * @param name Tool name
	 * @return The tool, or null if not found
	 */
	public IMCPTool getTool(String name) {
		return tools.get(name);
	}

	/**
	 * Get all registered tool definitions
	 * @return List of tool definitions
	 */
	public List<MCPToolDefinition> getAllToolDefinitions() {
		List<MCPToolDefinition> definitions = new ArrayList<>();
		for (IMCPTool tool : tools.values()) {
			definitions.add(tool.getDefinition());
		}
		return definitions;
	}

	/**
	 * Check if a tool exists
	 * @param name Tool name
	 * @return true if the tool exists
	 */
	public boolean hasTool(String name) {
		return tools.containsKey(name);
	}
}
