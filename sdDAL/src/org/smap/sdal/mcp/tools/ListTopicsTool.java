package org.smap.sdal.mcp.tools;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.smap.sdal.mcp.IMCPTool;
import org.smap.sdal.managers.BundleManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Bundle;
import org.smap.sdal.model.MCPToolDefinition;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/**
 * Tool to list surveys accessible by the authenticated user
 */
public class ListTopicsTool implements IMCPTool {

	@Override
	public MCPToolDefinition getDefinition() {
		Map<String, Object> inputSchema = new HashMap<>();
		inputSchema.put("type", "object");

		Map<String, Object> properties = new HashMap<>();

		inputSchema.put("properties", properties);

		return new MCPToolDefinition(
			"list_topics",
			"Lists all topics (also called bundles) accessible by the authenticated user.",
			inputSchema
		);
	}

	@Override
	public MCPToolResult execute(Connection sd, String username, Map<String, Object> arguments) throws Exception {

		// Get surveys using BundleManager
		// Note: Using null ResourceBundle and UTC timezone for MCP context
		BundleManager bm = new BundleManager(null);		
		ArrayList<Bundle> topics = bm.getBundles(sd, username);

		// Format the response
		StringBuilder result = new StringBuilder();
		result.append("Found ").append(topics.size()).append(" topic(s):\n\n");

		if (topics.isEmpty()) {
			result.append("No topics found.");
		} else {
			for (Bundle topic : topics) {
				result.append("- ").append(topic.name);
				result.append(" (Description: ").append(topic.description);
				result.append(")\n");
			}
		}

		return new MCPToolResult(result.toString());
	}
}
