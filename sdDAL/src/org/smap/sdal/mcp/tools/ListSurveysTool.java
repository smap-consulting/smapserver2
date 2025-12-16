package org.smap.sdal.mcp.tools;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.smap.sdal.mcp.IMCPTool;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.MCPToolDefinition;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/**
 * Tool to list surveys accessible by the authenticated user
 */
public class ListSurveysTool implements IMCPTool {

	@Override
	public MCPToolDefinition getDefinition() {
		Map<String, Object> inputSchema = new HashMap<>();
		inputSchema.put("type", "object");

		Map<String, Object> properties = new HashMap<>();

		// Optional project_id parameter
		Map<String, Object> projectIdProperty = new HashMap<>();
		projectIdProperty.put("type", "integer");
		projectIdProperty.put("description", "Optional project ID to filter surveys. Set to 0 or omit to get all surveys.");
		properties.put("project_id", projectIdProperty);

		// Optional include_deleted parameter
		Map<String, Object> includeDeletedProperty = new HashMap<>();
		includeDeletedProperty.put("type", "boolean");
		includeDeletedProperty.put("description", "Include deleted surveys in the results. Default is false.");
		properties.put("include_deleted", includeDeletedProperty);

		inputSchema.put("properties", properties);

		return new MCPToolDefinition(
			"list_surveys",
			"Lists all surveys accessible by the authenticated user. Can optionally filter by project ID.",
			inputSchema
		);
	}

	@Override
	public MCPToolResult execute(Connection sd, String username, Map<String, Object> arguments) throws Exception {
		// Parse arguments
		int projectId = 0;
		if (arguments.get("project_id") != null) {
			Object projectIdObj = arguments.get("project_id");
			if (projectIdObj instanceof Number) {
				projectId = ((Number) projectIdObj).intValue();
			}
		}

		boolean includeDeleted = false;
		if (arguments.get("include_deleted") != null) {
			Object includeDeletedObj = arguments.get("include_deleted");
			if (includeDeletedObj instanceof Boolean) {
				includeDeleted = (Boolean) includeDeletedObj;
			}
		}

		// Get surveys using SurveyManager
		// Note: Using null ResourceBundle and UTC timezone for MCP context
		SurveyManager sm = new SurveyManager(null, "UTC");
		ArrayList<Survey> surveys = sm.getSurveys(
			sd,
			username,
			includeDeleted,  // getDeleted
			false,           // getBlocked
			projectId,       // projectId (0 = all projects)
			false,           // superUser (we'll use regular user permissions)
			false,           // onlyGroup
			false,           // getGroupDetails
			false,           // onlyDataSurvey
			false,           // links
			null             // urlprefix
		);

		// Format the response
		StringBuilder result = new StringBuilder();
		result.append("Found ").append(surveys.size()).append(" survey(s):\n\n");

		if (surveys.isEmpty()) {
			result.append("No surveys found.");
		} else {
			for (Survey survey : surveys) {
				result.append("- ").append(survey.getDisplayName());
				result.append(" (ID: ").append(survey.getId());
				result.append(", Project: ").append(survey.getProjectName());
				result.append(", Ident: ").append(survey.getIdent());
				if (survey.getDeleted()) {
					result.append(", DELETED");
				}
				if (survey.getBlocked()) {
					result.append(", BLOCKED");
				}
				result.append(")\n");
			}
		}

		return new MCPToolResult(result.toString());
	}
}
