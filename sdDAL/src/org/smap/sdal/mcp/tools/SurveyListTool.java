package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * The surveys this user can see.
 *
 * SurveyManager.getSurveys filters by the caller's project membership, so the answer is what they
 * would see in the console and nothing more.  superUser is passed false deliberately: a caller
 * coming through MCP gets their ordinary rights, not the elevated view the console offers an
 * administrator, because an agent should not quietly hold more than the person operating it.
 */
public class SurveyListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_list";
	}

	@Override
	public String getTitle() {
		return "List surveys";
	}

	@Override
	public String getDescription() {
		return "Lists the surveys this user can access, optionally filtered to one project. "
				+ "Returns the survey id and ident needed by the other survey and data tools.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"project_id", property("integer", "Only surveys in this project. Omit for all."),
				"include_deleted", property("boolean", "Include deleted surveys. Default false."));
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> survey = new LinkedHashMap<>();
		Map<String, Object> props = new LinkedHashMap<>();
		props.put("id", property("integer", "Survey id"));
		props.put("ident", property("string", "Survey ident, stable across versions"));
		props.put("name", property("string", "Display name"));
		props.put("project", property("string", "Project name"));
		props.put("deleted", property("boolean", "Soft deleted"));
		props.put("blocked", property("boolean", "Blocked from further submissions"));
		survey.put("type", "object");
		survey.put("properties", props);

		Map<String, Object> list = new LinkedHashMap<>();
		list.put("type", "array");
		list.put("items", survey);

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		Map<String, Object> top = new LinkedHashMap<>();
		top.put("surveys", list);
		schema.put("properties", top);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int projectId = intArg(arguments, "project_id", 0);
		boolean includeDeleted = boolArg(arguments, "include_deleted", false);

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		ArrayList<Survey> surveys = sm.getSurveys(ctx.sd, ctx.user,
				includeDeleted,
				false,			// getBlocked
				projectId,
				false,			// superUser - see the class comment
				false,			// onlyGroup
				false,			// getGroupDetails
				false,			// onlyDataSurvey
				false,			// links
				null);			// urlprefix

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(surveys.isEmpty()) {
			text.append("No surveys found.");
		} else {
			text.append("Found ").append(surveys.size()).append(" survey(s):\n");
			int shown = 0;
			for(Survey s : surveys) {
				if(ctx.maxRows > 0 && shown >= ctx.maxRows) {
					text.append("\n(list truncated at ").append(ctx.maxRows).append(" surveys)");
					break;
				}
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("id", s.getId());
				row.put("ident", s.getIdent());
				row.put("name", s.getDisplayName());
				row.put("project", s.getProjectName());
				row.put("deleted", s.getDeleted());
				row.put("blocked", s.getBlocked());
				rows.add(row);

				text.append("\n- ").append(s.getDisplayName())
						.append(" (id ").append(s.getId())
						.append(", ident ").append(s.getIdent())
						.append(", project ").append(s.getProjectName());
				if(s.getDeleted()) {
					text.append(", DELETED");
				}
				if(s.getBlocked()) {
					text.append(", BLOCKED");
				}
				text.append(")");
				shown++;
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("surveys", rows);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
