package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Project;

/*
 * Start a new survey, empty or as a copy of one that exists.
 *
 * Creating and duplicating are one call in Smap and are one tool here for the same reason: the
 * difference is whether an existing survey is named, and splitting them would be two tools wrapping
 * the same manager with the same arguments.
 *
 * This is how a model builds a form.  The XLSForm path is not exposed, because an agent cannot
 * produce a spreadsheet it has never seen; it makes a survey here and then puts questions in it with
 * survey_add_question, which is the same thing the online editor does.
 */
public class SurveyCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_create";
	}

	@Override
	public String getTitle() {
		return "Create a survey";
	}

	@Override
	public String getDescription() {
		return "Creates a survey in a project, either empty or as a copy of one that already "
				+ "exists. Add questions to it with survey_add_question. A copy takes the questions "
				+ "of the survey it is copied from and none of its data. Undo it with "
				+ "survey_delete.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.WRITE;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "survey_delete removes it, and survey_undelete would bring it back again";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"name", property("string", "What the survey is called"),
				"project_id", property("integer",
						"The project it belongs to, from project_list"),
				"copy_of_survey_id", property("integer",
						"Optional. Copy this survey's design instead of starting empty. The copy "
						+ "gets the questions and none of the answers."));
		schema.put("required", new String[] { "name", "project_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required.", true);
		}
		name = name.trim();

		int projectId = intArg(arguments, "project_id", 0);
		if(projectId <= 0) {
			return new MCPToolResult("A project_id is required. Use project_list to find one.", true);
		}
		/*
		 * The project has to be one this user is a member of.  createNewSurvey is given the project
		 * as a number and does not ask whether the caller may put a survey in it, so it is asked
		 * here, the same way every other tool decides what the caller can reach.
		 */
		boolean inProject = false;
		String projectName = null;
		for(Project p : new org.smap.sdal.managers.ProjectManager(ctx.localisation)
				.getProjects(ctx.sd, ctx.user, false, false, null, false, false)) {
			if(p.id == projectId) {
				inProject = true;
				projectName = p.name;
				break;
			}
		}
		if(!inProject) {
			return new MCPToolResult("No such project, or you are not a member of it. project_list "
					+ "shows the ones you can use.", true);
		}

		int copyOf = intArg(arguments, "copy_of_survey_id", 0);
		String copiedFrom = null;
		if(copyOf > 0) {
			org.smap.sdal.model.Survey source = org.smap.sdal.mcp.McpData.surveyById(ctx, copyOf);
			if(source == null) {
				return new MCPToolResult("No such survey to copy, or you do not have access to it.",
						true);
			}
			copiedFrom = source.getDisplayName();
		}

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		int sId = sm.createNewSurvey(ctx.sd, name, projectId,
				copyOf > 0,		// existing
				copyOf,			// existingSurveyId
				false,			// sharedResults - a copy gets its own data tables, not the original's
				ctx.user,
				false);			// superUser - the caller's own rights

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("survey_id", sId);
		data.put("name", name);
		data.put("project", projectName);
		if(copiedFrom != null) {
			data.put("copiedFrom", copiedFrom);
		}

		StringBuilder text = new StringBuilder();
		text.append("Created \"").append(name).append("\" in ").append(projectName)
				.append(" as survey ").append(sId).append(".");
		if(copiedFrom != null) {
			text.append(" It is a copy of \"").append(copiedFrom)
					.append("\", with that survey's questions and none of its answers.");
		} else {
			text.append(" It has no questions yet - add them with survey_add_question.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
