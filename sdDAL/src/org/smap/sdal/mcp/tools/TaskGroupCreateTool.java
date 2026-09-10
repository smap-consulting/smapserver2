package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Make a batch of work to put tasks in.
 *
 * A task group is named and belongs to a project, and every task in it is work to be done in one
 * survey.  Creating one is the first half of "give these fifty records to these people": the group,
 * then a task in it for each record.
 */
public class TaskGroupCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "task_group_create";
	}

	@Override
	public String getTitle() {
		return "Create a task group";
	}

	@Override
	public String getDescription() {
		return "Creates a task group: a named batch of work to be done in one survey. Add tasks to "
				+ "it with task_create. If a group of that name already exists in the project it is "
				+ "used rather than a second one being made.";
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
		return "task_action with delete removes the tasks in it; the empty group is harmless";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"name", property("string", "What this batch of work is called"),
				"survey_id", property("integer",
						"The survey the work is done in, from survey_list. Its project is the "
						+ "project the group belongs to."),
				"complete_all", property("boolean",
						"Optional. Every assignment on a task must be finished, not just one. "
						+ "Default false."),
				"assign_auto", property("boolean",
						"Optional. Let people take an unassigned task for themselves. "
						+ "Default false."));
		schema.put("required", new String[] { "name", "survey_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required.", true);
		}
		name = name.trim();

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey listed = McpData.surveyById(ctx, surveyId);
		if(listed == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		/*
		 * The project comes from the survey rather than being asked for, so a group cannot be made
		 * in a project the survey is not in.  outline is used because the listed survey comes from a
		 * list query, which leaves what a list does not need unset.
		 */
		Survey s = McpData.outline(ctx, surveyId);
		int projectId = s.surveyData.p_id;

		TaskManager tm = new TaskManager(ctx.localisation, ctx.timezone);
		int tgId = tm.createTaskGroup(ctx.sd, name, projectId,
				null,			// address columns
				null,			// rule
				0,				// source survey
				0,				// target survey
				0,				// download distance
				boolArg(arguments, "complete_all", false),
				boolArg(arguments, "assign_auto", false),
				true);			// use an existing group of the same name

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("task_group_id", tgId);
		data.put("name", name);
		data.put("survey", listed.getDisplayName());
		data.put("project", s.surveyData.projectName);

		MCPToolResult result = new MCPToolResult("Task group \"" + name + "\" (" + tgId + ") for "
				+ "work in \"" + listed.getDisplayName() + "\". Add tasks to it with task_create.");
		result.setStructuredContent(data);
		return result;
	}
}
