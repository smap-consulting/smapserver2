package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskListGeoJson;

/*
 * The tasks in a project or a task group, with who they are assigned to and where they have got to.
 *
 * A task and an assignment are not quite the same thing: the task is the work, and the assignment is
 * the work given to a particular person, so one task can carry several.  They are reported together
 * here rather than as two tools, because a task with no assignment and an assignment with no task
 * are both meaningless and asking about one nearly always means asking about the other.  The
 * assignment id is reported alongside the task id, and it is the assignment id that the write tools
 * take.
 *
 * mine narrows this to the caller's own work, which is the question a person asks about themselves
 * and the reason there is no separate tool for it.
 */
public class TaskListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "task_list";
	}

	@Override
	public String getTitle() {
		return "Tasks";
	}

	@Override
	public String getDescription() {
		return "The tasks in a project or a task group: what is to be done, in which survey, who it "
				+ "is assigned to and what state it is in. Pass mine to see only your own "
				+ "assignments. One task can be assigned to several people, so the assignment id is "
				+ "reported alongside the task id.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"project_id", property("integer",
						"Optional. Tasks in this project, from project_list."),
				"task_group_id", property("integer",
						"Optional. Only the tasks in this task group, from task_group_list."),
				"mine", property("boolean",
						"Optional. Only tasks assigned to you. Default false."),
				"include_complete", property("boolean",
						"Optional. Include tasks that are finished. Default true."),
				"limit", property("integer",
						"Optional. Most tasks to return. Default 100."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		Map<Integer, String> projects = McpData.userProjects(ctx);
		if(projects.isEmpty()) {
			return new MCPToolResult("You are not a member of any project, so there are no tasks to "
					+ "show.", false);
		}
		int projectId = intArg(arguments, "project_id", 0);
		if(projectId > 0 && !projects.containsKey(projectId)) {
			return new MCPToolResult("No such project, or you are not a member of it.", true);
		}
		int taskGroupId = intArg(arguments, "task_group_id", 0);
		boolean mine = boolArg(arguments, "mine", false);
		boolean includeComplete = boolArg(arguments, "include_complete", true);
		int limit = ctx.cap(intArg(arguments, "limit", 100));

		TaskManager tm = new TaskManager(ctx.localisation, ctx.timezone);

		/*
		 * A task group named on its own still has to be one of the caller's.  getTasks takes it as a
		 * number and says in its own comment that it assumes the check has already happened, so it
		 * happens here.
		 */
		if(taskGroupId > 0) {
			boolean found = false;
			for(Integer pId : projects.keySet()) {
				for(TaskGroup tg : tm.getTaskGroups(ctx.sd, pId)) {
					if(tg.tg_id == taskGroupId) {
						found = true;
						break;
					}
				}
				if(found) {
					break;
				}
			}
			if(!found) {
				return new MCPToolResult("No such task group, or it is in a project you are not a "
						+ "member of. task_group_list shows the ones you can see.", true);
			}
		}

		int userId = mine ? GeneralUtilityMethods.getUserId(ctx.sd, ctx.user) : 0;
		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/*
		 * Which project each task group belongs to.
		 *
		 * getTasks does not fill in p_id - getUnassignedTasks does, getTasks does not - so the task
		 * itself cannot say which project it is in.  The task group can, and every task has one.
		 */
		Map<Integer, Integer> groupProject = new LinkedHashMap<>();
		for(Integer pId : projects.keySet()) {
			for(TaskGroup tg : tm.getTaskGroups(ctx.sd, pId)) {
				groupProject.put(tg.tg_id, pId);
			}
		}

		TaskListGeoJson tasks = tm.getTasks(ctx.sd, null,
				taskGroupId > 0 ? 0 : oId,
				taskGroupId,
				0,				// one task
				0,				// one assignment
				includeComplete,
				userId,
				null,			// incStatus
				null,			// period
				0,				// start
				limit,
				"scheduled",
				"desc",
				false);			// links

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(tasks != null && tasks.features != null) {
			for(TaskFeature f : tasks.features) {
				if(f.properties == null) {
					continue;
				}
				/*
				 * Tasks are asked for by organisation when no group is named, so they are filtered
				 * back to the projects this caller is in.  An organisation is wider than a person.
				 */
				Integer taskProject = groupProject.get(f.properties.tg_id);
				if(taskProject == null) {
					continue;		// A group outside this caller's projects
				}
				if(projectId > 0 && taskProject != projectId) {
					continue;
				}

				Map<String, Object> row = new LinkedHashMap<>();
				row.put("task_id", f.properties.id);
				row.put("assignment_id", f.properties.a_id);
				row.put("name", f.properties.name);
				row.put("task_group", f.properties.tg_name);
				row.put("survey", f.properties.survey_name);
				row.put("status", f.properties.status);
				row.put("assignee", f.properties.assignee_ident);
				row.put("assigneeName", f.properties.assignee_name);
				row.put("scheduledAt", f.properties.from == null ? null : f.properties.from.toString());
				row.put("project", projects.get(taskProject));
				rows.add(row);

				text.append("\n- ").append(f.properties.name);
				if(f.properties.status != null) {
					text.append(" [").append(f.properties.status).append("]");
				}
				if(f.properties.assignee_name != null && !f.properties.assignee_name.isEmpty()) {
					text.append(" assigned to ").append(f.properties.assignee_name);
				} else if(f.properties.assignee_ident == null) {
					text.append(" unassigned");
				}
				if(f.properties.survey_name != null) {
					text.append(", in ").append(f.properties.survey_name);
				}
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append(mine ? "You have no tasks assigned to you."
					: "No tasks found.");
		} else {
			text.insert(0, (mine ? "You have " : "Found ") + rows.size() + " task(s):");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("tasks", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
