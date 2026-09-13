package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.TaskGroup;

/*
 * The task groups in a project.
 *
 * A task group is a batch of work: it names the survey the work is done in, optionally the survey
 * the work was generated from, and holds the tasks themselves.  Counts of total and complete come
 * back with it, because "how much of this is done" is the question usually being asked.
 *
 * Task groups belong to a project rather than to a survey, so project membership is what decides
 * whether a caller sees them.  That is a different question from whether they may read the records
 * the tasks point at, which is asked where it matters rather than here.
 */
public class TaskGroupListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "task_group_list";
	}

	@Override
	public String getTitle() {
		return "Task groups";
	}

	@Override
	public String getDescription() {
		return "The task groups in a project, with how many tasks each holds and how many are "
				+ "complete. A task group is a batch of work to be done in a survey. Use task_list "
				+ "to see the tasks in one.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"project_id", property("integer",
						"Optional. Only the task groups in this project, from project_list. Omit "
						+ "for every project you are a member of."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		Map<Integer, String> projects = McpData.userProjects(ctx);
		int projectId = intArg(arguments, "project_id", 0);
		if(projectId > 0 && !projects.containsKey(projectId)) {
			return new MCPToolResult("No such project, or you are not a member of it. project_list "
					+ "shows the ones you can see.", true);
		}

		TaskManager tm = new TaskManager(ctx.localisation, ctx.timezone);
		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		for(Map.Entry<Integer, String> project : projects.entrySet()) {
			if(projectId > 0 && project.getKey() != projectId) {
				continue;
			}
			ArrayList<TaskGroup> taskGroups = tm.getTaskGroups(ctx.sd, project.getKey());
			for(TaskGroup tg : taskGroups) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("task_group_id", tg.tg_id);
				row.put("name", tg.name);
				row.put("project", project.getValue());
				row.put("tasks", tg.totalTasks);
				row.put("complete", tg.completeTasks);
				rows.add(row);

				text.append("\n- ").append(tg.name).append(" (").append(tg.tg_id).append(") in ")
						.append(project.getValue()).append(": ").append(tg.completeTasks)
						.append(" of ").append(tg.totalTasks).append(" complete");
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append(projectId > 0
					? "That project has no task groups."
					: "None of your projects has any task groups.");
		} else {
			text.insert(0, "Found " + rows.size() + " task group(s):");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("task_groups", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
