package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * Remove a task group.
 *
 * Three things go, and the third is the one nobody expects.  The tasks in the group are deleted and
 * their assignments cancelled; the group itself goes; and **any reminder notification naming this
 * group goes with it**, because a reminder points at its group through forward.tg_id with no foreign
 * key behind it - left alone it would name a group that no longer exists.
 *
 * Tasks are removed through the manager rather than by cascade, because the temporary users created
 * to carry an assignment have to go with them, and anybody who had accepted an assignment is told it
 * has gone.  So this is not silent for the people affected, which is the reason it asks first.
 *
 * A task group that a workflow rule generates work into is a different matter from an empty one
 * somebody made by hand, and the answer says which it is before the question is put.
 */
public class TaskGroupDeleteTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "task_group_delete";
	}

	@Override
	public String getTitle() {
		return "Delete a task group";
	}

	@Override
	public String getDescription() {
		return "Removes a task group, the tasks in it, and any reminder notification that names it. "
				+ "People holding an accepted assignment are told it has gone. Says what will go "
				+ "before doing it. To stop a group being used without removing its history, leave "
				+ "it and switch off whatever creates work in it.";
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
		return "none - task_group_create makes a group of the same name, but its tasks, their "
				+ "assignments and any reminder are gone";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getAnnotations() {
		Map<String, Object> a = super.getAnnotations();
		a.put("destructiveHint", Boolean.TRUE);
		return a;
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"task_group_id", property("integer",
						"The task group to remove, from task_group_list"),
				"acknowledge", property("object",
						"Only when your client cannot show an approval prompt. After the person has "
								+ "agreed, call again with {\"task_group\": \"<its name>\"}."));
		schema.put("required", new String[] { "task_group_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int tgId = intArg(arguments, "task_group_id", 0);
		if(tgId <= 0) {
			return new MCPToolResult("A task_group_id is required. Use task_group_list to find one.",
					true);
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/*
		 * Found within this organisation's own projects.  A task group id is a bare number and would
		 * otherwise reach one belonging to somebody else.
		 */
		String name = null;
		String projectName = null;
		boolean hasRule = false;
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select tg.name, p.name as project, tg.rule "
				+ "from task_group tg, project p "
				+ "where tg.p_id = p.id and p.o_id = ? and tg.tg_id = ?")) {
			pstmt.setInt(1, oId);
			pstmt.setInt(2, tgId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				name = rs.getString(1);
				projectName = rs.getString(2);
				String rule = rs.getString(3);
				hasRule = rule != null && !rule.trim().isEmpty();
			}
		}
		if(name == null) {
			return new MCPToolResult("There is no task group with id " + tgId + " in this "
					+ "organisation. task_group_list shows what there is.", true);
		}

		int taskCount = 0;
		int acceptedCount = 0;
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select count(*) from tasks where tg_id = ? and not deleted")) {
			pstmt.setInt(1, tgId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				taskCount = rs.getInt(1);
			}
		}
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select count(*) from assignments a, tasks t "
				+ "where a.task_id = t.id and t.tg_id = ? and a.status = 'accepted'")) {
			pstmt.setInt(1, tgId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				acceptedCount = rs.getInt(1);
			}
		}

		/* Reminder notifications, which go with the group and are easy to forget */
		List<String> reminders = new ArrayList<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select name from forward where tg_id = ?")) {
			pstmt.setInt(1, tgId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String n = rs.getString(1);
				reminders.add(n == null || n.trim().isEmpty() ? "an unnamed reminder" : n);
			}
		}

		if(refused(ctx)) {
			return new MCPToolResult("Left alone. \"" + name + "\" and everything in it are "
					+ "unchanged.", false);
		}
		if(!approved(ctx)) {
			StringBuilder what = new StringBuilder("Delete the task group \"").append(name)
					.append("\" in ").append(projectName).append("?");
			if(taskCount == 0) {
				what.append("\n\nIt holds no tasks.");
			} else {
				what.append("\n\nIt holds ").append(taskCount)
						.append(taskCount == 1 ? " task, which goes with it."
								: " tasks, which go with it.");
				if(acceptedCount > 0) {
					what.append(" ").append(acceptedCount)
							.append(acceptedCount == 1
									? " has been accepted by somebody, who will be told it has gone."
									: " have been accepted by people, who will be told they have "
											+ "gone.");
				}
			}
			if(!reminders.isEmpty()) {
				what.append("\n\n").append(String.join(", ", reminders))
						.append(reminders.size() == 1 ? " is a reminder naming this group and goes "
								+ "with it." : " are reminders naming this group and go with it.");
			}
			if(hasRule) {
				what.append("\n\nWork is generated into this group by a rule, so it appears on the "
						+ "workflow. Anything upstream of it will have nowhere to send work.");
			}
			what.append("\n\nNone of it can be put back.");

			if(ctx.canElicit()) {
				return ask(ctx, what.toString());
			}
			Object ackArg = arguments.get("acknowledge");
			String claimed = null;
			if(ackArg instanceof Map) {
				Object v = ((Map<?, ?>) ackArg).get("task_group");
				claimed = v == null ? null : v.toString().trim();
			}
			if(claimed == null || !claimed.equalsIgnoreCase(name)) {
				return new MCPToolResult(what
						+ "\n\nThis client cannot ask you to approve that mid-request. Put it to the "
						+ "person responsible, and if they agree call again with acknowledge set to "
						+ "{\"task_group\": \"" + name + "\"}.", true);
			}
		}

		new TaskManager(ctx.localisation, ctx.timezone)
				.deleteTaskGroup(ctx.sd, tgId, ctx.user, ctx.request.getServerName());

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("task_group_id", tgId);
		data.put("name", name);
		data.put("project", projectName);
		data.put("tasksRemoved", taskCount);
		data.put("acceptedAssignments", acceptedCount);
		data.put("remindersRemoved", reminders);

		StringBuilder text = new StringBuilder();
		text.append("Deleted the task group \"").append(name).append("\" in ").append(projectName)
				.append(".");
		if(taskCount > 0) {
			text.append(" ").append(taskCount)
					.append(taskCount == 1 ? " task went with it." : " tasks went with it.");
		}
		if(acceptedCount > 0) {
			text.append(" ").append(acceptedCount)
					.append(acceptedCount == 1 ? " person has been told their assignment has gone."
							: " people have been told their assignments have gone.");
		}
		if(!reminders.isEmpty()) {
			text.append("\n\nAlso removed: ").append(String.join(", ", reminders)).append(".");
		}
		if(hasRule) {
			text.append("\n\nThis group was fed by a rule. Check workflow_list for a step that now "
					+ "leads nowhere.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
