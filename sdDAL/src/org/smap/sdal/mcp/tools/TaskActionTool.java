package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.TaskBulkAction;
import org.smap.sdal.model.TaskAssignmentPair;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskListGeoJson;

/*
 * Do something to tasks that already exist: give them to somebody, move them along, or cancel them.
 *
 * One tool rather than three, because underneath it is one call with a different word in it.
 * Assigning, setting a status and deleting are the same operation over a list of tasks, and a list
 * of one is how a single task is dealt with.
 *
 * The assignee check is made here rather than left to the manager.  applyBulkAction inserts the
 * assignments without asking whether the person may have them - the single task path asks and the
 * bulk path does not - so this asks, and asks the corrected question: is the assignee in the
 * project, not can the assignee see the record.
 */
public class TaskActionTool extends AbstractMcpTool {

	/*
	 * accept maps to the manager's "status" action, which sets accepted and nothing else - it is not
	 * a general status setter, so it is not offered as one.  Naming it accept says what it does.
	 */
	private static final List<String> ACTIONS = Arrays.asList("assign", "accept", "delete");

	@Override
	public String getName() {
		return "task_action";
	}

	@Override
	public String getTitle() {
		return "Act on tasks";
	}

	@Override
	public String getDescription() {
		return "Assigns tasks to somebody, marks them accepted, or cancels them. Takes one task or "
				+ "many, so it is how a single task is dealt with as well as a batch. task_list "
				+ "gives the task ids. Cancelling a task does not delete anything that was already "
				+ "submitted for it.";
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
		return "task_action again: assign to somebody else, or recreate a cancelled task with task_create";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("action", property("string",
				"One of assign, accept or delete"));
		properties.put("task_ids", listOfIntegers(
				"The tasks to act on, from task_list"));
		properties.put("assignee", property("string",
				"For assign: the username to give the work to."));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "action", "task_ids" });
		return schema;
	}

	private static Map<String, Object> listOfIntegers(String description) {
		Map<String, Object> items = new LinkedHashMap<>();
		items.put("type", "integer");
		Map<String, Object> p = new LinkedHashMap<>();
		p.put("type", "array");
		p.put("items", items);
		p.put("description", description);
		return p;
	}

	@Override
	@SuppressWarnings("unchecked")
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String action = stringArg(arguments, "action");
		if(action == null || !ACTIONS.contains(action)) {
			return new MCPToolResult("action has to be one of: " + String.join(", ", ACTIONS)
					+ ".", true);
		}
		Object idsArg = arguments.get("task_ids");
		if(!(idsArg instanceof List) || ((List<Object>) idsArg).isEmpty()) {
			return new MCPToolResult("task_ids is required: the tasks to act on, from task_list.",
					true);
		}
		List<Integer> wanted = new ArrayList<>();
		for(Object o : (List<Object>) idsArg) {
			if(o instanceof Number) {
				wanted.add(((Number) o).intValue());
			}
		}
		if(wanted.isEmpty()) {
			return new MCPToolResult("task_ids has to hold task id numbers.", true);
		}

		Map<Integer, String> projects = McpData.userProjects(ctx);
		if(projects.isEmpty()) {
			return new MCPToolResult("You are not a member of any project.", true);
		}

		/*
		 * The tasks have to be ones this caller can see, and the assignment id has to come from the
		 * server rather than the caller: applyBulkAction acts on the pairs it is given, so a pair
		 * invented by a client would act on somebody else's assignment.
		 */
		TaskManager tm = new TaskManager(ctx.localisation, ctx.timezone);
		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/*
		 * Which project each task group belongs to.  getTasks leaves p_id at zero - only
		 * getUnassignedTasks fills it in - so a task cannot say which project it is in and the
		 * group has to be asked instead.  Reading it off the task rejected every task there was.
		 */
		Map<Integer, Integer> groupProject = new LinkedHashMap<>();
		for(Integer projectKey : projects.keySet()) {
			for(TaskGroup tg : tm.getTaskGroups(ctx.sd, projectKey)) {
				groupProject.put(tg.tg_id, projectKey);
			}
		}

		TaskListGeoJson all = tm.getTasks(ctx.sd, null, oId, 0, 0, 0, true, 0,
				null, null, 0, 0, "scheduled", "desc", false);

		ArrayList<TaskAssignmentPair> pairs = new ArrayList<>();
		List<Integer> notFound = new ArrayList<>(wanted);
		int pId = 0;
		int tgId = 0;
		if(all != null && all.features != null) {
			for(TaskFeature f : all.features) {
				if(f.properties == null || !wanted.contains(f.properties.id)) {
					continue;
				}
				Integer taskProject = groupProject.get(f.properties.tg_id);
				if(taskProject == null) {
					continue;		// Somebody else's project, so not one of theirs to act on
				}
				TaskAssignmentPair pair = new TaskAssignmentPair();
				pair.taskId = f.properties.id;
				pair.assignmentId = f.properties.a_id;
				pairs.add(pair);
				notFound.remove(Integer.valueOf(f.properties.id));
				pId = taskProject;
				tgId = f.properties.tg_id;
			}
		}
		if(pairs.isEmpty()) {
			return new MCPToolResult("None of those tasks is one you can act on. task_list shows "
					+ "the ones you can see.", true);
		}

		TaskBulkAction bulk = new TaskBulkAction();
		// The manager's word for accept is status, which is all that action does
		bulk.action = action.equals("accept") ? "status" : action;
		bulk.tasks = pairs;

		if(action.equals("assign")) {
			String assignee = stringArg(arguments, "assignee");
			if(assignee == null || assignee.trim().isEmpty()) {
				return new MCPToolResult("assign needs an assignee: the username to give the work "
						+ "to.", true);
			}
			assignee = assignee.trim();
			int assigneeId = GeneralUtilityMethods.getUserId(ctx.sd, assignee);
			if(assigneeId <= 0) {
				return new MCPToolResult("There is no user called " + assignee + ".", true);
			}
			/*
			 * Project membership, which is the rule for who may be given work.  Asked here because
			 * the bulk path in the manager inserts the assignments without asking.
			 */
			if(!new Authorise(null, null).isValidProject(ctx.sd, assignee, pId)) {
				return new MCPToolResult(assignee + " is not a member of "
						+ projects.get(pId) + ", the project this work is in, so the work cannot be "
						+ "given to them.", true);
			}
			bulk.userId = assigneeId;
		}

		tm.applyBulkAction(ctx.request, ctx.sd, ctx.cResults, ctx.user, tgId, pId, bulk);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("action", action);
		data.put("tasks", pairs.size());
		if(!notFound.isEmpty()) {
			data.put("skipped", notFound);
		}

		StringBuilder text = new StringBuilder();
		if(action.equals("assign")) {
			text.append("Gave ").append(pairs.size()).append(" task(s) to ")
					.append(stringArg(arguments, "assignee")).append(".");
		} else if(action.equals("accept")) {
			text.append("Marked ").append(pairs.size()).append(" task(s) accepted.");
		} else {
			text.append("Cancelled ").append(pairs.size()).append(" task(s). Nothing submitted for "
					+ "them has been deleted.");
		}
		if(!notFound.isEmpty()) {
			text.append("\n\n").append(notFound.size()).append(" of the ids given were left alone "
					+ "because they are not tasks you can act on.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
