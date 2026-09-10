package org.smap.sdal.mcp.tools;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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
import org.smap.sdal.model.CreateTaskResp;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskProperties;

/*
 * One piece of work: fill in this survey, optionally about this record, optionally by this person.
 *
 * Goes through the manager the console's own endpoint uses, so a task made here is the task the
 * console would have made - the same record checks, the same assignment rule, the same rows.
 *
 * Two different access questions are asked and they are not the same.  The caller must be able to
 * read the record the task is about, because they are the one authorising the work.  The assignee
 * needs only to be a member of the project: a row filter that limits an enumerator to their own
 * submissions is exactly the case where giving them the work is the point, since the record is not
 * theirs yet.  Both are enforced inside the manager rather than here, so the console and this agree.
 */
public class TaskCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "task_create";
	}

	@Override
	public String getTitle() {
		return "Create a task";
	}

	@Override
	public String getDescription() {
		return "Creates one task: work to be done in a survey, optionally about an existing record "
				+ "and optionally assigned to somebody. Give a task_group_id to put it in an "
				+ "existing batch, or leave it out and a group named after the survey is used. To "
				+ "assign work about a record, you must be able to see that record yourself; the "
				+ "person you assign it to needs only to be in the project.";
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
		return "task_action with delete cancels it";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the work is done in, from survey_list"),
				"name", property("string", "What the task is called, as the person doing it sees it"),
				"task_group_id", property("integer",
						"Optional. The batch to put it in, from task_group_list. Omit and a group "
						+ "named after the survey is used or made."),
				"assignee", property("string",
						"Optional. The username to give the work to. Leave it out for an unassigned "
						+ "task somebody can pick up."),
				"instance_id", property("string",
						"Optional. The record this work is about, so the assignee opens it filled "
						+ "in rather than blank. From data_query."),
				"scheduled_at", property("string",
						"Optional. When the work is due, as yyyy-MM-dd HH:mm:ss."));
		schema.put("required", new String[] { "survey_id", "name" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey listed = McpData.surveyById(ctx, surveyId);
		if(listed == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required. It is what the person doing the work "
					+ "sees.", true);
		}

		TaskProperties tp = new TaskProperties();
		tp.form_id = surveyId;
		tp.name = name.trim();
		tp.tg_id = intArg(arguments, "task_group_id", 0);
		tp.assignee_ident = stringArg(arguments, "assignee");
		tp.update_id = stringArg(arguments, "instance_id");
		tp.assignee_type = "user";
		tp.initial_data_source = tp.update_id != null && !tp.update_id.trim().isEmpty()
				? TaskManager.SURVEY_DATA_SOURCE : TaskManager.NO_DATA_SOURCE;

		if(tp.assignee_ident != null && tp.assignee_ident.trim().isEmpty()) {
			tp.assignee_ident = null;
		}
		if(tp.assignee_ident != null) {
			int assigneeId = GeneralUtilityMethods.getUserId(ctx.sd, tp.assignee_ident);
			if(assigneeId <= 0) {
				return new MCPToolResult("There is no user called " + tp.assignee_ident + ".", true);
			}
			tp.assignee = assigneeId;
		}

		String scheduled = stringArg(arguments, "scheduled_at");
		if(scheduled != null && !scheduled.trim().isEmpty()) {
			try {
				tp.from = new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
						.parse(scheduled.trim()).getTime());
			} catch (Exception e) {
				return new MCPToolResult("scheduled_at has to be yyyy-MM-dd HH:mm:ss, such as "
						+ "2026-09-30 09:00:00.", true);
			}
		}

		/*
		 * superUser false: the caller's own rights decide what survey this can be made against, as
		 * everywhere else here.
		 */
		TaskManager tm = new TaskManager(ctx.localisation, ctx.timezone);
		CreateTaskResp resp;
		try {
			resp = tm.createTask(ctx.sd, ctx.cResults, tp, ctx.user,
					ctx.request.getServerName(),
					GeneralUtilityMethods.getUrlPrefix(ctx.request),
					false,			// preserveInitialData
					false);			// superUser
		} catch (Exception e) {
			/*
			 * The manager refuses in words - the record is not one you can see, or the assignee is
			 * not in the project - and those are answers rather than failures, so they are handed
			 * back as they are.
			 */
			return new MCPToolResult(e.getMessage() == null
					? "The task could not be created." : e.getMessage(), true);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("task_id", resp == null || resp.task == null ? 0 : resp.task.id);
		data.put("name", tp.name);
		data.put("survey", listed.getDisplayName());
		data.put("assignee", tp.assignee_ident);
		if(tp.update_id != null) {
			data.put("aboutRecord", tp.update_id);
		}

		StringBuilder text = new StringBuilder();
		text.append("Created task \"").append(tp.name).append("\" in \"")
				.append(listed.getDisplayName()).append("\"");
		if(tp.assignee_ident != null) {
			text.append(", assigned to ").append(tp.assignee_ident);
		} else {
			text.append(", unassigned");
		}
		text.append(".");
		if(tp.update_id != null) {
			text.append(" It opens the existing record rather than a blank form.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
