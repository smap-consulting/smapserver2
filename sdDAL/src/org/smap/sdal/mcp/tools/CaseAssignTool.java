package org.smap.sdal.mcp.tools;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Give a case to somebody, or take it back.
 *
 * The same two access questions as a task, asked the same way.  The caller must be able to see the
 * record, because they are the one handing the work over; the assignee needs only to be in the
 * project.  Both are asked here because CaseManager.assignRecord does the update and no checking -
 * the checking lives in the endpoints that call it, so a tool calling it directly has to do the same
 * or it would be the one path into a case that asks nothing.
 *
 * Releasing and locking take no assignee: they are about whether anyone holds the case at all.
 */
public class CaseAssignTool extends AbstractMcpTool {

	private static final List<String> ACTIONS = Arrays.asList("assign", "release", "lock");

	@Override
	public String getName() {
		return "case_assign";
	}

	@Override
	public String getTitle() {
		return "Assign a case";
	}

	@Override
	public String getDescription() {
		return "Gives a case to somebody, releases it so anyone can take it, or locks it. A case is "
				+ "a record, so find one with data_query and use its instance id. To give a case "
				+ "away you must be able to see it yourself; the person you give it to needs only "
				+ "to be in the project. Closing a case is different - see case_settings.";
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
		return "case_assign again, to whoever held it before or with release";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the case is in, from survey_list"),
				"instance_id", property("string", "The case's record, from data_query"),
				"action", property("string",
						"One of assign, release or lock. Default assign."),
				"assignee", property("string",
						"For assign: the username to give the case to."),
				"note", property("string", "Optional. Why, recorded against the case."));
		schema.put("required", new String[] { "survey_id", "instance_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String instanceId = stringArg(arguments, "instance_id");
		if(instanceId == null || instanceId.trim().isEmpty()) {
			return new MCPToolResult("An instance_id is required: the case's record, from "
					+ "data_query.", true);
		}
		instanceId = instanceId.trim();

		String action = stringArg(arguments, "action");
		if(action == null || action.trim().isEmpty()) {
			action = "assign";
		}
		action = action.trim();
		if(!ACTIONS.contains(action)) {
			return new MCPToolResult("action has to be one of: " + String.join(", ", ACTIONS)
					+ ".", true);
		}

		/*
		 * The caller has to be able to see the record they are handing over.  This is the check the
		 * whole assignment model turns on, and it is asked of the caller rather than the assignee.
		 */
		if(!McpData.canSeeRecord(ctx, survey, instanceId)) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
		}

		String assignee = stringArg(arguments, "assignee");
		if(action.equals("assign")) {
			if(assignee == null || assignee.trim().isEmpty()) {
				return new MCPToolResult("assign needs an assignee: the username to give the case "
						+ "to. Use release to leave it for anyone to take.", true);
			}
			assignee = assignee.trim();
			if(GeneralUtilityMethods.getUserId(ctx.sd, assignee) <= 0) {
				return new MCPToolResult("There is no user called " + assignee + ".", true);
			}
			/* Project membership, the rule for who may be given work */
			RoleManager roleMgr = new RoleManager(ctx.localisation);
			if(!roleMgr.assignmentAllowed(ctx.sd, ctx.cResults, survey.getIdent(), instanceId,
					assignee, ctx.user, ctx.timezone, null)) {
				return new MCPToolResult(assignee + " is not a member of the project this case is "
						+ "in, so it cannot be given to them.", true);
			}
		} else {
			assignee = null;		// release and lock are about nobody in particular
		}

		String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(ctx.sd, ctx.cResults,
				survey.getIdent());
		if(tableName == null) {
			return new MCPToolResult("This survey has no data yet, so it has no cases.", true);
		}

		int count = new CaseManager(ctx.localisation).assignRecord(ctx.sd, ctx.cResults,
				ctx.localisation, tableName, instanceId, assignee, action, survey.getIdent(),
				stringArg(arguments, "note"), ctx.user);

		if(count == 0) {
			return new MCPToolResult("Nothing changed. The case may already be as you are asking "
					+ "for, or held by somebody else.", true);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("action", action);
		data.put("survey", survey.getDisplayName());
		data.put("instanceid", instanceId);
		data.put("assignee", assignee);

		String text;
		if(action.equals("assign")) {
			text = "Gave the case to " + assignee + ".";
		} else if(action.equals("release")) {
			text = "Released the case, so anyone in the project can take it.";
		} else {
			text = "Locked the case.";
		}

		MCPToolResult result = new MCPToolResult(text);
		result.setStructuredContent(data);
		return result;
	}
}
