package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.SqlWhereClause;

/*
 * Change the rule that decides when a task group gives out work, and to whom.
 *
 * A task group fires on a submission of its source survey that matches its filter.  Everything about
 * that lived in one JSON column and could only be edited in the console, which meant a workflow could
 * be built here and then not finished here - the OPP Public Prosecutor's approval had to be gated on
 * a question this interface had just added, and could not be.
 *
 * The rule is read, changed and written back rather than replaced.  It carries fields nothing here
 * offers - how tasks are scheduled, how far away they download, whether they are created for records
 * already submitted as well as new ones - and rewriting it from the arguments alone would silently
 * drop them.  Only what is named is touched.
 *
 * The source survey is not among those things.  What a task group fires on is what it is; pointing
 * it at a different survey makes it a different rule, and the filter, the assignee and the tasks
 * already created all assume the one it has.  Make another group instead.
 */
public class TaskGroupUpdateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "task_group_update";
	}

	@Override
	public String getTitle() {
		return "Change a task group's rule";
	}

	@Override
	public String getDescription() {
		return "Changes when a task group gives out work and to whom: its condition, its assignee, the "
				+ "survey the work is done in, and what it is called. Only what you name is changed, and "
				+ "an empty condition means it fires on every submission of its source survey.";
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
		return "task_group_update again with the previous values, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"task_group_id", property("integer",
						"The task group to change, from task_group_list or workflow_list"),
				"name", property("string", "Optional. What the group is called on the workflow."),
				"only_when", property("string",
						"Optional. The condition a submission of the source survey must match for work "
								+ "to be given out, such as ${request_decision} = 'yes'. Every name in "
								+ "it has to be a question on the record. An empty string takes the "
								+ "condition off, so it fires on every submission."),
				"assign_to", property("string",
						"Optional. A role name from role_list, a username from user_list, "
								+ "\"from_answer\" to take the assignee from an answer on the record, "
								+ "or an email address. Only one applies - setting it replaces "
								+ "whatever was there."),
				"target_survey", property("string",
						"Optional. The ident of the survey the work is done in, from survey_list. The "
								+ "group moves to that survey's project with it."));
		schema.put("required", new String[] { "task_group_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int tgId = intArg(arguments, "task_group_id", 0);
		if(tgId <= 0) {
			return new MCPToolResult("A task_group_id is required. task_group_list reports it.", true);
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();

		/*
		 * The group, in a project this caller belongs to.  A group in the organisation but outside
		 * their projects is answered as though it does not exist, which is what every other tool
		 * here does with a record somebody may not reach.
		 */
		String currentName = null;
		String projectName = null;
		String ruleJson = null;
		int sourceSId = 0;
		int targetSId = 0;
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select tg.name, p.name as project, tg.rule, tg.source_s_id, tg.target_s_id "
				+ "from task_group tg, project p, user_project up, users u "
				+ "where tg.p_id = p.id and p.id = up.p_id and up.u_id = u.id "
				+ "and u.ident = ? and p.o_id = ? and tg.tg_id = ?")) {
			pstmt.setString(1, ctx.user);
			pstmt.setInt(2, oId);
			pstmt.setInt(3, tgId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				currentName = rs.getString("name");
				projectName = rs.getString("project");
				ruleJson = rs.getString("rule");
				sourceSId = rs.getInt("source_s_id");
				targetSId = rs.getInt("target_s_id");
			}
		}
		if(currentName == null) {
			return new MCPToolResult("No task group " + tgId + ", or it is in a project you are not a "
					+ "member of. task_group_list shows the ones you can see.", true);
		}

		/*
		 * Read the rule and change it, rather than building a new one.  It holds scheduling, download
		 * distance and the flags deciding whether work is created for records already submitted - none
		 * of which this tool offers, and all of which would be lost by writing a fresh rule.
		 */
		AssignFromSurvey afs = null;
		if(ruleJson != null && !ruleJson.trim().isEmpty()) {
			afs = gson.fromJson(ruleJson, AssignFromSurvey.class);
		}
		if(afs == null) {
			afs = new AssignFromSurvey();
		}

		Map<String, Object> was = new LinkedHashMap<>();
		was.put("name", currentName);
		was.put("onlyWhen", afs.filter == null || afs.filter.advanced == null ? "" : afs.filter.advanced);
		was.put("assignedTo", describeAssignee(ctx, afs, oId));
		was.put("targetSurvey", targetSId > 0
				? GeneralUtilityMethods.getSurveyIdent(ctx.sd, targetSId) : "");

		List<String> changes = new ArrayList<>();
		String newName = currentName;

		String name = stringArg(arguments, "name");
		if(name != null && !name.trim().isEmpty() && !name.trim().equals(currentName)) {
			newName = name.trim();
			changes.add("called \"" + newName + "\"");
		}

		/*
		 * The condition.  An empty string is a real instruction - fire on every submission - and is
		 * distinct from leaving it alone, which is what an absent argument means.
		 */
		if(arguments.containsKey("only_when") && arguments.get("only_when") != null) {
			String onlyWhen = stringArg(arguments, "only_when");
			onlyWhen = onlyWhen == null ? "" : onlyWhen.trim();
			String current = (String) was.get("onlyWhen");
			if(!onlyWhen.equals(current)) {
				if(onlyWhen.isEmpty()) {
					afs.filter = null;
					changes.add("firing on every submission, with no condition");
				} else {
					if(afs.filter == null) {
						afs.filter = new SqlWhereClause();
					}
					afs.filter.advanced = onlyWhen;
					changes.add("firing only when " + onlyWhen);
				}
			}
		}

		/*
		 * Who it goes to.  Resolved against the roles and users that exist, so a name that is neither
		 * is refused now rather than becoming a group that gives work to nobody each time it fires.
		 */
		List<String> holders = new ArrayList<>();
		boolean isRole = false;
		String assignTo = stringArg(arguments, "assign_to");
		if(assignTo != null && !assignTo.trim().isEmpty()) {
			assignTo = assignTo.trim();
			int roleId = roleId(ctx, assignTo, oId);
			int userId = roleId > 0 ? 0 : userId(ctx, assignTo, oId);
			if(roleId <= 0 && userId <= 0
					&& !"from_answer".equalsIgnoreCase(assignTo) && assignTo.indexOf('@') < 0) {
				return new MCPToolResult("\"" + assignTo + "\" is neither a role in this organisation "
						+ "nor a user in it, and is not an email address. role_list and user_list show "
						+ "what there is, and \"from_answer\" takes the assignee from the record. "
						+ "Nothing was changed.", true);
			}
			/* One kind of assignee at a time: the others are cleared so none is left behind */
			afs.role_id = 0;
			afs.user_id = 0;
			afs.emails = null;
			afs.assign_data = null;
			String described;
			if(roleId > 0) {
				afs.role_id = roleId;
				isRole = true;
				holders = roleHolders(ctx, roleId, oId);
				described = "the role " + assignTo;
			} else if(userId > 0) {
				afs.user_id = userId;
				described = assignTo;
			} else if("from_answer".equalsIgnoreCase(assignTo)) {
				afs.assign_data = "_data";
				described = "whoever an answer on the record names";
			} else {
				afs.emails = assignTo;
				described = assignTo;
			}
			if(!described.equals(was.get("assignedTo"))) {
				changes.add("assigned to " + described);
			}
		} else if(afs.role_id > 0) {
			isRole = true;
			holders = roleHolders(ctx, afs.role_id, oId);
		}

		/*
		 * Where the work is done.  The group follows the survey into its project, which is what the
		 * console does - a task in one project whose group sits in another is not reachable.
		 */
		int newTargetSId = targetSId;
		int newProjectId = 0;
		String targetSurvey = stringArg(arguments, "target_survey");
		if(targetSurvey != null && !targetSurvey.trim().isEmpty()) {
			targetSurvey = targetSurvey.trim();
			int sId = GeneralUtilityMethods.getSurveyId(ctx.sd, targetSurvey);
			if(sId <= 0) {
				return new MCPToolResult("No survey with the ident \"" + targetSurvey + "\". "
						+ "survey_list reports the ident of each. Nothing was changed.", true);
			}
			if(sId != targetSId) {
				newTargetSId = sId;
				afs.target_survey_id = sId;
				newProjectId = projectOf(ctx, sId, oId);
				changes.add("done in " + targetSurvey);
			}
		}

		if(changes.isEmpty()) {
			return new MCPToolResult("\"" + currentName + "\" already reads that way, so nothing was "
					+ "changed.", false);
		}

		StringBuilder sql = new StringBuilder("update task_group set name = ?, rule = ?, target_s_id = ?");
		if(newProjectId > 0) {
			sql.append(", p_id = ?");
		}
		sql.append(" where tg_id = ?");
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(sql.toString())) {
			int idx = 1;
			pstmt.setString(idx++, newName);
			pstmt.setString(idx++, gson.toJson(afs));
			pstmt.setInt(idx++, newTargetSId);
			if(newProjectId > 0) {
				pstmt.setInt(idx++, newProjectId);
			}
			pstmt.setInt(idx++, tgId);
			pstmt.executeUpdate();
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("task_group_id", tgId);
		data.put("name", newName);
		data.put("project", projectName);
		data.put("changed", changes);
		data.put("was", was);
		if(isRole) {
			data.put("roleHolders", holders);
		}

		StringBuilder text = new StringBuilder();
		text.append("\"").append(newName).append("\" is now ")
				.append(String.join(", ", changes)).append(".");
		text.append("\n\nIt is the same group, so the tasks already in it and where it sits on the "
				+ "workflow are unchanged.");

		if(isRole) {
			if(holders.isEmpty()) {
				text.append("\n\n**Nobody holds that role.** Work it gives out will have nobody to go "
						+ "to. Give somebody the role with user_set_roles.");
			} else {
				text.append(" ").append(holders.size())
						.append(holders.size() == 1 ? " person holds it: " : " people hold it: ")
						.append(String.join(", ", holders)).append(".");
			}
		}
		if(sourceSId > 0) {
			/*
			 * Said every time a condition changes.  A filter is read against a submission of the
			 * source survey, and a name that is not a question there matches nothing and reports
			 * nothing - the group simply stops giving out work.
			 */
			String src = GeneralUtilityMethods.getSurveyIdent(ctx.sd, sourceSId);
			text.append("\n\nThe condition is read against a submission of ").append(src)
					.append(". Every question named in it has to exist on that record, or it matches "
							+ "nothing and the group quietly stops giving out work.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/* How the stored assignee reads to somebody */
	private String describeAssignee(McpToolContext ctx, AssignFromSurvey afs, int oId) throws Exception {
		if(afs.role_id > 0) {
			try (PreparedStatement pstmt = ctx.sd.prepareStatement(
					"select name from role where id = ? and o_id = ?")) {
				pstmt.setInt(1, afs.role_id);
				pstmt.setInt(2, oId);
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()) {
					return "the role " + rs.getString(1);
				}
			}
			return "role " + afs.role_id;
		}
		if(afs.user_id > 0) {
			try (PreparedStatement pstmt = ctx.sd.prepareStatement(
					"select ident from users where id = ? and o_id = ?")) {
				pstmt.setInt(1, afs.user_id);
				pstmt.setInt(2, oId);
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()) {
					return rs.getString(1);
				}
			}
			return "user " + afs.user_id;
		}
		if(afs.assign_data != null && !afs.assign_data.trim().isEmpty()) {
			return "whoever an answer on the record names";
		}
		if(afs.emails != null && !afs.emails.trim().isEmpty()) {
			return afs.emails;
		}
		return "";
	}

	private int roleId(McpToolContext ctx, String name, int oId) throws Exception {
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select id from role where o_id = ? and name = ?")) {
			pstmt.setInt(1, oId);
			pstmt.setString(2, name);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	private int userId(McpToolContext ctx, String ident, int oId) throws Exception {
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select id from users where ident = ? and o_id = ?")) {
			pstmt.setString(1, ident);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	private List<String> roleHolders(McpToolContext ctx, int roleId, int oId) throws Exception {
		List<String> out = new ArrayList<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select u.ident from users u, user_role ur "
				+ "where u.id = ur.u_id and ur.r_id = ? and u.o_id = ? and not u.temporary "
				+ "order by u.ident")) {
			pstmt.setInt(1, roleId);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				out.add(rs.getString(1));
			}
		}
		return out;
	}

	/* The project a survey is in, so the group can follow it */
	private int projectOf(McpToolContext ctx, int sId, int oId) throws Exception {
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select s.p_id from survey s, project p "
				+ "where s.p_id = p.id and s.s_id = ? and p.o_id = ? and not s.deleted")) {
			pstmt.setInt(1, sId);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}
}
