package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Notification;
import org.smap.sdal.model.NotifyDetails;

/*
 * Change an assignment rule without replacing it.
 *
 * The point of this tool is the row it does not touch.  Until it existed the only way to change a
 * rule here was notification_delete followed by notification_create_assignment, which allocates a
 * new forward record, and a forward record is the identity of a step everywhere else in Smap:
 *
 *   - the workflow page keys each node on the rule, and a user's hand placed layout on the node.
 *     Replace the rule and the saved position has nothing to attach to, so the step reappears
 *     wherever the default layout puts it - which is how a laid out workflow comes back jumbled
 *     after a few edits, with nothing added and nothing removed.
 *   - wf_prev_node_id on forward and task_group records the step before this one, and it holds a
 *     node id containing the forward id.  Deleting a rule orphans whatever followed it.
 *
 * None of that is visible at the time.  The delete succeeds, the create succeeds, the workflow still
 * lists every step, and the damage is to layout and links that nothing reports on.  So editing in
 * place is not a convenience here, it is the difference between a change and a rebuild.
 *
 * Only what is named is changed.  A field left out - or given null - keeps what it has, which is the
 * same rule the data tools follow; only_when takes an empty string to mean take the filter off, so
 * that a rule can be made to fire on every submission again.
 */
public class NotificationAssignUpdateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "notification_update_assignment";
	}

	@Override
	public String getTitle() {
		return "Change an assignment rule";
	}

	@Override
	public String getDescription() {
		return "Changes who an assignment rule assigns to, what it is called, when it fires, or which form "
				+ "the case points at - keeping the same rule, so the workflow layout and the links from "
				+ "earlier steps survive. Use this rather than deleting and creating a rule again. Whether "
				+ "it is switched on is notification_enable.";
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
		return "notification_update_assignment again with the previous values, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"notification_id", property("integer",
						"The rule to change, from notification_list or workflow_list"),
				"name", property("string",
						"Optional. What this step is called on the workflow."),
				"assign_to", property("string",
						"Optional. A role name from role_list, a username from user_list, or "
								+ "\"submitter\" for whoever filled the form in."),
				"only_when", property("string",
						"Optional. An expression over question names, such as ${unit} = 'faso', so "
								+ "this fires only for some submissions. Give an empty string to "
								+ "take the condition off and fire on every submission."),
				"case_survey", property("string",
						"Optional. The ident of the survey holding the form the person is meant to "
								+ "open next. In a bundle this is the next stage's form, not the one "
								+ "being submitted. survey_list gives idents."));
		schema.put("required", new String[] { "notification_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int notificationId = intArg(arguments, "notification_id", 0);
		if(notificationId <= 0) {
			return new MCPToolResult("A notification_id is required. notification_list and "
					+ "workflow_list both report it.", true);
		}

		NotificationManager nm = new NotificationManager(ctx.localisation);

		/* In the caller's own projects, the same place notification_delete and _enable look */
		Notification listed = null;
		Map<Integer, String> projects = McpData.userProjects(ctx);
		for(Map.Entry<Integer, String> project : projects.entrySet()) {
			ArrayList<Notification> notifications = nm.getProjectNotifications(ctx.sd, null,
					ctx.user, project.getKey(), ctx.timezone);
			for(Notification n : notifications) {
				if(n.id == notificationId) {
					listed = n;
					break;
				}
			}
			if(listed != null) {
				break;
			}
		}
		if(listed == null) {
			return new MCPToolResult("No such notification, or it is in a project you are not a "
					+ "member of. notification_list shows the ones you can see.", true);
		}

		/*
		 * The whole record, because the update writes every column back and anything not read here
		 * would be written as empty.
		 */
		Notification n = nm.getNotification(ctx.sd, notificationId, ctx.timezone);
		if(n == null) {
			return new MCPToolResult("Notification " + notificationId + " could not be read.", true);
		}
		if(!"escalate".equals(n.target)) {
			return new MCPToolResult("\"" + listed.name + "\" does not assign work - it sends "
					+ (n.target == null ? "something else" : n.target) + ". This tool changes "
					+ "assignment rules, the ones notification_create_assignment makes.", true);
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/* What it was, read before anything is changed, so the change can be described and undone */
		Map<String, Object> was = new LinkedHashMap<>();
		was.put("name", n.name);
		was.put("assignedTo", describeAssignee(ctx, n.remote_user, oId));
		was.put("onlyWhen", n.filter == null ? "" : n.filter);
		was.put("caseSurvey", n.notifyDetails == null || n.notifyDetails.survey_case == null
				? "" : n.notifyDetails.survey_case);

		List<String> changes = new ArrayList<>();

		String name = stringArg(arguments, "name");
		if(name != null && !name.trim().isEmpty() && !name.trim().equals(n.name)) {
			n.name = name.trim();
			changes.add("called \"" + n.name + "\"");
		}

		/*
		 * Who it assigns to.  Resolved against the roles and users that exist, so a name that is
		 * neither is refused now rather than becoming a rule that assigns to nobody each time it
		 * fires - which reports nothing and looks like a queue that nobody is working.
		 */
		String assignTo = stringArg(arguments, "assign_to");
		boolean isRole = false;
		String assignedDescription = (String) was.get("assignedTo");
		List<String> holders = new ArrayList<>();
		if(assignTo != null && !assignTo.trim().isEmpty()) {
			assignTo = assignTo.trim();
			String remoteUser;
			if("submitter".equalsIgnoreCase(assignTo)) {
				remoteUser = "_submitter";
				assignedDescription = "whoever submitted it";
			} else {
				int roleId = roleId(ctx, assignTo, oId);
				if(roleId > 0) {
					isRole = true;
					remoteUser = "_role:" + roleId;
					assignedDescription = "the role " + assignTo;
					holders = roleHolders(ctx, roleId, oId);
				} else if(userExists(ctx, assignTo, oId)) {
					remoteUser = assignTo;
					assignedDescription = assignTo;
				} else {
					return new MCPToolResult("\"" + assignTo + "\" is neither a role in this "
							+ "organisation nor a user in it. role_list and user_list show what "
							+ "there is, and \"submitter\" assigns to whoever filled the form in. "
							+ "Nothing was changed.", true);
				}
			}
			if(!remoteUser.equals(n.remote_user)) {
				n.remote_user = remoteUser;
				changes.add("assigned to " + assignedDescription);
			}
		} else if(n.remote_user != null && n.remote_user.startsWith("_role:")) {
			/*
			 * Not being changed, but still reported on: a rule whose role has lost its holders
			 * assigns nothing, and somebody editing the rule is the right person to be told.
			 */
			isRole = true;
			try {
				int roleId = Integer.parseInt(n.remote_user.substring("_role:".length()).trim());
				holders = roleHolders(ctx, roleId, oId);
			} catch (NumberFormatException e) {
				// A malformed assignee is reported by role_list, not worth failing an edit over
			}
		}

		/*
		 * The condition.  An empty string takes it off, which is a real thing to want - a rule that
		 * was filtered to one stage and should now fire on every submission - and is distinct from
		 * leaving it alone.
		 */
		if(arguments.containsKey("only_when") && arguments.get("only_when") != null) {
			String onlyWhen = stringArg(arguments, "only_when");
			onlyWhen = onlyWhen == null ? "" : onlyWhen.trim();
			String current = n.filter == null ? "" : n.filter.trim();
			if(!onlyWhen.equals(current)) {
				n.filter = onlyWhen.isEmpty() ? null : onlyWhen;
				changes.add(onlyWhen.isEmpty()
						? "firing on every submission, with no condition"
						: "firing only when " + onlyWhen);
			}
		}

		String caseSurvey = stringArg(arguments, "case_survey");
		if(caseSurvey != null && !caseSurvey.trim().isEmpty()) {
			caseSurvey = caseSurvey.trim();
			if(!surveyIdentExists(ctx, caseSurvey, oId)) {
				return new MCPToolResult("No survey in this organisation has the ident \""
						+ caseSurvey + "\". survey_list reports the ident of each. Nothing was "
						+ "changed.", true);
			}
			if(n.notifyDetails == null) {
				n.notifyDetails = new NotifyDetails();
				n.notifyDetails.emailQuestionName = "-1";
			}
			if(!caseSurvey.equals(n.notifyDetails.survey_case)) {
				n.notifyDetails.survey_case = caseSurvey;
				changes.add("pointing the case at " + caseSurvey);
			}
		}

		if(changes.isEmpty()) {
			return new MCPToolResult("\"" + n.name + "\" already reads that way, so nothing was "
					+ "changed.", false);
		}

		/*
		 * updateNotification writes the existing forward row, so the rule keeps its id - which is
		 * what everything else in Smap uses to recognise this step.
		 */
		nm.updateNotification(ctx.sd, null, ctx.user, n, ctx.timezone);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("notification_id", notificationId);
		data.put("name", n.name);
		data.put("assignedTo", assignedDescription);
		data.put("onlyWhen", n.filter == null ? "" : n.filter);
		data.put("caseSurvey", n.notifyDetails == null || n.notifyDetails.survey_case == null
				? "" : n.notifyDetails.survey_case);
		data.put("enabled", n.enabled);
		data.put("was", was);
		if(isRole) {
			data.put("roleHolders", holders);
		}

		StringBuilder text = new StringBuilder();
		text.append("\"").append(n.name).append("\" is now ")
				.append(String.join(", ", changes)).append(".");
		text.append("\n\nIt is the same rule, so where it sits on the workflow and anything leading "
				+ "into it are unchanged.");

		if(isRole) {
			if(holders.isEmpty()) {
				text.append("\n\n**Nobody holds that role.** Every submission matching this will "
						+ "create a case and leave it unassigned, with nothing reported. Give "
						+ "somebody the role with user_set_roles.");
			} else {
				text.append(" Assigning to a role gives the record to **one** person: the holders "
						+ "are taken in alphabetical order and the first who may see that record "
						+ "gets it. ").append(holders.size())
						.append(holders.size() == 1 ? " person holds it: " : " people hold it: ")
						.append(String.join(", ", holders)).append(".");
			}
		}

		text.append(n.enabled
				? "\n\nIt is switched on, so the change applies to the next matching submission."
				: "\n\nIt is switched off and assigns nothing until notification_enable turns it on.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/* How the stored assignee reads to somebody, which is not how it is stored */
	private String describeAssignee(McpToolContext ctx, String remoteUser, int oId) throws Exception {
		if(remoteUser == null || remoteUser.trim().isEmpty()) {
			return "";
		}
		if("_submitter".equals(remoteUser)) {
			return "whoever submitted it";
		}
		if(remoteUser.startsWith("_role:")) {
			try {
				int roleId = Integer.parseInt(remoteUser.substring("_role:".length()).trim());
				try (PreparedStatement pstmt = ctx.sd.prepareStatement(
						"select name from role where id = ? and o_id = ?")) {
					pstmt.setInt(1, roleId);
					pstmt.setInt(2, oId);
					ResultSet rs = pstmt.executeQuery();
					if(rs.next()) {
						return "the role " + rs.getString(1);
					}
				}
			} catch (NumberFormatException e) {
				// Fall through and report it as it is stored
			}
			return remoteUser;
		}
		return remoteUser;
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

	private boolean userExists(McpToolContext ctx, String ident, int oId) throws Exception {
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select count(*) from users where ident = ? and o_id = ?")) {
			pstmt.setString(1, ident);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() && rs.getInt(1) > 0;
		}
	}

	private boolean surveyIdentExists(McpToolContext ctx, String ident, int oId) throws Exception {
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select count(*) from survey s, project p "
				+ "where s.p_id = p.id and p.o_id = ? and s.ident = ? and not s.deleted")) {
			pstmt.setInt(1, oId);
			pstmt.setString(2, ident);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() && rs.getInt(1) > 0;
		}
	}
}
