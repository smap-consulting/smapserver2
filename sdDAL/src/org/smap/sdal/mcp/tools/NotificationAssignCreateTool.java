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
import org.smap.sdal.model.Survey;

/*
 * A rule that hands each new submission to somebody as a case.
 *
 * This is the step every workflow in Smap is built out of, and the one thing MCP could not make.
 * notification_create sends an email to fixed addresses; this assigns work, which is what a case
 * management system is - a form arrives, it becomes somebody's to deal with, and when they finish it
 * moves on.  Without it a survey could be designed here and its workflow could not.
 *
 * **Assigning to a role does not assign to the role.**  Worth stating plainly because the word says
 * otherwise: when the rule fires, Smap takes the users who hold that role, in alphabetical order, and
 * gives the record to the first one the row filters allow to see it.  If nobody holding the role can
 * see the record - or nobody holds it at all - the case is created and left **unassigned**, with no
 * error and nothing to say why.  A role with no holders is the commonest cause of a queue of
 * unassigned cases, and it is invisible from the workflow, which shows the rule and not whether
 * anybody is behind it.  So this checks, and says so.
 *
 * Created switched off, like every other notification here.  A rule that assigns work starts doing so
 * on the next submission, and somebody should read it first.
 */
public class NotificationAssignCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "notification_create_assignment";
	}

	@Override
	public String getTitle() {
		return "Assign submissions to somebody";
	}

	@Override
	public String getDescription() {
		return "Creates a rule that turns each new submission into a case and assigns it - to a "
				+ "role, a named person, or whoever submitted it. This is how a workflow is built: "
				+ "the step that gives work to somebody. Created SWITCHED OFF; turn it on with "
				+ "notification_enable. Assigning to a role gives the record to one person holding "
				+ "it, not to all of them.";
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
		return "notification_delete, or notification_enable to leave it in place but switched off";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ANALYST, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer",
						"The survey whose submissions set this off, from survey_list"),
				"name", property("string", "What this step is called, as shown on the workflow"),
				"assign_to", property("string",
						"A role name from role_list, a username from user_list, or \"submitter\" "
								+ "for whoever filled the form in."),
				"only_when", property("string",
						"Optional. An expression over question names, such as ${unit} = 'faso', so "
								+ "this only fires for some submissions."),
				"case_survey", property("string",
						"Optional. The ident of the survey the case belongs to, when the case is "
								+ "not in the survey being submitted. survey_list gives idents."));
		schema.put("required", new String[] { "survey_id", "name", "assign_to" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}

		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required - it is what this step is called on the "
					+ "workflow.", true);
		}
		name = name.trim();

		String assignTo = stringArg(arguments, "assign_to");
		if(assignTo == null || assignTo.trim().isEmpty()) {
			return new MCPToolResult("assign_to is required: a role, a username, or \"submitter\".",
					true);
		}
		assignTo = assignTo.trim();

		Survey listed = McpData.surveyById(ctx, surveyId);
		if(listed == null) {
			return new MCPToolResult("No survey with id " + surveyId + " that you can reach.", true);
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/*
		 * Who it goes to, resolved here so an unknown name is refused now rather than becoming a rule
		 * that silently assigns to nobody every time it fires.
		 */
		String remoteUser;
		String assignedDescription;
		List<String> holders = new ArrayList<>();
		boolean isRole = false;

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
						+ "organisation nor a user in it. role_list and user_list show what there "
						+ "is, and \"submitter\" assigns to whoever filled the form in.", true);
			}
		}

		String caseSurvey = stringArg(arguments, "case_survey");
		if(caseSurvey != null) {
			caseSurvey = caseSurvey.trim();
			if(caseSurvey.isEmpty()) {
				caseSurvey = null;
			} else if(!surveyIdentExists(ctx, caseSurvey, oId)) {
				return new MCPToolResult("No survey in this organisation has the ident \""
						+ caseSurvey + "\". survey_list reports the ident of each.", true);
			}
		}

		Notification n = new Notification();
		n.name = name;
		n.s_id = surveyId;
		n.sIdent = listed.getIdent();
		n.trigger = "submission";
		n.target = "escalate";
		n.enabled = false;			// Never on at creation - a rule that assigns work starts at once
		n.filter = stringArg(arguments, "only_when");
		n.remote_user = remoteUser;
		n.p_id = GeneralUtilityMethods.getProjectIdFromSurveyIdent(ctx.sd, listed.getIdent());

		NotifyDetails nd = new NotifyDetails();
		nd.survey_case = caseSurvey == null ? listed.getIdent() : caseSurvey;
		/*
		 * "-1" where no question supplies an address, which is what the rest of Smap writes and what
		 * the notification code reads back.  An escalate sends nothing, but the record is shared.
		 */
		nd.emailQuestionName = "-1";
		n.notifyDetails = nd;

		int id = new NotificationManager(ctx.localisation)
				.addNotification(ctx.sd, null, ctx.user, n, ctx.timezone);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("notification_id", id);
		data.put("name", name);
		data.put("survey", listed.getDisplayName());
		data.put("assignedTo", assignedDescription);
		data.put("enabled", Boolean.FALSE);
		data.put("onlyWhen", n.filter == null ? "" : n.filter);
		if(isRole) {
			data.put("roleHolders", holders);
		}

		StringBuilder text = new StringBuilder();
		text.append("Created \"").append(name).append("\" on ").append(listed.getDisplayName())
				.append(": each new submission becomes a case assigned to ")
				.append(assignedDescription).append(".");
		if(n.filter != null && !n.filter.trim().isEmpty()) {
			text.append("\n\nOnly when ").append(n.filter.trim()).append(".");
		}

		if(isRole) {
			/*
			 * The gap between what "assign to a role" sounds like and what it does.  Said every time
			 * rather than only when it looks wrong, because the reader is usually building a workflow
			 * and this is the part that decides whether it will work.
			 */
			text.append("\n\nAssigning to a role gives the record to **one** person: the users "
					+ "holding it are taken in alphabetical order and the first who is allowed to "
					+ "see that record gets it.");
			if(holders.isEmpty()) {
				text.append("\n\n**Nobody holds ").append(assignTo)
						.append(".** Every submission matching this will create a case and leave it "
								+ "unassigned, with nothing reported. Give somebody the role with "
								+ "user_set_roles before switching this on.");
			} else {
				text.append(" ").append(holders.size())
						.append(holders.size() == 1 ? " person holds it: " : " people hold it: ")
						.append(String.join(", ", holders))
						.append(". A record none of them may see is left unassigned.");
			}
		}

		text.append("\n\nIt is SWITCHED OFF and assigns nothing until notification_enable turns it "
				+ "on. workflow_list shows where it sits once it is on.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
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

	/* Who actually holds the role, which is what decides whether this rule can assign anything */
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
