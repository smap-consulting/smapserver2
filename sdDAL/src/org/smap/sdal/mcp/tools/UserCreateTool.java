package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.User;

/*
 * Add somebody to this organisation.
 *
 * **No email is sent.**  Everything else here follows the same rule - nothing in this server reaches
 * out of it on an agent's say so - and a welcome message is exactly the kind of thing that cannot be
 * recalled once it has gone to the wrong address.  So the account is made and the answer says how the
 * person gets a password: the forgotten password page if they have an email, or an administrator
 * setting one in the console.
 *
 * Groups and projects can be given at the same time, because onboarding somebody in three calls
 * invites the second and third to be forgotten, leaving an account that can sign in and do nothing.
 * The group rules are the same ones user_set_groups applies and are checked the same way: a group
 * this administrator cannot grant refuses the whole call, before the user exists, rather than
 * creating them and silently leaving the permission off.
 */
public class UserCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "user_create";
	}

	@Override
	public String getTitle() {
		return "Add a user";
	}

	@Override
	public String getDescription() {
		return "Adds somebody to this organisation, optionally with groups and projects. NO EMAIL IS "
				+ "SENT - they get in by using the forgotten password page, or an administrator sets "
				+ "a password in the console. group_list and project_list show what can be given.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ACCESS;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "none from here - removing a user is done in the console, and for somebody in one "
				+ "organisation it is permanent";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> groups = property("array",
				"Optional. Permission groups to give them, from group_list.");
		groups.put("items", property("string", "A group name"));

		Map<String, Object> projects = property("array",
				"Optional. Projects to put them in, from project_list.");
		projects.put("items", property("string", "A project name"));

		Map<String, Object> schema = schema(
				"username", property("string", "What they sign in with. Cannot be changed later."),
				"name", property("string", "Their display name"),
				"email", property("string", "Optional, but without one they cannot use the "
						+ "forgotten password page and an administrator has to set their password."),
				"groups", groups,
				"projects", projects);
		schema.put("required", new String[] { "username", "name" });
		return schema;
	}

	@SuppressWarnings("unchecked")
	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String username = stringArg(arguments, "username");
		if(username == null || username.trim().isEmpty()) {
			return new MCPToolResult("A username is required. It is what they sign in with and it "
					+ "cannot be changed afterwards.", true);
		}
		username = username.trim();

		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required.", true);
		}
		name = name.trim();

		String email = stringArg(arguments, "email");
		if(email != null) {
			email = email.trim();
			if(!email.isEmpty() && !email.matches("[^@\\s]+@[^@\\s]+\\.[^@\\s]+")) {
				return new MCPToolResult("\"" + email + "\" is not an email address.", true);
			}
		}

		UserManager um = new UserManager(ctx.localisation);
		if(!um.isValiduserIdent(username)) {
			return new MCPToolResult("\"" + username + "\" cannot be used as a username.", true);
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/*
		 * Checked across the whole server, not just this organisation.  An ident is what somebody
		 * signs in with, so it has to be unique everywhere; refusing here names the clash instead of
		 * letting a constraint violation come back as a failure that says nothing.
		 */
		if(GeneralUtilityMethods.getUserId(ctx.sd, username) > 0) {
			return new MCPToolResult("There is already a user called " + username + " on this "
					+ "server. Usernames are unique across every organisation.", true);
		}

		boolean isOrgUser = GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ORG_ID);
		boolean isSecurityManager =
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.SECURITY_ID);
		boolean isEnterpriseManager =
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ENTERPRISE_ID);
		boolean isServerOwner =
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.OWNER_ID);
		boolean mcpEnabled = org.smap.sdal.managers.ServerManager.isMcpEnabled(ctx.sd);

		User u = new User();
		u.ident = username;
		u.name = name;
		u.email = email;
		u.sendEmail = false;			// Nothing here sends anything
		u.groups = new ArrayList<>();
		u.projects = new ArrayList<>();
		u.roles = null;

		List<String> groupNames = new ArrayList<>();
		Object wantedGroups = arguments.get("groups");
		if(wantedGroups instanceof List) {
			Map<String, Integer> byName = new LinkedHashMap<>();
			Map<Integer, String> byId = new LinkedHashMap<>();
			try (java.sql.PreparedStatement pstmt =
					ctx.sd.prepareStatement("select id, name from groups")) {
				java.sql.ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					byName.put(rs.getString(2).toLowerCase(), rs.getInt(1));
					byId.put(rs.getInt(1), rs.getString(2));
				}
			}
			List<String> unknown = new ArrayList<>();
			List<String> cannotGrant = new ArrayList<>();
			for(Object o : (List<Object>) wantedGroups) {
				if(o == null) {
					continue;
				}
				String g = o.toString().trim();
				if(g.isEmpty()) {
					continue;
				}
				Integer id = byName.get(g.toLowerCase());
				if(id == null) {
					unknown.add(g);
				} else if(!UserManager.canGrantGroup(id, isOrgUser, isSecurityManager,
						isEnterpriseManager, isServerOwner, mcpEnabled)) {
					cannotGrant.add(byId.get(id));
				} else if(!groupNames.contains(byId.get(id))) {
					groupNames.add(byId.get(id));
					org.smap.sdal.model.UserGroup ug = new org.smap.sdal.model.UserGroup();
					ug.id = id;
					ug.name = byId.get(id);
					u.groups.add(ug);
				}
			}
			if(!unknown.isEmpty()) {
				return new MCPToolResult("There is no group called " + String.join(", ", unknown)
						+ ". group_list shows the groups there are. No user was created.", true);
			}
			if(!cannotGrant.isEmpty()) {
				return new MCPToolResult("You cannot grant " + String.join(", ", cannotGrant)
						+ ", so no user was created. group_list reports who may grant each group.",
						true);
			}
		}

		List<String> projectNames = new ArrayList<>();
		Object wantedProjects = arguments.get("projects");
		if(wantedProjects instanceof List) {
			List<Project> orgProjects = new ProjectManager(ctx.localisation)
					.getProjects(ctx.sd, ctx.user, true, false, null, false, false);
			List<String> unknown = new ArrayList<>();
			for(Object o : (List<Object>) wantedProjects) {
				if(o == null) {
					continue;
				}
				String p = o.toString().trim();
				if(p.isEmpty()) {
					continue;
				}
				Project match = null;
				for(Project candidate : orgProjects) {
					if(p.equalsIgnoreCase(candidate.name)) {
						match = candidate;
						break;
					}
				}
				if(match == null) {
					unknown.add(p);
				} else if(!projectNames.contains(match.name)) {
					projectNames.add(match.name);
					u.projects.add(match);
				}
			}
			if(!unknown.isEmpty()) {
				return new MCPToolResult("This organisation has no project called "
						+ String.join(", ", unknown) + ". project_list shows what there is. No user "
						+ "was created.", true);
			}
		}

		int uId = um.createUser(ctx.sd, u, oId,
				isOrgUser, isSecurityManager, isEnterpriseManager, isServerOwner,
				ctx.user,
				ctx.request.getScheme(),
				ctx.request.getServerName(),
				ctx.user,
				null,
				ctx.localisation);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("user_id", uId);
		data.put("username", username);
		data.put("name", name);
		data.put("email", email == null ? "" : email);
		data.put("groups", groupNames);
		data.put("projects", projectNames);
		data.put("emailSent", Boolean.FALSE);

		StringBuilder text = new StringBuilder();
		text.append("Created ").append(username).append(" (").append(name).append(").");
		text.append("\n- can: ").append(groupNames.isEmpty()
				? "nothing but sign in - give them groups with user_set_groups"
				: String.join(", ", groupNames));
		text.append("\n- projects: ").append(projectNames.isEmpty()
				? "none - they can reach no surveys until they are in one"
				: String.join(", ", projectNames));

		text.append("\n\n**They cannot sign in yet.** No email was sent - nothing here sends "
				+ "anything. ");
		if(email == null || email.isEmpty()) {
			text.append("They have no email address either, so an administrator has to set their "
					+ "password in the console.");
		} else {
			text.append("They set a password from the forgotten password page using ").append(email)
					.append(", or an administrator sets one in the console.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
