package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.User;

/*
 * Which roles somebody holds, and so which records they see.
 *
 * Roles are only written for an organisation administrator or a security manager - that condition is
 * inside insertUserGroupsProjects, and a plain administrator calling it changes nothing while
 * everything appears to have worked.  So the group is checked here and the call refused, rather than
 * leaving somebody to discover from a support ticket that the role they granted was never granted.
 *
 * Replaces the list, like its neighbours.  Removing somebody's last role on a survey where roles are
 * what grant access takes their access away, which is not what "set their roles" sounds like it
 * might do, so the answer says what was removed and not only what was set.
 */
public class UserSetRolesTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "user_set_roles";
	}

	@Override
	public String getTitle() {
		return "Set a user's roles";
	}

	@Override
	public String getDescription() {
		return "Sets which roles somebody holds, which decides the records they see inside the "
				+ "surveys they can reach. THIS REPLACES THE WHOLE LIST - roles left out are taken "
				+ "away. role_list shows the roles and what each filters. Needs an organisation "
				+ "administrator or a security manager. You cannot change your own.";
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
		return "user_set_roles again with the previous list, which this returns";
	}

	@Override
	public String getSelfProtectedArgument() {
		return "username";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ORG, Authorise.SECURITY, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> roles = property("array",
				"The complete list of role names they should hold. An empty list takes them all "
						+ "away.");
		roles.put("items", property("string", "A role name, from role_list"));

		Map<String, Object> schema = schema(
				"username", property("string", "The person to change, from user_list"),
				"roles", roles);
		schema.put("required", new String[] { "username", "roles" });
		return schema;
	}

	@SuppressWarnings("unchecked")
	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String username = stringArg(arguments, "username");
		if(username == null || username.trim().isEmpty()) {
			return new MCPToolResult("A username is required. Use user_list to find one.", true);
		}
		username = username.trim();

		Object wanted = arguments.get("roles");
		if(!(wanted instanceof List)) {
			return new MCPToolResult("roles has to be a list of role names - the complete list they "
					+ "should hold, not just the ones to add.", true);
		}

		boolean isOrgUser = GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ORG_ID);
		boolean isSecurityManager =
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.SECURITY_ID);
		if(!isOrgUser && !isSecurityManager) {
			/*
			 * Refused rather than attempted.  insertUserGroupsProjects ignores the role list for
			 * anybody else, so this would otherwise report a change it had not made.
			 */
			return new MCPToolResult("Changing somebody's roles needs an organisation administrator "
					+ "or a security manager. Nothing was changed.", true);
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		UserManager um = new UserManager(ctx.localisation);

		User found = null;
		for(User u : um.getUserList(ctx.sd, oId, false, false, true, ctx.user)) {
			if(username.equalsIgnoreCase(u.ident)) {
				found = u;
				break;
			}
		}
		if(found == null) {
			return new MCPToolResult("There is no user called " + username + " in this "
					+ "organisation. user_list shows who there is.", true);
		}

		Map<String, Integer> byName = new LinkedHashMap<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select id, name from role where o_id = ?")) {
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				byName.put(rs.getString(2).toLowerCase(), rs.getInt(1));
			}
		}

		ArrayList<Role> toSet = new ArrayList<>();
		List<String> unknown = new ArrayList<>();
		List<String> names = new ArrayList<>();
		for(Object o : (List<Object>) wanted) {
			if(o == null) {
				continue;
			}
			String name = o.toString().trim();
			if(name.isEmpty()) {
				continue;
			}
			Integer id = byName.get(name.toLowerCase());
			if(id == null) {
				unknown.add(name);
			} else if(!names.contains(name)) {
				names.add(name);
				Role r = new Role();
				r.id = id;
				r.name = name;
				toSet.add(r);
			}
		}
		if(!unknown.isEmpty()) {
			return new MCPToolResult("There is no role called " + String.join(", ", unknown)
					+ " in this organisation. role_list shows what there is. Nothing was changed.",
					true);
		}

		/*
		 * Read straight from user_role rather than from the User object.
		 *
		 * getUserList builds an empty roles list and then only fills it when it was told the caller
		 * is an organisation administrator or a security manager.  Ask it any other way and every
		 * user comes back holding no roles - not null, empty, which reads as an answer rather than as
		 * a question that was never asked.
		 *
		 * Taking that at face value cost a real role in testing: previous came back empty, removed
		 * came back empty, and the role this tool exists to report the loss of was removed without
		 * the answer mentioning it.  The whole point of a tool that replaces a list is that it can
		 * say what it replaced, so the list it compares against has to be the real one.
		 */
		List<String> before = new ArrayList<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select r.name from user_role ur, role r "
				+ "where ur.r_id = r.id and ur.u_id = ? and r.o_id = ? order by r.name asc")) {
			pstmt.setInt(1, found.id);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				before.add(rs.getString(1));
			}
		}

		List<String> added = new ArrayList<>();
		List<String> removed = new ArrayList<>();
		for(String n : names) {
			if(!before.contains(n)) {
				added.add(n);
			}
		}
		for(String n : before) {
			if(!names.contains(n)) {
				removed.add(n);
			}
		}
		if(added.isEmpty() && removed.isEmpty()) {
			return new MCPToolResult(found.ident + " already holds exactly those roles, so nothing "
					+ "was changed.", false);
		}

		um.setUserRoles(ctx.sd, found.id, toSet, isOrgUser, isSecurityManager,
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ENTERPRISE_ID),
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.OWNER_ID));

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("username", found.ident);
		data.put("roles", names);
		data.put("added", added);
		data.put("removed", removed);
		data.put("previous", before);

		StringBuilder text = new StringBuilder();
		text.append(found.ident).append(" now holds ")
				.append(names.isEmpty() ? "no roles" : String.join(", ", names)).append(".");
		if(!added.isEmpty()) {
			text.append("\n- added: ").append(String.join(", ", added));
		}
		if(!removed.isEmpty()) {
			text.append("\n- removed: ").append(String.join(", ", removed));
		}
		if(names.isEmpty() && !before.isEmpty()) {
			text.append("\n\nThey hold no role now. On a survey where a role is what grants access, "
					+ "they will see nothing there at all.");
		}
		text.append("\n\nThey held: ")
				.append(before.isEmpty() ? "no roles" : String.join(", ", before));
		text.append("\nTheir groups and projects are unchanged.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
