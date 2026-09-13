package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;

/*
 * What somebody is allowed to do.
 *
 * The highest blast radius of anything here, and it is built to refuse rather than to try.
 *
 * Two rules that are easy to miss and are both enforced before anything is written.  Not every
 * administrator may grant every group: a plain administrator cannot make somebody a security manager,
 * only a server owner can grant MCP access, and nobody grants server owner through user
 * administration at all.  And the removal side is a different question from the granting side - the
 * delete that precedes the insert leaves out the groups this administrator has no business removing,
 * so a group they cannot grant survives a list that did not mention it.
 *
 * insertUserGroupsProjects **silently skips** a group the caller may not grant.  A tool that simply
 * passed the list through would report success on a change that was half applied, which is why every
 * requested group is checked here first and the whole call refused if any of them fails.  Partly
 * applied is the one outcome worth avoiding: it leaves somebody believing a permission was given.
 *
 * The answer is read back from the database rather than restated from the request, so what it reports
 * is what is true.
 */
public class UserSetGroupsTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "user_set_groups";
	}

	@Override
	public String getTitle() {
		return "Set what a user may do";
	}

	@Override
	public String getDescription() {
		return "Sets which permission groups somebody belongs to. THIS REPLACES THE WHOLE LIST - "
				+ "groups left out are removed, so include everything they should keep. group_list "
				+ "shows the groups and who may grant each; not every administrator may grant every "
				+ "one, and the call is refused rather than partly applied. Returns the previous "
				+ "list. You cannot change your own.";
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
		return "user_set_groups again with the previous list, which this returns";
	}

	@Override
	public String getSelfProtectedArgument() {
		return "username";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> groups = property("array",
				"The complete list of group names they should have. An empty list leaves them able "
						+ "to do nothing but sign in.");
		groups.put("items", property("string", "A group name, from group_list"));

		Map<String, Object> schema = schema(
				"username", property("string", "The person to change, from user_list"),
				"groups", groups);
		schema.put("required", new String[] { "username", "groups" });
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

		Object wanted = arguments.get("groups");
		if(!(wanted instanceof List)) {
			return new MCPToolResult("groups has to be a list of group names - the complete list "
					+ "they should have, not just the ones to add.", true);
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
		Map<Integer, String> byId = new LinkedHashMap<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement("select id, name from groups")) {
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				byName.put(rs.getString(2).toLowerCase(), rs.getInt(1));
				byId.put(rs.getInt(1), rs.getString(2));
			}
		}

		ArrayList<UserGroup> toSet = new ArrayList<>();
		List<String> unknown = new ArrayList<>();
		List<String> names = new ArrayList<>();
		List<Integer> ids = new ArrayList<>();
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
			} else if(!ids.contains(id)) {
				ids.add(id);
				names.add(byId.get(id));
				UserGroup g = new UserGroup();
				g.id = id;
				g.name = byId.get(id);
				toSet.add(g);
			}
		}
		if(!unknown.isEmpty()) {
			return new MCPToolResult("There is no group called " + String.join(", ", unknown)
					+ ". group_list shows the groups there are. Nothing was changed.", true);
		}

		boolean isOrgUser = GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ORG_ID);
		boolean isSecurityManager =
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.SECURITY_ID);
		boolean isEnterpriseManager =
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ENTERPRISE_ID);
		boolean isServerOwner =
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.OWNER_ID);
		boolean mcpEnabled = ServerManager.isMcpEnabled(ctx.sd);

		List<String> beforeNames = new ArrayList<>();
		List<Integer> beforeIds = new ArrayList<>();
		if(found.groups != null) {
			for(UserGroup g : found.groups) {
				beforeIds.add(g.id);
				beforeNames.add(byId.containsKey(g.id) ? byId.get(g.id) : g.name);
			}
		}

		/*
		 * Every group the caller is asking to ADD is checked.  One they cannot grant refuses the
		 * whole call rather than being dropped from it - being told "done" about a permission that
		 * was never given is worse than being told no.
		 *
		 * A group the user already has is not an addition, so asking to keep something the caller
		 * could not have granted is allowed.  It changes nothing, which is the test.
		 */
		List<String> cannotGrant = new ArrayList<>();
		for(int i = 0; i < ids.size(); i++) {
			int id = ids.get(i);
			if(beforeIds.contains(id)) {
				continue;
			}
			if(!UserManager.canGrantGroup(id, isOrgUser, isSecurityManager, isEnterpriseManager,
					isServerOwner, mcpEnabled)) {
				cannotGrant.add(names.get(i));
			}
		}
		if(!cannotGrant.isEmpty()) {
			StringBuilder why = new StringBuilder("You cannot grant ")
					.append(String.join(", ", cannotGrant)).append(". Nothing was changed.");
			if(cannotGrant.contains("mcp access")) {
				why.append("\n\nMCP access is granted by a server owner, and only while the MCP "
						+ "server is switched on.");
			}
			why.append("\n\ngroup_list reports who may grant each group.");
			return new MCPToolResult(why.toString(), true);
		}

		/*
		 * And the groups this caller cannot take away, which stay whatever list is sent.  Said before
		 * the change rather than left to be noticed afterwards, because otherwise the answer looks
		 * like the tool ignored part of the request.
		 */
		List<String> keptAnyway = new ArrayList<>();
		for(int i = 0; i < beforeIds.size(); i++) {
			int id = beforeIds.get(i);
			if(ids.contains(id)) {
				continue;
			}
			if(!UserManager.canRemoveGroup(id, isOrgUser, isSecurityManager, isEnterpriseManager,
					isServerOwner, mcpEnabled)) {
				keptAnyway.add(beforeNames.get(i));
			}
		}

		um.setUserGroups(ctx.sd, found.id, toSet,
				isOrgUser, isSecurityManager, isEnterpriseManager, isServerOwner);

		/*
		 * Read back rather than restated.  What is reported is what the database holds, so a group
		 * that was kept, skipped or removed by a rule shows up as it really is.
		 */
		List<String> after = new ArrayList<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select g.name from user_group ug, groups g "
				+ "where ug.g_id = g.id and ug.u_id = ? order by g.id asc")) {
			pstmt.setInt(1, found.id);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				after.add(rs.getString(1));
			}
		}

		List<String> added = new ArrayList<>();
		List<String> removed = new ArrayList<>();
		for(String n : after) {
			if(!beforeNames.contains(n)) {
				added.add(n);
			}
		}
		for(String n : beforeNames) {
			if(!after.contains(n)) {
				removed.add(n);
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", !added.isEmpty() || !removed.isEmpty());
		data.put("username", found.ident);
		data.put("groups", after);
		data.put("added", added);
		data.put("removed", removed);
		data.put("keptBeyondYourReach", keptAnyway);
		data.put("previous", beforeNames);

		StringBuilder text = new StringBuilder();
		if(added.isEmpty() && removed.isEmpty()) {
			text.append(found.ident).append(" already had exactly those groups, so nothing changed.");
		} else {
			text.append(found.ident).append(" can now: ")
					.append(after.isEmpty() ? "nothing but sign in" : String.join(", ", after))
					.append(".");
			if(!added.isEmpty()) {
				text.append("\n- added: ").append(String.join(", ", added));
			}
			if(!removed.isEmpty()) {
				text.append("\n- removed: ").append(String.join(", ", removed));
			}
		}
		if(!keptAnyway.isEmpty()) {
			text.append("\n\n").append(String.join(", ", keptAnyway))
					.append(keptAnyway.size() == 1 ? " was kept" : " were kept")
					.append(" because you cannot remove ")
					.append(keptAnyway.size() == 1 ? "it" : "them")
					.append(". Somebody with more rights has to do that.");
		}
		text.append("\n\nThey were: ")
				.append(beforeNames.isEmpty() ? "in no groups" : String.join(", ", beforeNames));
		text.append("\nTheir projects and roles are unchanged.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
