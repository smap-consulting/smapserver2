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
import org.smap.sdal.model.Project;
import org.smap.sdal.model.User;

/*
 * Who is in this organisation.
 *
 * Reports what each person is and what they can reach - their groups and their projects - because
 * "who can see this survey" and "who could I assign this to" are the questions actually asked of a
 * user list, and neither is answerable from names alone.
 *
 * Reading who holds what is not the same as changing it.  This lists; nothing here grants anything,
 * and the tools that would are a separate increment behind a separate permission, because widening
 * somebody's access is the one kind of change that lets every other kind happen unnoticed.
 */
public class UserListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "user_list";
	}

	@Override
	public String getTitle() {
		return "Users";
	}

	@Override
	public String getDescription() {
		return "The people in this organisation: username, name, email, what they are allowed to do and "
				+ "which projects they belong to, and which roles they hold. Listing only - nothing "
				+ "here changes anyone's access.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ADMIN;
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"project_id", property("integer",
						"Optional. Only people who are members of this project, from project_list."),
				"contains", property("string",
						"Optional. Only people whose username, name or email mentions this."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		int projectId = intArg(arguments, "project_id", 0);
		String contains = stringArg(arguments, "contains");
		if(contains != null) {
			contains = contains.trim().toLowerCase();
			if(contains.isEmpty()) {
				contains = null;
			}
		}

		/*
		 * Asked for as an administrator of this organisation and nothing more.  getUserList takes
		 * flags for an organisational user and a security manager, which widen what comes back to
		 * people in other organisations and to security detail; neither is this caller's business
		 * here, and a list tool that quietly answered more than it was asked is how a boundary stops
		 * being one.
		 */
		ArrayList<User> users = new UserManager(ctx.localisation).getUserList(ctx.sd, oId,
				false,			// isOrgUser
				false,			// isSecurityManager
				true,			// isAdminUser
				ctx.user);

		/*
		 * Roles, read straight from user_role rather than taken from the User objects.
		 *
		 * getUserList builds an empty roles list and fills it only when it was told the caller is an
		 * organisation administrator or a security manager, which this is not - so every user would
		 * come back holding none.  Not null, empty, which reads as an answer.
		 *
		 * One query for everybody rather than one per user: this list is often the whole
		 * organisation.
		 */
		Map<String, List<String>> rolesByUser = new LinkedHashMap<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select u.ident, r.name from users u, user_role ur, role r "
				+ "where ur.u_id = u.id and ur.r_id = r.id "
				+ "and r.o_id = ? "
				+ "order by u.ident, r.name")) {
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String ident = rs.getString(1);
				List<String> list = rolesByUser.get(ident);
				if(list == null) {
					list = new ArrayList<>();
					rolesByUser.put(ident, list);
				}
				list.add(rs.getString(2));
			}
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		for(User u : users) {
			List<String> projects = new ArrayList<>();
			boolean inProject = projectId <= 0;
			if(u.projects != null) {
				for(Project p : u.projects) {
					projects.add(p.name);
					if(p.id == projectId) {
						inProject = true;
					}
				}
			}
			if(!inProject) {
				continue;
			}
			if(contains != null) {
				String haystack = ((u.ident == null ? "" : u.ident) + " "
						+ (u.name == null ? "" : u.name) + " "
						+ (u.email == null ? "" : u.email)).toLowerCase();
				if(!haystack.contains(contains)) {
					continue;
				}
			}

			Map<String, Object> row = new LinkedHashMap<>();
			row.put("username", u.ident);
			row.put("name", u.name);
			row.put("email", u.email);
			row.put("projects", projects);

			List<String> allowed = new ArrayList<>();
			if(u.groups != null) {
				for(org.smap.sdal.model.UserGroup g : u.groups) {
					allowed.add(g.name);
				}
			}
			row.put("allowedTo", allowed);
			/*
			 * Which records they see, as distinct from what they may do.  Reported because
			 * user_set_roles replaces the whole list, and there was no way to read what was about to
			 * be replaced.
			 */
			List<String> roles = rolesByUser.get(u.ident);
			row.put("roles", roles == null ? new ArrayList<String>() : roles);
			rows.add(row);

			text.append("\n- ").append(u.ident);
			if(u.name != null && !u.name.equals(u.ident)) {
				text.append(" (").append(u.name).append(")");
			}
			if(!allowed.isEmpty()) {
				text.append(": ").append(String.join(", ", allowed));
			}
			if(roles != null && !roles.isEmpty()) {
				text.append("\n    roles: ").append(String.join(", ", roles));
			}
			if(!projects.isEmpty()) {
				text.append(" - in ").append(String.join(", ", projects));
			} else {
				text.append(" - in no project, so they can reach no surveys");
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append("No users match.");
		} else {
			text.insert(0, rows.size() + " user(s):");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("users", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
