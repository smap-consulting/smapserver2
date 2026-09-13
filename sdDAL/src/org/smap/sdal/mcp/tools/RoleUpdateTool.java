package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * Rename a role, or change what it says it is for.
 *
 * Only those two things, and the reason is worth stating.  The ordinary role update finishes by
 * calling setUsersForRole, which deletes every holder of the role before looking at the list it was
 * given, and does that even when there is no list.  Renaming a role through it would take the role
 * away from everybody who had it.
 *
 * And that failure is silent in the way that matters most: a role decides which records its holders
 * see, so nobody is refused anything - they quietly see fewer records, or none, with nothing to
 * connect it to a rename.  So this uses a narrow manager method that writes the two columns.
 *
 * Who holds the role is changed with user_set_roles.  The row filter is set in the console.
 */
public class RoleUpdateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "role_update";
	}

	@Override
	public String getTitle() {
		return "Rename a role";
	}

	@Override
	public String getDescription() {
		return "Changes a role's name or description. Who holds it and what it filters are "
				+ "untouched. Returns the previous values.";
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
		return "role_update again with the previous name, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ORG, Authorise.SECURITY, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"role", property("string", "The role to change, from role_list"),
				"name", property("string", "Optional. What it should be called."),
				"description", property("string", "Optional. What it is for."));
		schema.put("required", new String[] { "role" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String role = stringArg(arguments, "role");
		if(role == null || role.trim().isEmpty()) {
			return new MCPToolResult("A role is required. Use role_list to find one.", true);
		}
		role = role.trim();

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		int rId = 0;
		String wasName = null;
		String wasDesc = null;
		int holders = 0;
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select r.id, r.name, r.description, "
				+ "(select count(*) from user_role ur where ur.r_id = r.id) as holders "
				+ "from role r where r.o_id = ? and r.name = ?")) {
			pstmt.setInt(1, oId);
			pstmt.setString(2, role);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				rId = rs.getInt(1);
				wasName = rs.getString(2);
				wasDesc = rs.getString(3);
				holders = rs.getInt(4);
			}
		}
		if(rId <= 0) {
			return new MCPToolResult("There is no role called \"" + role + "\" in this "
					+ "organisation. role_list shows what there is.", true);
		}

		String name = stringArg(arguments, "name");
		String description = stringArg(arguments, "description");
		if(name == null && description == null) {
			return new MCPToolResult("Give a name or a description to change.", true);
		}
		if(name == null) {
			name = wasName;
		}
		name = name.trim();
		if(name.isEmpty()) {
			return new MCPToolResult("A role cannot have an empty name.", true);
		}
		if(description == null) {
			description = wasDesc;
		}

		if(!name.equalsIgnoreCase(wasName)) {
			if(GeneralUtilityMethods.getRoleId(ctx.sd, name, oId) > 0) {
				return new MCPToolResult("There is already a role called \"" + name + "\" in this "
						+ "organisation.", true);
			}
		}

		Map<String, Object> previous = new LinkedHashMap<>();
		previous.put("name", wasName);
		previous.put("description", wasDesc == null ? "" : wasDesc);

		new RoleManager(ctx.localisation)
				.updateRoleDetails(ctx.sd, rId, oId, name, description, ctx.user);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("role_id", rId);
		data.put("name", name);
		data.put("description", description == null ? "" : description);
		data.put("holders", holders);
		data.put("previous", previous);

		StringBuilder text = new StringBuilder();
		if(!name.equals(wasName)) {
			text.append("Renamed \"").append(wasName).append("\" to \"").append(name).append("\".");
		} else {
			text.append("Updated \"").append(name).append("\".");
		}
		text.append(" The ").append(holders).append(holders == 1 ? " person who holds" : " people who hold")
				.append(" it still ").append(holders == 1 ? "does" : "do")
				.append(", and it filters the same records.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
