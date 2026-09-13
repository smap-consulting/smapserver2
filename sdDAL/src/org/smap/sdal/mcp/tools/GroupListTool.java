package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * The things a person can be allowed to do.
 *
 * Groups are what user_list reports against each person, and a name on its own does not say much:
 * "manage" and "manage tasks" are different, and neither is "admin".  This lists them so the names in
 * that answer mean something, and so a caller can tell that a group they were about to ask for does
 * not exist.
 *
 * Listing only.  Putting somebody in a group is the change that decides what every other change can
 * reach, and it is not offered here at any price.
 *
 * What each group reports instead is who may grant it, because that is not uniform and the console
 * enforces a hierarchy rather than a single rule.  From insertUserGroupsProjects:
 *
 *   server owner       - never granted this way at all, by any caller
 *   mcp access         - a server owner only, and only while the MCP server is switched on
 *   enterprise admin   - a server owner or an enterprise manager
 *   organisation admin - not a security manager, and not a plain administrator
 *   security, DPO      - not a plain administrator
 *   everything else    - any administrator
 *
 * Reported because a caller who cannot be told this will ask for a group somebody has no power to
 * give, and the refusal, when it comes, comes from the console rather than from here.
 */
public class GroupListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "group_list";
	}

	@Override
	public String getTitle() {
		return "Permission groups";
	}

	@Override
	public String getDescription() {
		return "The permission groups a person can belong to, which is what user_list reports for "
				+ "each of them, and who is allowed to grant each one - not every group can be "
				+ "given by every administrator. Listing only: who belongs to a group is changed "
				+ "in the console.";
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
		return noArguments();
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select id, name from groups order by id asc")) {
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				int id = rs.getInt(1);
				String name = rs.getString(2);

				Map<String, Object> row = new LinkedHashMap<>();
				row.put("name", name);
				String grantableBy = grantableBy(id);
				row.put("grantableBy", grantableBy);
				if(id == Authorise.MCP_ACCESS_ID) {
					row.put("note", "Lets a person use an AI client against this server, the way "
							+ "this connection is being used.");
				}
				rows.add(row);

				text.append("\n- ").append(name).append(" - ").append(grantableBy);
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append("No groups are defined on this server, which should not happen.");
		} else {
			text.insert(0, rows.size() + " permission group(s), and who may grant each:");
			text.append("\n\nuser_list shows which of these each person has. Changing who belongs "
					+ "to a group is done in the console, and the console applies the same rules - "
					+ "asking somebody to grant a group they have no power to give will be refused "
					+ "there.");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("groups", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/*
	 * Who may put somebody in this group, from the rules insertUserGroupsProjects applies.  Said in
	 * the words a person would use rather than as a list of flags, since the point is to tell a
	 * caller whether the change they are about to ask for is one anybody can make.
	 */
	private String grantableBy(int id) {

		if(id == Authorise.OWNER_ID) {
			return "nobody - this group is not granted through user administration at all";
		}
		if(id == Authorise.MCP_ACCESS_ID) {
			return "a server owner, and only while the MCP server is switched on";
		}
		if(id == Authorise.ENTERPRISE_ID) {
			return "a server owner or an enterprise manager";
		}
		if(id == Authorise.ORG_ID) {
			return "a server owner, an organisation administrator or an enterprise manager";
		}
		if(id == Authorise.SECURITY_ID || id == Authorise.DPO_ID) {
			return "anyone above a plain administrator";
		}
		return "any administrator";
	}
}
