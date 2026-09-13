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
 * reach, and it is not offered here at any price - not even to say which groups this tool could
 * grant, because it cannot grant any.
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
				+ "each of them. Listing only - who belongs to a group is changed in the console.";
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
				/*
				 * The one group worth a word of its own: it is what lets somebody reach this server
				 * the way you are reaching it now, and only a server owner can grant it.
				 */
				if(id == Authorise.MCP_ACCESS_ID) {
					row.put("note", "Lets a person use an AI client against this server. Only a "
							+ "server owner can give it.");
				}
				rows.add(row);

				text.append("\n- ").append(name);
				if(id == Authorise.MCP_ACCESS_ID) {
					text.append("  (lets a person use an AI client here; only a server owner can "
							+ "give it)");
				}
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append("No groups are defined on this server, which should not happen.");
		} else {
			text.insert(0, rows.size() + " permission group(s):");
			text.append("\n\nuser_list shows which of these each person has. Changing who belongs "
					+ "to a group is done in the console.");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("groups", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
