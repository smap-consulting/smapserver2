package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * The roles in this organisation, and what each of them actually does.
 *
 * A role is not a permission in the sense group_list reports.  Groups say what kind of thing a
 * person may do; a role says which **records** they may see once they are allowed to see any, by
 * attaching a row filter to a survey.  The two are asked about together and mean entirely different
 * things, which is most of the reason this exists.
 *
 * The filters are reported, not just counted.  "Three surveys" says nothing; `q1 = 'rabbit'` says
 * exactly who can see what, and a role whose filter is wrong is invisible from any other angle -
 * nobody is refused anything, they simply never see records that were there all along.
 *
 * Listing only.  Who holds a role is changed with user_set_roles and the filters in the console:
 * editing a filter silently changes what every holder of that role can see, which is a different act
 * from listing it and belongs behind its own confirmation.
 */
public class RoleListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "role_list";
	}

	@Override
	public String getTitle() {
		return "Roles and record filters";
	}

	@Override
	public String getDescription() {
		return "The roles in this organisation: what each is called, how many people hold it, and "
				+ "which surveys it filters records on. A role decides which records somebody sees "
				+ "within a survey they can already reach, which is a different question from what "
				+ "user_list and group_list report. Use it to answer why somebody can see only some "
				+ "of the data.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ADMIN;
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"role", property("string", "Optional. Only the role with this name."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		String wanted = stringArg(arguments, "role");
		if(wanted != null) {
			wanted = wanted.trim();
			if(wanted.isEmpty()) {
				wanted = null;
			}
		}

		/*
		 * Holders counted in the query rather than by reading each role's user list, because the
		 * count is all that is wanted here and the list of names belongs to user_list.
		 */
		String sql = "select r.id, r.name, r.description, r.changed_by, r.changed_ts, "
				+ "(select count(*) from user_role ur where ur.r_id = r.id) as holders "
				+ "from role r "
				+ "where r.o_id = ? "
				+ (wanted == null ? "" : "and r.name = ? ")
				+ "order by r.name asc";

		String sqlSurveys = "select sr.survey_ident, sr.enabled, sr.row_filter, sr.column_filter, "
				+ "s.display_name "
				+ "from survey_role sr "
				+ "left join survey s on s.ident = sr.survey_ident "
				+ "where sr.r_id = ? "
				+ "order by s.display_name asc";

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		try (PreparedStatement pstmt = ctx.sd.prepareStatement(sql);
				PreparedStatement pstmtSurveys = ctx.sd.prepareStatement(sqlSurveys)) {

			pstmt.setInt(1, oId);
			if(wanted != null) {
				pstmt.setString(2, wanted);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				int rId = rs.getInt("id");
				String name = rs.getString("name");
				int holders = rs.getInt("holders");

				Map<String, Object> row = new LinkedHashMap<>();
				row.put("role_id", rId);
				row.put("name", name);
				row.put("description", rs.getString("description") == null
						? "" : rs.getString("description"));
				row.put("holders", holders);

				List<Map<String, Object>> surveys = new ArrayList<>();
				pstmtSurveys.setInt(1, rId);
				ResultSet rsS = pstmtSurveys.executeQuery();
				while(rsS.next()) {
					Map<String, Object> s = new LinkedHashMap<>();
					s.put("survey", rsS.getString("display_name") == null
							? rsS.getString("survey_ident") : rsS.getString("display_name"));
					s.put("ident", rsS.getString("survey_ident"));
					s.put("enabled", rsS.getBoolean("enabled"));
					s.put("rowFilter", rsS.getString("row_filter") == null
							? "" : rsS.getString("row_filter"));
					s.put("columnFilter", rsS.getString("column_filter") == null
							? "" : rsS.getString("column_filter"));
					surveys.add(s);
				}
				row.put("surveys", surveys);
				row.put("changedBy", rs.getString("changed_by") == null
						? "" : rs.getString("changed_by"));
				rows.add(row);

				text.append("\n\n").append(name);
				if(rs.getString("description") != null && !rs.getString("description").isEmpty()) {
					text.append(" - ").append(rs.getString("description"));
				}
				text.append("\n  held by ").append(holders)
						.append(holders == 1 ? " person" : " people");
				if(surveys.isEmpty()) {
					/*
					 * Worth saying rather than leaving blank.  A role attached to no survey filters
					 * nothing, so holding it has no effect at all - which is either a role somebody
					 * has not finished setting up, or one left behind after its survey went.
					 */
					text.append("\n  not attached to any survey, so it filters nothing");
				} else {
					for(Map<String, Object> s : surveys) {
						text.append("\n  - ").append(s.get("survey"));
						if(!Boolean.TRUE.equals(s.get("enabled"))) {
							text.append(" (switched off)");
						}
						String rf = (String) s.get("rowFilter");
						String cf = (String) s.get("columnFilter");
						if(rf != null && !rf.isEmpty()) {
							text.append("\n      records where ").append(rf);
						}
						if(cf != null && !cf.isEmpty()) {
							text.append("\n      columns ").append(cf);
						}
						if((rf == null || rf.isEmpty()) && (cf == null || cf.isEmpty())) {
							text.append("\n      no filter, so every record and column");
						}
					}
				}
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append(wanted == null
					? "No roles are defined in this organisation. Nobody's view of a survey is "
							+ "narrowed by a role, so anybody who can reach a survey sees all of it."
					: "There is no role called \"" + wanted + "\".");
		} else {
			text.insert(0, rows.size() + (rows.size() == 1 ? " role:" : " roles:"));
			text.append("\n\nA role narrows which records somebody sees inside a survey they can "
					+ "already reach. Reaching the survey at all is project membership, which "
					+ "user_list reports.");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("roles", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
