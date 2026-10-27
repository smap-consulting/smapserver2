package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * What happened to each submission: the Submitted tab of the console's monitor page.
 *
 * Every upload has a row in upload_event recording whether it arrived and whether the subscriber
 * then applied it to the results database.  A submission that failed to apply never reaches the
 * results table, so data_query and data_count cannot see it at all - this is the only place the
 * failure and its reason are visible.
 *
 * Access follows the monitor page: the caller sees uploads for projects they are a member of, in
 * their own organisation.
 *
 * Each row gets one state, worked out the way the monitor page shows it.  An upload that failed to
 * arrive is an upload_error whatever else it holds; one that arrived but has no result from the
 * subscriber yet is not_loaded; otherwise the subscriber's own status decides, with an error whose
 * reason starts "Duplicate survey:" reported as a duplicate.
 */
public class SubmissionEventsTool extends AbstractMcpTool {

	private static final List<String> STATES = Arrays.asList(
			"success", "error", "duplicate", "merged", "not_loaded", "upload_error");

	/* The condition selecting each state.  Kept in step with the monitor page's totals */
	private static String condition(String state) {
		switch(state) {
		case "success":
			return "ue.status is distinct from 'error' and ue.db_status = 'success'";
		case "error":
			return "ue.status is distinct from 'error' and ue.db_status = 'error' "
					+ "and (ue.db_reason is null or ue.db_reason not like 'Duplicate survey:%')";
		case "duplicate":
			return "ue.status is distinct from 'error' and ue.db_status = 'error' "
					+ "and ue.db_reason like 'Duplicate survey:%'";
		case "merged":
			return "ue.status is distinct from 'error' and ue.db_status = 'merged'";
		case "not_loaded":
			return "ue.status is distinct from 'error' and ue.db_status is null";
		case "upload_error":
			return "ue.status = 'error'";
		default:
			return null;
		}
	}

	@Override
	public String getName() {
		return "submission_events";
	}

	@Override
	public String getTitle() {
		return "Submission processing";
	}

	@Override
	public String getDescription() {
		return "Whether each submission was applied to the database, and why not if it failed - the "
				+ "Submitted tab of the console's monitor page. Newest first, with totals per state. "
				+ "A submission that failed is not in data_query or data_count; it is only here.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> states = new LinkedHashMap<>();
		states.put("type", "array");
		states.put("description", "Optional. Only these states: success, error, duplicate, merged, "
				+ "not_loaded (arrived, not yet applied), upload_error (did not arrive). Omit for all.");
		Map<String, Object> items = new LinkedHashMap<>();
		items.put("type", "string");
		items.put("enum", STATES);
		states.put("items", items);

		return schema(
				"survey_id", property("integer", "Optional. Only this survey. Omit for every survey "
						+ "in the projects you belong to."),
				"project_id", property("integer", "Optional. Only surveys in this project."),
				"states", states,
				"ignore_old", property("boolean", "Optional. Leave out uploads more than 100 days old, "
						+ "as the monitor page does by default. Default false."),
				"limit", property("integer", "Optional. Maximum uploads to return. Default 50."),
				"cursor", property("integer", "Optional. next_cursor from a previous call, to read "
						+ "older uploads."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		int projectId = intArg(arguments, "project_id", 0);
		boolean ignoreOld = boolArg(arguments, "ignore_old", false);
		int limit = ctx.cap(intArg(arguments, "limit", 50));
		int cursor = intArg(arguments, "cursor", 0);

		Set<String> wanted = new LinkedHashSet<>();
		Object statesArg = arguments.get("states");
		if(statesArg instanceof List) {
			for(Object o : (List<?>) statesArg) {
				String s = o == null ? "" : o.toString().trim().toLowerCase();
				if(!STATES.contains(s)) {
					return new MCPToolResult("\"" + o + "\" is not a state. Use any of "
							+ String.join(", ", STATES) + ".", true);
				}
				wanted.add(s);
			}
		} else if(statesArg != null) {
			return new MCPToolResult("states is a list, such as [\"error\"].", true);
		}

		/*
		 * The survey is matched on its ident rather than its id, because uploads keep the ident
		 * when a survey is replaced, as the monitor page does
		 */
		String surveyIdent = null;
		if(surveyId > 0) {
			surveyIdent = GeneralUtilityMethods.getSurveyIdent(ctx.sd, surveyId);
			if(surveyIdent == null) {
				return new MCPToolResult("There is no survey " + surveyId + ".", true);
			}
		}

		/*
		 * The caller's own projects in their own organisation.  A survey or project outside them
		 * simply matches nothing, so there is no different answer to give away whether it exists.
		 */
		StringBuilder scope = new StringBuilder("from upload_event ue "
				+ "join user_project up on ue.p_id = up.p_id "
				+ "join users u on up.u_id = u.id "
				+ "join project p on up.p_id = p.id "
				+ "where u.ident = ? "
				+ "and p.o_id = ? ");
		if(projectId > 0) {
			scope.append("and ue.p_id = ? ");
		}
		if(surveyIdent != null) {
			scope.append("and ue.ident = ? ");
		}
		if(ignoreOld) {
			scope.append("and ue.upload_time > now() - interval '100 days' ");
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		String tz = (ctx.timezone == null || ctx.timezone.trim().isEmpty()) ? "UTC" : ctx.timezone;

		/*
		 * Totals over the whole scope, before states and the cursor narrow it, so the caller can
		 * see how many failures there are while reading a page of something else
		 */
		Map<String, Object> totals = new LinkedHashMap<>();
		StringBuilder totalsSql = new StringBuilder("select ");
		for(int i = 0; i < STATES.size(); i++) {
			if(i > 0) {
				totalsSql.append(", ");
			}
			totalsSql.append("count(*) filter (where ").append(condition(STATES.get(i)))
					.append(") as ").append(STATES.get(i));
		}
		totalsSql.append(" ").append(scope);

		try (PreparedStatement pstmt = ctx.sd.prepareStatement(totalsSql.toString())) {
			bindScope(pstmt, 1, ctx.user, oId, projectId, surveyIdent);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				for(String s : STATES) {
					totals.put(s, rs.getInt(s));
				}
			}
		}

		StringBuilder sql = new StringBuilder("select ue.ue_id, ue.instanceid, ue.ident, ue.s_id, "
				+ "ue.survey_name, ue.user_name, ue.imei, ue.file_name, ue.incomplete, "
				+ "ue.status, ue.reason, ue.db_status, ue.db_reason, "
				+ "to_char(timezone(?, ue.upload_time), 'YYYY-MM-DD HH24:MI:SS') as upload_time ")
				.append(scope);
		if(!wanted.isEmpty()) {
			sql.append("and (");
			int i = 0;
			for(String s : wanted) {
				if(i++ > 0) {
					sql.append(" or ");
				}
				sql.append("(").append(condition(s)).append(")");
			}
			sql.append(") ");
		}
		if(cursor > 0) {
			sql.append("and ue.ue_id < ? ");
		}
		sql.append("order by ue.ue_id desc limit ?");

		List<Map<String, Object>> rows = new ArrayList<>();
		Map<Integer, String> surveyNames = new LinkedHashMap<>();
		boolean more = false;
		int lastId = 0;

		try (PreparedStatement pstmt = ctx.sd.prepareStatement(sql.toString())) {
			int idx = 1;
			pstmt.setString(idx++, tz);
			idx = bindScope(pstmt, idx, ctx.user, oId, projectId, surveyIdent);
			if(cursor > 0) {
				pstmt.setInt(idx++, cursor);
			}
			pstmt.setInt(idx++, limit + 1);		// One extra says whether there is another page

			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				if(rows.size() >= limit) {
					more = true;
					break;
				}
				String uploadStatus = rs.getString("status");
				String dbStatus = rs.getString("db_status");
				String dbReason = rs.getString("db_reason");

				String state;
				String reason;
				if("error".equals(uploadStatus)) {
					state = "upload_error";
					reason = rs.getString("reason");
				} else if(dbStatus == null) {
					state = "not_loaded";
					reason = "Not yet added to the database";
				} else if("error".equals(dbStatus) && dbReason != null
						&& dbReason.startsWith("Duplicate survey:")) {
					state = "duplicate";
					reason = dbReason;
				} else {
					state = dbStatus;
					reason = dbReason;
				}

				/* The current name, or the one recorded at upload if the survey has been erased */
				int sId = rs.getInt("s_id");
				String surveyName = surveyNames.get(sId);
				if(surveyName == null) {
					surveyName = GeneralUtilityMethods.getSurveyName(ctx.sd, sId);
					if(surveyName == null) {
						surveyName = rs.getString("survey_name");
					}
					surveyNames.put(sId, surveyName == null ? "" : surveyName);
				}

				lastId = rs.getInt("ue_id");
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("upload_id", lastId);
				row.put("upload_time", nz(rs.getString("upload_time")));
				row.put("state", state);
				row.put("reason", nz(reason));
				row.put("survey", surveyNames.get(sId));
				row.put("survey_ident", nz(rs.getString("ident")));
				row.put("instanceid", nz(rs.getString("instanceid")));
				row.put("user", nz(rs.getString("user_name")));
				row.put("device", nz(rs.getString("imei")));
				row.put("file", nz(rs.getString("file_name")));
				row.put("complete", !rs.getBoolean("incomplete"));
				rows.add(row);
			}
		}

		StringBuilder text = new StringBuilder();
		text.append("Totals: ");
		int t = 0;
		for(String s : STATES) {
			if(t++ > 0) {
				text.append(", ");
			}
			text.append(s).append(" ").append(totals.get(s));
		}
		text.append(".\n");
		if(rows.isEmpty()) {
			text.append("\nNo uploads match.");
		} else {
			text.append("\n").append(rows.size()).append(" upload(s), newest first (times in ")
					.append(tz).append("):");
			for(Map<String, Object> row : rows) {
				text.append("\n- ").append(row.get("upload_id")).append("  ")
						.append(row.get("upload_time")).append("  ").append(row.get("survey"))
						.append("  ").append(row.get("state"));
				String reason = (String) row.get("reason");
				if(!reason.isEmpty() && !"success".equals(row.get("state"))) {
					text.append("\n    ").append(reason);
				}
			}
			if(more) {
				text.append("\n\nMore remain: pass cursor ").append(lastId).append(".");
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("totals", totals);
		data.put("uploads", rows);
		data.put("count", rows.size());
		data.put("timezone", tz);
		if(more) {
			data.put("next_cursor", lastId);
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/* Bind the parameters of the scope clause in the order it was built.  Returns the next index */
	private int bindScope(PreparedStatement pstmt, int idx, String user, int oId, int projectId,
			String surveyIdent) throws Exception {
		pstmt.setString(idx++, user);
		pstmt.setInt(idx++, oId);
		if(projectId > 0) {
			pstmt.setInt(idx++, projectId);
		}
		if(surveyIdent != null) {
			pstmt.setString(idx++, surveyIdent);
		}
		return idx;
	}

	private static String nz(String s) {
		return s == null ? "" : s;
	}
}
