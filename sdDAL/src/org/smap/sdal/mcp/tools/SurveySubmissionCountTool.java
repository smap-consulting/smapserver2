package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * How many submissions each survey has had.
 *
 * The prototype version of this tool carried a comment saying the user did not need explicit access
 * to a survey when reaching it through MCP, and checked only that the survey was somewhere in their
 * organisation.  That is the opposite of the rule: an MCP caller gets exactly what the person holds
 * and nothing more.  It happened to fail closed, because the survey list it then filtered was
 * already user scoped, but it failed closed by accident rather than by design.
 *
 * Now the user's own list of surveys is the only source, and a survey_id that is not in it is
 * simply not found.  There is nothing to leak, because there is no path that reads a survey the
 * caller could not have listed.
 */
public class SurveySubmissionCountTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_submission_counts";
	}

	@Override
	public String getTitle() {
		return "Survey submission counts";
	}

	@Override
	public String getDescription() {
		return "Counts the submissions received for each survey this user can access. "
				+ "Optionally narrowed to one project or one survey.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"survey_id", property("integer", "Only this survey. Omit for all."),
				"project_id", property("integer", "Only surveys in this project. Omit for all."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		int projectId = intArg(arguments, "project_id", 0);

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		ArrayList<Survey> surveys = sm.getSurveys(ctx.sd, ctx.user, false, false, projectId,
				false, false, false, false, false, null);

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();
		int total = 0;
		int counted = 0;

		for(Survey s : surveys) {
			if(surveyId > 0 && s.getId() != surveyId) {
				continue;
			}
			if(ctx.maxRows > 0 && counted >= ctx.maxRows) {
				text.append("\n(truncated at ").append(ctx.maxRows).append(" surveys)");
				break;
			}
			int count = countSubmissions(ctx, s.getIdent());
			total += count;
			counted++;

			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", s.getId());
			row.put("ident", s.getIdent());
			row.put("name", s.getDisplayName());
			row.put("project", s.getProjectName());
			row.put("submissions", count);
			rows.add(row);

			text.append("\n- ").append(s.getDisplayName())
					.append(" (id ").append(s.getId()).append("): ")
					.append(count).append(" submission(s)");
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append(surveyId > 0
					? "No such survey, or you do not have access to it."
					: "No surveys found.");
		} else {
			text.insert(0, "Submission counts for " + rows.size() + " survey(s):");
			text.append("\n\nTotal: ").append(total);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("surveys", rows);
		data.put("total", total);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/*
	 * Counted from upload_event rather than the results table, so a survey whose table has not been
	 * created yet reads as zero instead of failing
	 */
	private int countSubmissions(McpToolContext ctx, String surveyIdent) throws Exception {

		String sql = "select count(*) from upload_event where ident = ? and status != 'error'";
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(sql)) {
			pstmt.setString(1, surveyIdent);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}
}
