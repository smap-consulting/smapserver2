package org.smap.sdal.mcp;

import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Survey;

/*
 * The access rules every data path in this package goes through, in one place.
 *
 * There are two of them and they are not the same question.  Whether the caller may see the survey
 * at all is answered by looking for it in their own list.  Whether they may see a particular record
 * within it is answered by the role row filters, which can restrict a user to their own submissions
 * inside a survey they otherwise have full access to.
 *
 * Keeping both here is the point.  An earlier version of the data tool asked only the first
 * question, because the manager it called happened to fetch the survey as a super user and applied
 * no row filter, so a user whose role limited them to their own rows saw every row in the survey as
 * soon as they asked through MCP.  Any new tool that reads records goes through these methods so
 * the second question cannot be forgotten again.
 */
public class McpData {

	/*
	 * The survey, if it is one the caller could have listed.  A survey they could not list does not
	 * exist as far as this package is concerned, so there is no separate survey permission check
	 * anywhere that could drift out of step with the listing.
	 */
	public static Survey surveyById(McpToolContext ctx, int surveyId) throws Exception {
		for(Survey s : userSurveys(ctx)) {
			if(s.getId() == surveyId) {
				return s;
			}
		}
		return null;
	}

	public static Survey surveyByIdent(McpToolContext ctx, String ident) throws Exception {
		for(Survey s : userSurveys(ctx)) {
			if(ident.equals(s.getIdent())) {
				return s;
			}
		}
		return null;
	}

	public static ArrayList<Survey> userSurveys(McpToolContext ctx) throws Exception {
		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		return sm.getSurveys(ctx.sd, ctx.user, false, false, 0, false, false, false, false, false, null);
	}

	/*
	 * One form of a survey: its results table and its parent.
	 *
	 * Both are needed to read a repeating group, because the columns are looked up per form and the
	 * query has to join back up to the top level form for the row filters to reach it.  DataManager
	 * runs this query inline wherever it needs it rather than offering it, so it is repeated here.
	 */
	public static Form form(McpToolContext ctx, int sId, int fId) throws Exception {
		String sql = "select f_id, parentform, table_name from form where s_id = ? and f_id = ?";
		try (java.sql.PreparedStatement pstmt = ctx.sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			pstmt.setInt(2, fId);
			java.sql.ResultSet rs = pstmt.executeQuery();
			if(!rs.next()) {
				return null;
			}
			Form f = new Form();
			f.id = rs.getInt(1);
			f.parentform = rs.getInt(2);
			f.tableName = rs.getString(3);
			return f;
		}
	}

	/*
	 * Whether one record is inside the caller's row filters.
	 *
	 * Records are named by instance id and never by the sequential primary key: a prikey can be
	 * guessed by counting, so accepting one would let a caller walk a table they were never shown.
	 */
	public static boolean canSeeRecord(McpToolContext ctx, Survey survey, String instanceId)
			throws Exception {

		if(instanceId == null) {
			return false;
		}
		String tableName = GeneralUtilityMethods.getMainResultsTable(ctx.sd, ctx.cResults, survey.getId());
		if(tableName == null) {
			return false;		// No results table yet, so there is no record to see
		}
		RoleManager rm = new RoleManager(ctx.localisation);
		return rm.canAccessRecord(ctx.sd, ctx.cResults, survey.getIdent(), tableName, instanceId,
				ctx.user, ctx.timezone);
	}
}
