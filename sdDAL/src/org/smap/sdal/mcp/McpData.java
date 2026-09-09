package org.smap.sdal.mcp;

import java.util.ArrayList;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.DataAggregateManager;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumn;

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

	/*
	 * A survey's full design, for the tools that read its structure rather than its data.
	 *
	 * Goes through surveyById first, so the same rule applies as everywhere else here: a survey the
	 * caller could not have listed does not exist.  The flags match the ones behind
	 * smap://survey/{ident}/definition, because two ways of reading the same design that disagree
	 * about soft deleted questions or external options would be worse than one.
	 *
	 * superUser stays false. getById will happily read a survey as an administrator and the design
	 * tools must not, for the same reason the data tools must not.
	 */
	/*
	 * A survey's settings and shape, without its design.
	 *
	 * The header getById returns when it is not asked for the full survey, plus the languages and
	 * the forms.  Reading a whole design to answer how many forms does this have, or what languages,
	 * is what the coarser call does, and those are the questions most often asked of a survey.
	 */
	public static Survey outline(McpToolContext ctx, int surveyId) throws Exception {

		Survey listed = surveyById(ctx, surveyId);
		if(listed == null) {
			return null;
		}
		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		Survey s = sm.getById(ctx.sd, ctx.cResults, ctx.user, false, surveyId,
				false,			// not the full definition: no questions, options or labels
				null, null,
				false, false, false, false, false,
				"real",
				false,			// getChangeHistory
				false,			// getRoles
				false,			// superUser - the caller's own rights, not an administrator's
				"geojson",
				false, false, false);
		if(s == null) {
			return null;
		}
		s.surveyData.languages = GeneralUtilityMethods.getLanguages(ctx.sd, surveyId);
		s.surveyData.forms = sm.getForms(ctx.sd, surveyId);
		return s;
	}

	public static Survey definition(McpToolContext ctx, int surveyId) throws Exception {

		Survey listed = surveyById(ctx, surveyId);
		if(listed == null) {
			return null;
		}
		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		return sm.getById(ctx.sd, ctx.cResults, ctx.user, false, surveyId,
				true,			// full definition
				null,			// basePath
				null,			// instanceId
				false,			// getResults
				false,			// generateDummyValues
				false,			// getPropertyTypeQuestions
				false,			// getSoftDeleted
				true,			// getHrk
				"real",			// external options if they exist
				false,			// getChangeHistory
				false,			// getRoles
				false,			// superUser - the caller's own rights, not an administrator's
				"geojson",
				false,			// referenceSurveys
				false,			// onlyGetLaunched
				false);			// mergeDefaultSetValue
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
	 * The columns of one form, as this caller may see them.
	 *
	 * The user is passed rather than left null so that role column filtering applies: a role can
	 * hide individual questions, and a tool that looked those up as nobody in particular would
	 * offer columns the caller is not allowed to read.
	 *
	 * The form id matters even for the main form.  Columns are found per form, so leaving it at
	 * zero finds no questions and yields records made entirely of metadata.
	 */
	public static ArrayList<TableColumn> columns(McpToolContext ctx, Survey survey, int parentForm,
			int fId, String tableName, boolean isChildForm, boolean includeBad, boolean includeMeta)
			throws Exception {

		return GeneralUtilityMethods.getColumnsInForm(
				ctx.sd,
				ctx.cResults,
				ctx.localisation,
				"none",					// language
				survey.getId(),
				survey.getIdent(),
				ctx.user,				// the caller, so role column filtering applies
				null,					// roles are looked up from the user
				parentForm,
				fId,
				tableName,
				true,					// include read only
				isChildForm,			// include the parent key on a repeating group
				includeBad,
				includeMeta,			// instance id
				includeMeta,			// prikey, which is what paging follows
				includeMeta,			// HRK
				includeMeta,			// other metadata
				includeMeta,			// preloads
				true,					// instance name
				includeMeta,			// survey duration
				includeMeta,			// case management
				ctx.superUser,
				false,					// HXL
				false,					// audit
				ctx.timezone,
				false,					// mgmt
				false,					// accuracy and altitude
				true);					// server calculates
	}

	/*
	 * The scope of a count or an aggregate: which table, which rows, and who is asking.
	 *
	 * Built here rather than in each tool so that data_count and data_aggregate cannot come to mean
	 * different things by the same arguments, and so the access facts - the caller's own columns,
	 * their super user flag, their view own data setting - are filled in from the context rather
	 * than from anything the caller sent.
	 *
	 * Returns null when the survey has no results table, which is not an error: a survey that has
	 * never been submitted to has a legitimate count of zero.
	 */
	public static DataAggregateManager.Query aggregateQuery(McpToolContext ctx, Survey survey,
			java.util.Map<String, Object> arguments) throws Exception {

		Form topForm = GeneralUtilityMethods.getTopLevelForm(ctx.sd, survey.getId());
		if(topForm == null || topForm.tableName == null) {
			return null;
		}

		String includeDeleted = includeDeleted(arg(arguments, "include_deleted"));
		boolean includeBad = includeDeleted.equals("yes") || includeDeleted.equals("only");

		DataAggregateManager.Query q = new DataAggregateManager.Query();
		q.sId = survey.getId();
		q.sIdent = survey.getIdent();
		q.tableName = topForm.tableName;
		q.user = ctx.user;
		q.superUser = ctx.superUser;
		q.viewOwnDataOnly = GeneralUtilityMethods.isOnlyViewOwnData(ctx.sd, ctx.user);
		q.includeBad = includeDeleted;
		q.advancedFilter = arg(arguments, "filter");
		q.columns = columns(ctx, survey, 0, topForm.id, topForm.tableName, false, includeBad, true);

		/*
		 * The date restriction is built here because getDateRange needs to know the column is really
		 * there, and that is a question about the results database.  A date question that does not
		 * exist leaves the range empty rather than failing, which matches how the read path behaves.
		 */
		String dateName = arg(arguments, "date_question");
		if(dateName != null && !dateName.trim().isEmpty()
				&& GeneralUtilityMethods.hasColumn(ctx.cResults, topForm.tableName, dateName.trim())) {
			q.dateName = dateName.trim();
			q.startDate = date(arg(arguments, "start_date"));
			q.endDate = date(arg(arguments, "end_date"));
			q.dateRange = GeneralUtilityMethods.getDateRange(q.startDate, q.endDate, q.dateName);
		}
		return q;
	}

	/*
	 * The deleted-records setting, checked rather than trusted.
	 *
	 * Every consumer of this value tests it for "none" and for "only" and does nothing when it is
	 * neither, so an unrecognised word does not fail, it quietly drops the restriction and returns
	 * the deleted rows as well.  A caller who mistypes gets more records than they asked for and no
	 * indication of it, which is the wrong way round for a default that exists to exclude things.
	 */
	public static String includeDeleted(String value) throws Exception {
		if(value == null || value.trim().isEmpty()) {
			return "none";
		}
		String v = value.trim().toLowerCase();
		if(v.equals("none") || v.equals("yes") || v.equals("only")) {
			return v;
		}
		throw new ApplicationException("include_deleted must be none, yes or only, not \""
				+ value + "\".");
	}

	private static String arg(java.util.Map<String, Object> args, String name) {
		Object v = args.get(name);
		return v == null ? null : v.toString();
	}

	private static java.sql.Date date(String value) {
		if(value == null || value.trim().isEmpty()) {
			return null;
		}
		try {
			return java.sql.Date.valueOf(value.trim());
		} catch (IllegalArgumentException e) {
			return null;
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
