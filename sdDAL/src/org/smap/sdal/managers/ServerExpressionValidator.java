package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.constants.SmapServerMeta;

import com.google.gson.Gson;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.ServerCalculation;
import org.smap.sdal.model.SqlFrag;

/*
 * Checks a server expression, an expression that is evaluated as SQL against a survey bundle's
 * results, such as a notification filter, a case alert filter or a server calculation.
 *
 * Three stages:
 *   1. Parse it, as it will be parsed when it runs
 *   2. Find each ${name} among the bundle's questions, and check it can be used here (an alert, for
 *      example, only sees the main form)
 *   3. Have the database plan and evaluate it against a single row of empty values typed as the
 *      bundle's columns are.  This finds type mismatches, unknown functions and a filter that is not
 *      true or false.  It needs no data, so it works before anything has been submitted.
 *
 * The dry run uses the columns the expression names with the types they have in the results tables,
 * or for questions not yet in a table, the type they will be given.  It binds its parameters as the
 * real query does, so they cannot disagree about a type.
 *
 * One validator can check many expressions, it keeps what it has read about each bundle.
 */
public class ServerExpressionValidator {

	private static Logger log = Logger.getLogger(ServerExpressionValidator.class.getName());

	public static final int CONDITION = 1;		// Must be true or false, a filter
	public static final int VALUE = 2;			// Any value, a calculation

	// Types as the database spells them, anything else is treated as text in the dry run
	private static final Pattern SAFE_TYPE = Pattern.compile("^[A-Za-z0-9 _(),]+$");

	private ResourceBundle localisation;
	private HashMap<String, Bundle> bundles = new HashMap<> ();
	private int excludedSurvey = 0;		// A survey being replaced, its questions are about to go

	/*
	 * A saved expression that does not work
	 */
	public static class Problem {
		public String kind;			// One of the KIND values
		public String what;			// What it is, in the user's language
		public int id;				// The notification, alert, task group, template, survey role, reference filter or question
		public String name;
		public int sId;				// The survey it is for, 0 if it is for a bundle that has none left
		public String expression;	// Null for a server calculation, which has several parts
		public String error;
	}
	public static final String KIND_NOTIFICATION = "notification";
	public static final String KIND_ALERT = "alert";
	public static final String KIND_TASK = "task";
	public static final String KIND_TASK_ASSIGN = "task_assign";
	public static final String KIND_TEMPLATE = "template";
	public static final String KIND_ROW_FILTER = "row_filter";
	public static final String KIND_REF_FILTER = "ref_filter";
	public static final String KIND_SERVER_CALC = "server_calc";

	private static class Column {
		String type;
		boolean subForm;
		boolean stored;		// Has an answer in the results tables, a server calculation or a group does not
		Column(String type, boolean subForm, boolean stored) {
			this.type = type;
			this.subForm = subForm;
			this.stored = stored;
		}
	}

	private static class Bundle {
		HashMap<String, String> columnMap = new HashMap<> ();		// Question name to column
		HashMap<String, Column> columns = new HashMap<> ();		// Column to its details
		boolean hasTable;
	}

	// Columns every main results table has, used for a bundle with nothing submitted yet
	private static final String[][] META = {
			{"prikey", "integer"}, {"parkey", "integer"}, {"_bad", "boolean"}, {"_bad_reason", "text"},
			{"_user", "text"}, {"_version", "text"}, {"_survey_notes", "text"}, {"_location_trigger", "text"},
			{"_assigned", "text"}, {"_complete", "boolean"}, {"_modified", "boolean"},
			{SmapServerMeta.UPLOAD_TIME_NAME, "timestamp with time zone"}, {SmapServerMeta.SURVEY_ID_NAME, "integer"},
			{"instanceid", "text"}, {"instancename", "text"}, {"_thread", "text"},
			{"_thread_created", "timestamp with time zone"}, {"_case_closed", "timestamp with time zone"}, {"_case_reopened", "timestamp with time zone"},
			{"_case_survey", "text"}, {"_alert", "text"},
			{SmapServerMeta.SCHEDULED_START_NAME, "timestamp with time zone"}, {"_hrk", "text"},
			{"_start", "timestamp with time zone"}, {"_end", "timestamp with time zone"}, {"_device", "text"}
	};

	public ServerExpressionValidator(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Check an expression, returning why it will not work or null if it will
	 * mainOnly is set where the expression only sees the main form, as for a case alert
	 */
	public String check(Connection sd, Connection cResults, String groupSurveyIdent, boolean mainOnly,
			String expression, int kind) throws SQLException {

		if(expression == null || expression.trim().length() == 0) {
			return null;
		}
		Bundle b = getBundle(sd, cResults, groupSurveyIdent);
		SqlFrag frag = new SqlFrag(b.columnMap);
		try {
			frag.addSqlFragment(expression, false, localisation, 0);
		} catch (Exception e) {
			return message(e);
		}
		return evaluate(cResults, b, frag, mainOnly, kind);
	}

	public String check(Connection sd, Connection cResults, int sId, boolean mainOnly,
			String expression, int kind) throws SQLException {
		return check(sd, cResults, GeneralUtilityMethods.getGroupSurveyIdent(sd, sId), mainOnly, expression, kind);
	}

	/*
	 * Check a server calculation, its conditions and values together as they are evaluated
	 */
	public String checkServerCalculation(Connection sd, Connection cResults, String groupSurveyIdent,
			ServerCalculation sc) throws SQLException {

		if(sc == null) {
			return null;
		}
		Bundle b = getBundle(sd, cResults, groupSurveyIdent);
		SqlFrag frag = new SqlFrag(b.columnMap);
		try {
			sc.populateSql(frag, localisation);
		} catch (Exception e) {
			return message(e);
		}
		if(frag.sql.toString().trim().length() == 0) {
			return null;
		}
		return evaluate(cResults, b, frag, false, VALUE);
	}

	/*
	 * Check and refuse an expression that will not work, the message names what it is for
	 */
	public void validate(Connection sd, Connection cResults, String groupSurveyIdent, boolean mainOnly,
			String expression, int kind, String what) throws Exception {
		String error = check(sd, cResults, groupSurveyIdent, mainOnly, expression, kind);
		if(error != null) {
			throw new ApplicationException(refusal(what, expression, error));
		}
	}

	public void validate(Connection sd, Connection cResults, int sId, boolean mainOnly,
			String expression, int kind, String what) throws Exception {
		validate(sd, cResults, GeneralUtilityMethods.getGroupSurveyIdent(sd, sId), mainOnly, expression, kind, what);
	}

	public void validateServerCalculation(Connection sd, Connection cResults, String groupSurveyIdent,
			ServerCalculation sc, String what) throws Exception {
		String error = checkServerCalculation(sd, cResults, groupSurveyIdent, sc);
		if(error != null) {
			throw new ApplicationException(refusal(what, null, error));
		}
	}

	/*
	 * Check a filter the user has typed in, for a data view or an export, before it is run.  The
	 * error says what is wrong in place of the database's own message.
	 */
	public void validateFilter(Connection sd, Connection cResults, int sId, String filter) throws Exception {
		String error = check(sd, cResults, sId, false, filter, CONDITION);
		if(error != null) {
			throw new ApplicationException(localisation.getString("filter_error")
					.replace("%s1", filter).replace("%s2", error));
		}
	}

	public String refusal(String what, String expression, String error) {
		String msg = localisation.getString(expression == null ? "se_refused_calc" : "se_refused");
		msg = msg.replace("%s1", what == null ? "" : what);
		msg = msg.replace("%s2", expression == null ? "" : expression);
		msg = msg.replace("%s3", error);
		return msg;
	}

	/*
	 * Forget what has been read about a bundle, after its questions have changed
	 */
	public void forget(String groupSurveyIdent) {
		bundles.remove(groupSurveyIdent);
	}

	/*
	 * Leave a survey's questions out, when it is being replaced by a survey already in the bundle
	 */
	public void excludeSurvey(int sId) {
		excludedSurvey = sId;
		bundles.clear();
	}

	/*
	 * Check every saved expression that is evaluated against a bundle's results.  For a survey change,
	 * to warn about what it broke, and for the operational issues report.
	 * onlySurveyIdent, if set, limits it to the definition of one survey: its server calculations
	 * and role row filters.
	 */
	public ArrayList<Problem> checkBundle(Connection sd, Connection cResults, String groupSurveyIdent,
			String onlySurveyIdent) throws SQLException {

		ArrayList<Problem> problems = new ArrayList<> ();
		if(groupSurveyIdent == null) {
			return problems;
		}
		String members = "select s_id from survey where group_survey_ident = ? and not deleted";
		String memberIdents = "select ident from survey where group_survey_ident = ? and not deleted";
		int firstSurvey = 0;
		try (PreparedStatement pstmt = sd.prepareStatement(members + " and s_id != ? order by s_id asc limit 1")) {
			pstmt.setString(1, groupSurveyIdent);
			pstmt.setInt(2, excludedSurvey);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				firstSurvey = rs.getInt(1);
			}
		}

		if(onlySurveyIdent == null) {

			// Notifications, a server calculation notification looks at the main form only
			String sql = "select f.id, f.name, f.filter, f.trigger, f.s_id from forward f "
					+ "where f.enabled and f.filter is not null and trim(f.filter) != '' "
					+ "and ((f.bundle and f.bundle_ident = ?) or (not f.bundle and f.s_id in (" + members + "))) "
					+ "order by f.id";
			try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
				pstmt.setString(1, groupSurveyIdent);
				pstmt.setString(2, groupSurveyIdent);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					String filter = rs.getString("filter");
					String error = check(sd, cResults, groupSurveyIdent, "server_calc".equals(rs.getString("trigger")),
							filter, CONDITION);
					int sId = rs.getInt("s_id");
					add(problems, KIND_NOTIFICATION, "se_w_notification", rs.getInt("id"), rs.getString("name"),
							sId > 0 ? sId : firstSurvey, filter, error);
				}
			}

			// Case alerts, the main form only
			sql = "select id, name, filter from cms_alert "
					+ "where group_survey_ident = ? and filter is not null and trim(filter) != '' order by id";
			try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
				pstmt.setString(1, groupSurveyIdent);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					String filter = rs.getString("filter");
					add(problems, KIND_ALERT, "se_w_alert", rs.getInt("id"), rs.getString("name"), firstSurvey, filter,
							check(sd, cResults, groupSurveyIdent, true, filter, CONDITION));
				}
			}

			// Task rules, their filter and the expression that finds who to assign to
			sql = "select tg_id, name, rule, source_s_id from task_group "
					+ "where rule is not null and source_s_id in (" + members + ") order by tg_id";
			try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
				pstmt.setString(1, groupSurveyIdent);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					AssignFromSurvey as = null;
					try {
						as = new Gson().fromJson(rs.getString("rule"), AssignFromSurvey.class);
					} catch (Exception e) {
						continue;
					}
					if(as == null) {
						continue;
					}
					int tgId = rs.getInt("tg_id");
					String name = rs.getString("name");
					int sId = rs.getInt("source_s_id");
					if(as.filter != null && as.filter.advanced != null && as.filter.advanced.trim().length() > 0) {
						add(problems, KIND_TASK, "se_w_task", tgId, name, sId, as.filter.advanced,
								check(sd, cResults, groupSurveyIdent, false, as.filter.advanced, CONDITION));
					}
					if(as.assign_data != null && as.assign_data.trim().length() > 0 && !as.assign_data.equals("_data")) {
						add(problems, KIND_TASK_ASSIGN, "se_w_task_assign", tgId, name, sId, as.assign_data,
								check(sd, cResults, groupSurveyIdent, true, as.assign_data, VALUE));
					}
				}
			}

			// PDF template rules
			sql = "select t.t_id, t.name, t.rule, s.s_id from survey_template t, survey s "
					+ "where t.ident = s.ident and s.group_survey_ident = ? and not s.deleted "
					+ "and t.rule is not null and trim(t.rule) != '' order by t.t_id";
			try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
				pstmt.setString(1, groupSurveyIdent);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					String rule = rs.getString("rule");
					add(problems, KIND_TEMPLATE, "se_w_template", rs.getInt("t_id"), rs.getString("name"),
							rs.getInt("s_id"), rule, check(sd, cResults, groupSurveyIdent, false, rule, CONDITION));
				}
			}

			// Reference data filters that other bundles apply to this one's data, its main form only
			sql = "select rf.id, rf.filter, "
					+ "(select display_name from survey where group_survey_ident = rf.linker_s_ident "
					+ "  and not deleted order by s_id limit 1) as linker, "
					+ "(select s_id from survey where group_survey_ident = rf.linker_s_ident "
					+ "  and not deleted order by s_id limit 1) as linker_s_id "
					+ "from reference_filter rf "
					+ "where rf.enabled and rf.linked_s_ident = ? "
					+ "and rf.filter is not null and trim(rf.filter) != '' order by rf.id";
			try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
				pstmt.setString(1, groupSurveyIdent);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					String filter = rs.getString("filter");
					add(problems, KIND_REF_FILTER, "se_w_ref_filter", rs.getInt("id"), rs.getString("linker"),
							rs.getInt("linker_s_id"), filter, check(sd, cResults, groupSurveyIdent, true, filter, CONDITION));
				}
			}
		}

		// Role row filters
		String sql = "select sr.id, r.name, sr.row_filter, s.s_id "
				+ "from survey_role sr, role r, survey s "
				+ "where sr.r_id = r.id and sr.survey_ident = s.ident "
				+ "and s.group_survey_ident = ? and not s.deleted "
				+ (onlySurveyIdent != null ? "and s.ident = ? " : "")
				+ "and sr.enabled and sr.row_filter is not null and trim(sr.row_filter) != '' order by sr.id";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, groupSurveyIdent);
			if(onlySurveyIdent != null) {
				pstmt.setString(2, onlySurveyIdent);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String filter = rs.getString("row_filter");
				add(problems, KIND_ROW_FILTER, "se_w_row_filter", rs.getInt("id"), rs.getString("name"),
						rs.getInt("s_id"), filter, check(sd, cResults, groupSurveyIdent, false, filter, CONDITION));
			}
		}

		// Server calculations
		sql = "select q.q_id, q.qname, q.server_calculate, f.s_id "
				+ "from question q, form f, survey s "
				+ "where q.f_id = f.f_id and f.s_id = s.s_id "
				+ "and s.group_survey_ident = ? and not s.deleted and s.s_id != ? "
				+ (onlySurveyIdent != null ? "and s.ident = ? " : "")
				+ "and q.qtype = 'server_calculate' and not q.soft_deleted "
				+ "and q.server_calculate is not null order by q.q_id";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, groupSurveyIdent);
			pstmt.setInt(2, excludedSurvey);
			if(onlySurveyIdent != null) {
				pstmt.setString(3, onlySurveyIdent);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				ServerCalculation sc = null;
				try {
					sc = new Gson().fromJson(rs.getString("server_calculate"), ServerCalculation.class);
				} catch (Exception e) {
					continue;
				}
				add(problems, KIND_SERVER_CALC, "se_w_server_calc", rs.getInt("q_id"), rs.getString("qname"),
						rs.getInt("s_id"), null, checkServerCalculation(sd, cResults, groupSurveyIdent, sc));
			}
		}
		return problems;
	}

	private void add(ArrayList<Problem> problems, String kind, String whatKey, int id, String name, int sId,
			String expression, String error) {
		if(error == null) {
			return;
		}
		Problem p = new Problem();
		p.kind = kind;
		p.what = localisation.getString(whatKey);
		p.id = id;
		p.name = name;
		p.sId = sId;
		p.expression = expression;
		p.error = error;
		problems.add(p);
	}

	/*
	 * A problem as a sentence, for a warning
	 */
	public String describe(Problem p) {
		String what = p.what + (p.name == null || p.name.length() == 0 ? "" : " " + p.name);
		if(p.expression == null) {
			return what + ": " + p.error;
		}
		return localisation.getString("se_broken").replace("%s1", what).replace("%s2", p.expression)
				.replace("%s3", p.error);
	}

	private String evaluate(Connection cResults, Bundle b, SqlFrag frag, boolean mainOnly, int kind) throws SQLException {

		/*
		 * Each ${name} must be a question in the bundle, or one of the columns every results table has
		 */
		LinkedHashMap<String, String> used = new LinkedHashMap<> ();	// Column to its type
		boolean unknownMeta = false;
		for(int i = 0; i < frag.columns.size(); i++) {
			String column = frag.columns.get(i);
			String name = frag.humanNames.get(i);
			Column c = b.columns.get(column);
			boolean isQuestion = !frag.unresolved.contains(name);
			if(c == null && isQuestion && b.columnMap.containsKey(name)) {
				return localisation.getString("se_not_stored").replace("%s1", name);
			}
			if(c == null || (!isQuestion && !c.stored)) {
				if(!b.hasTable && name.startsWith("_")) {
					unknownMeta = true;		// Perhaps a preload, not known until something is submitted
					continue;
				}
				return localisation.getString("se_unknown").replace("%s1", name);
			}
			if(!c.stored) {
				return localisation.getString("se_not_stored").replace("%s1", name);
			}
			if(mainOnly && c.subForm) {
				return localisation.getString("se_sub_form").replace("%s1", name);
			}
			used.put(column, c.type);
		}
		if(unknownMeta) {
			return null;		// Nothing more can be said until there is a table
		}

		// Upload time and scheduled start can be named without ${}
		String sql = frag.sql.toString();
		for(String meta : new String[] {SmapServerMeta.UPLOAD_TIME_NAME, SmapServerMeta.SCHEDULED_START_NAME}) {
			if(sql.contains(meta) && !used.containsKey(meta)) {
				used.put(meta, "timestamp with time zone");
			}
		}

		/*
		 * One row of empty values, each typed as its column is
		 */
		StringBuilder row = new StringBuilder("select ");
		if(used.isEmpty()) {
			row.append("1 as _none");
		} else {
			boolean first = true;
			for(String column : used.keySet()) {
				String type = used.get(column);
				if(type == null || !SAFE_TYPE.matcher(type).matches()) {
					type = "text";
				}
				if(!first) {
					row.append(", ");
				}
				first = false;
				row.append("null::").append(type).append(" as \"").append(column.replace("\"", "\"\"")).append("\"");
			}
		}

		String dryRun = "select pg_typeof((" + sql + "))::text from (" + row + ") as t";

		Savepoint sp = null;
		boolean autoCommit = cResults.getAutoCommit();
		try (PreparedStatement pstmt = cResults.prepareStatement(dryRun)) {
			if(!autoCommit) {
				sp = cResults.setSavepoint();
			}
			pstmt.setQueryTimeout(10);
			GeneralUtilityMethods.setFragParams(pstmt, frag, 1, "UTC");
			log.fine("Check server expression: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next() && kind == CONDITION) {
				String type = rs.getString(1);
				if(!"boolean".equals(type)) {
					return localisation.getString("se_not_bool").replace("%s1", type == null ? "" : type);
				}
			}
			if(sp != null) {
				cResults.releaseSavepoint(sp);
			}
		} catch (SQLException e) {
			if(sp != null) {
				try {cResults.rollback(sp);} catch (Exception ex) {}
			}
			return localisation.getString("se_eval").replace("%s1", firstLine(e.getMessage()));
		} catch (Exception e) {
			if(sp != null) {
				try {cResults.rollback(sp);} catch (Exception ex) {}
			}
			return message(e);
		}
		return null;
	}

	/*
	 * Read a bundle's questions and the types of its results columns
	 */
	private Bundle getBundle(Connection sd, Connection cResults, String groupSurveyIdent) throws SQLException {

		String key = groupSurveyIdent == null ? "" : groupSurveyIdent;
		Bundle b = bundles.get(key);
		if(b != null) {
			return b;
		}
		b = new Bundle();
		bundles.put(key, b);
		if(groupSurveyIdent == null) {
			return b;
		}

		String sql = "select q.qname, q.column_name, q.qtype, q.source, f.table_name, f.parentform "
				+ "from question q, form f, survey s "
				+ "where q.f_id = f.f_id "
				+ "and f.s_id = s.s_id "
				+ "and s.group_survey_ident = ? "
				+ "and not s.deleted "
				+ "and not q.soft_deleted "
				+ "and not f.reference "
				+ "and q.column_name is not null "
				+ "and s.s_id != ? "
				+ "order by s.s_id asc, q.q_id asc";

		HashMap<String, Boolean> tables = new HashMap<> ();		// Table to whether it is a sub form
		HashMap<String, String> columnTable = new HashMap<> ();
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, groupSurveyIdent);
			pstmt.setInt(2, excludedSurvey);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String qname = rs.getString("qname");
				String column = rs.getString("column_name");
				String qType = rs.getString("qtype");
				String source = rs.getString("source");
				String table = rs.getString("table_name");
				boolean subForm = rs.getInt("parentform") != 0;

				b.columnMap.putIfAbsent(qname, column);
				b.columnMap.putIfAbsent(qname.toLowerCase(), column);
				if(table != null) {
					tables.putIfAbsent(table, subForm);
				}

				boolean geoRepeat = qType.equals("begin repeat")
						&& (qname.startsWith("geopolygon_") || qname.startsWith("geolinestring_"));
				boolean stored = (source != null && !qType.equals("server_calculate")
						&& !qType.equals("begin repeat") && !qType.equals("begin group")) || geoRepeat;
				if(!b.columns.containsKey(column) || (stored && !b.columns.get(column).stored)) {
					b.columns.put(column, new Column(questionType(qType, geoRepeat), subForm, stored));
					columnTable.put(column, table);
				}
			}
		}

		/*
		 * A deleted question keeps its column while it has answers, which is not a meta column
		 */
		HashSet<String> deletedColumns = new HashSet<> ();
		String sqlDeleted = "select q.column_name "
				+ "from question q, form f, survey s "
				+ "where q.f_id = f.f_id "
				+ "and f.s_id = s.s_id "
				+ "and s.group_survey_ident = ? "
				+ "and not s.deleted "
				+ "and q.soft_deleted "
				+ "and q.column_name is not null";
		try (PreparedStatement pstmt = sd.prepareStatement(sqlDeleted)) {
			pstmt.setString(1, groupSurveyIdent);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				deletedColumns.add(rs.getString(1));
			}
		}

		/*
		 * Where a table exists use the types its columns have, and pick up the meta and preload
		 * columns, which are not questions
		 */
		String sqlCols = "select a.attname, format_type(a.atttypid, a.atttypmod) "
				+ "from pg_attribute a, pg_class c, pg_namespace n "
				+ "where a.attrelid = c.oid "
				+ "and c.relnamespace = n.oid "
				+ "and n.nspname = 'public' "
				+ "and c.relname = ? "
				+ "and a.attnum > 0 "
				+ "and not a.attisdropped";
		try (PreparedStatement pstmt = cResults.prepareStatement(sqlCols)) {
			for(String table : tables.keySet()) {
				boolean subForm = tables.get(table);
				pstmt.setString(1, table);
				ResultSet rs = pstmt.executeQuery();
				boolean found = false;
				while(rs.next()) {
					found = true;
					String column = rs.getString(1);
					String type = rs.getString(2);
					Column c = b.columns.get(column);
					if(c != null && c.stored && table.equals(columnTable.get(column))) {
						c.type = type;
					} else if(c == null && !subForm && !deletedColumns.contains(column)) {
						b.columns.put(column, new Column(type, false, true));		// Meta or preload
					}
				}
				if(found && !subForm) {
					b.hasTable = true;
				}
			}
		}
		if(!b.hasTable) {
			for(String [] m : META) {
				b.columns.putIfAbsent(m[0], new Column(m[1], false, true));
			}
		}
		return b;
	}

	private String questionType(String qType, boolean geoRepeat) {
		if(geoRepeat || qType.equals("geopoint") || qType.equals("geoshape") || qType.equals("geotrace")
				|| qType.equals("geocompound")) {
			return "geometry";
		} else if(qType.equals("int")) {
			return "integer";
		}
		return GeneralUtilityMethods.getPostgresColType(qType);
	}

	private String message(Exception e) {
		String msg = e.getMessage();
		if(msg == null) {
			log.log(Level.SEVERE, "Check server expression", e);
			msg = e.getClass().getSimpleName();
		}
		return firstLine(msg);
	}

	// The database adds the position and a hint on further lines
	private String firstLine(String msg) {
		if(msg == null) {
			return "";
		}
		int nl = msg.indexOf('\n');
		msg = nl > 0 ? msg.substring(0, nl) : msg;
		if(msg.startsWith("ERROR: ")) {
			msg = msg.substring(7);
		}
		return msg;
	}
}
