package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.sql.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.TableColumn;

/*
 * Counting and grouping survey data, with the same access rules as reading it.
 *
 * Smap has no server side aggregation over results: the console fetches rows and adds them up in the
 * browser, so there was nothing to call.  An agent cannot do that - "how many submissions per
 * district" would mean pulling every row into a model's context to count them - so the arithmetic
 * has to happen in the database.
 *
 * This does not reuse TableDataManager.getPreparedStatement, which builds the equivalent query for
 * reading.  That method assembles its SELECT list and its WHERE clause together in one pass, so
 * replacing the SELECT would have meant restructuring a method the console and the REST API both
 * depend on, to add a feature that is off by default.  Instead the filters are built here from the
 * same primitives that method uses - RoleManager row filters, SqlFrag for the caller's own filter,
 * getDateRange - so the two agree because they are made of the same parts rather than because one
 * calls the other.  The assembly is duplicated; the security logic is not.
 *
 * Only the top level form.  Aggregating across a repeating group needs the join tree
 * QueryManager builds, and that is worth doing when something asks for it rather than in advance.
 */
public class DataAggregateManager {

	private static Logger log = Logger.getLogger(DataAggregateManager.class.getName());

	private ResourceBundle localisation;
	private String tz;

	/* The functions a caller may ask for, mapped to SQL.  Nothing else is accepted */
	private static final Map<String, String> FUNCTIONS = new LinkedHashMap<>();
	static {
		FUNCTIONS.put("count", "count(%s)");
		FUNCTIONS.put("sum", "sum(cast(%s as double precision))");
		FUNCTIONS.put("avg", "avg(cast(%s as double precision))");
		FUNCTIONS.put("min", "min(cast(%s as double precision))");
		FUNCTIONS.put("max", "max(cast(%s as double precision))");
	}

	public DataAggregateManager(ResourceBundle l, String tz) {
		this.localisation = l;
		this.tz = tz == null ? "UTC" : tz;
	}

	public static boolean isFunction(String name) {
		return name != null && FUNCTIONS.containsKey(name.toLowerCase());
	}

	public static List<String> functions() {
		return new ArrayList<>(FUNCTIONS.keySet());
	}

	/*
	 * How many records match, which is a different question from how many were returned.  A count
	 * is the one honest answer to "how much is there" that does not cost a row per record.
	 */
	public long count(Connection sd, Connection cResults, Query q) throws Exception {

		if(!GeneralUtilityMethods.tableExists(cResults, q.tableName)) {
			return 0;
		}

		Where where = where(sd, q);
		String sql = "select count(*) from " + q.tableName + where.sql;

		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql);
			where.setParams(pstmt, 1);
			log.info("Count: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getLong(1) : 0;
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
	}

	/*
	 * One row per distinct value of the grouping question.
	 *
	 * Both column names are resolved against the caller's own column list before they reach the SQL.
	 * They are identifiers and cannot be bound as parameters, so the only safe way to accept them is
	 * to match them to a column the caller has already been shown and use the stored name, never the
	 * string that arrived.
	 */
	public List<Map<String, Object>> aggregate(Connection sd, Connection cResults, Query q,
			String groupBy, String function, String valueColumn, int limit) throws Exception {

		List<Map<String, Object>> rows = new ArrayList<>();
		if(!GeneralUtilityMethods.tableExists(cResults, q.tableName)) {
			return rows;
		}

		String groupCol = selectable(cResults, q, groupBy);

		String fn = function == null ? "count" : function.toLowerCase();
		if(!FUNCTIONS.containsKey(fn)) {
			throw new ApplicationException("Unknown function " + function
					+ ". Use one of " + functions());
		}

		/*
		 * count is the only function that means something without a column to apply it to, and for
		 * that one counting rows is what is wanted rather than counting non null values of some
		 * arbitrary column.
		 */
		String valueCol = null;
		if(!"count".equals(fn) || valueColumn != null) {
			if(valueColumn == null) {
				throw new ApplicationException("A value_question is needed for " + fn);
			}
			valueCol = selectable(cResults, q, valueColumn);
		}

		String aggregate = String.format(FUNCTIONS.get(fn), valueCol == null ? "*" : valueCol);

		Where where = where(sd, q);
		/*
		 * Coalesced to a string so that "not answered" is one group carrying an empty value, rather
		 * than a null that arrives as a row with no group at all - Gson drops a null field, so the
		 * reader would get a count attached to nothing.  It also merges null and empty, which mean
		 * the same thing in survey results and would otherwise be two rows that look identical.
		 */
		String groupExpr = "coalesce(cast(" + groupCol + " as text), '')";

		StringBuilder sql = new StringBuilder("select ")
				.append(groupExpr).append(" as group_value, ")
				.append(aggregate).append(" as value, ")
				.append("count(*) as records")
				.append(" from ").append(q.tableName)
				.append(where.sql)
				.append(" group by ").append(groupExpr)
				.append(" order by ").append("2 desc nulls last")
				.append(" limit ?");

		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql.toString());
			int idx = where.setParams(pstmt, 1);
			pstmt.setInt(idx, limit);
			log.info("Aggregate: " + pstmt.toString());

			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("group", rs.getString("group_value"));
				row.put("value", rs.getObject("value"));
				row.put("records", rs.getLong("records"));
				rows.add(row);
			}
			return rows;
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
	}

	/*
	 * A column the caller has been shown, found by the name they used.  Matching on the stored name
	 * is what makes the value safe to put in SQL; matching on the display name as well is what makes
	 * the tool usable, since that is the name a model reads out of the data.
	 */
	private TableColumn resolve(ArrayList<TableColumn> columns, String name) {
		if(name == null) {
			return null;
		}
		String wanted = name.trim();
		for(TableColumn c : columns) {
			if(wanted.equalsIgnoreCase(c.column_name)) {
				return c;
			}
		}
		for(TableColumn c : columns) {
			if(wanted.equalsIgnoreCase(c.question_name) || wanted.equalsIgnoreCase(c.displayName)) {
				return c;
			}
		}
		return null;
	}

	/*
	 * A name the caller gave, turned into something that can be written into SQL.
	 *
	 * Two conditions, and the second is not obvious.  It has to be a column the caller has been
	 * shown, which is what makes it safe.  It also has to be a real column of the results table,
	 * because not every entry in the column list is one: survey duration, for instance, is computed
	 * in the SELECT list by TableColumn.getSqlSelect and has no column behind it, so grouping by it
	 * would put a name into the query that Postgres has never heard of.  Reading such a column works
	 * and grouping by it cannot, so the check belongs here rather than in the shared column lookup.
	 */
	private String selectable(Connection cResults, Query q, String name) throws Exception {

		TableColumn tc = resolve(q.columns, name);
		if(tc == null) {
			throw new ApplicationException(invalidColumn(name));
		}
		/*
		 * Geometry is stored as PostGIS binary, so grouping by it returns a page of hex that means
		 * nothing to read, and every point is distinct anyway so the grouping would not summarise
		 * anything.  Refused with a reason rather than answered uselessly.
		 */
		if(tc.type != null && GeneralUtilityMethods.isGeometry(tc.type)) {
			throw new ApplicationException("\"" + name + "\" is a map location. Locations cannot be "
					+ "grouped or summed, because each one is different. Use data_query to read "
					+ "them.");
		}
		String col = tc.column_name;
		if(!GeneralUtilityMethods.hasColumn(cResults, q.tableName, col)) {
			throw new ApplicationException("\"" + name + "\" is worked out when data is read "
					+ "rather than stored, so it cannot be grouped or summed. Pick a question that "
					+ "is answered in the form.");
		}
		return col;
	}

	/*
	 * Deliberately not the inv_qn_misc string, which is worded for the export screen and tells the
	 * reader their question is not in the forms they selected for export.  Here there is no export
	 * and nothing was selected, so it would send someone looking for a setting that is not involved.
	 */
	private String invalidColumn(String name) {
		return "There is no question called \"" + (name == null ? "" : name)
				+ "\" in this survey. Read the survey definition resource to see the names.";
	}

	/*
	 * Everything that restricts which rows are in scope, built once so count and aggregate cannot
	 * drift apart, and so the clauses and their parameters are written in the same order in one
	 * place rather than two.
	 */
	private Where where(Connection sd, Query q) throws Exception {

		Where w = new Where();
		StringBuilder sql = new StringBuilder(" where true");

		// Deleted records are marked rather than removed, so they are excluded unless asked for
		if("none".equals(q.includeBad)) {
			sql.append(" and ").append(q.tableName).append("._bad = 'false'");
		} else if("only".equals(q.includeBad)) {
			sql.append(" and ").append(q.tableName).append("._bad = 'true'");
		}

		// The role row filters, which are the reason this cannot be a plain count
		if(!q.superUser) {
			RoleManager rm = new RoleManager(localisation);
			w.roleFilters = rm.getSurveyRowFilter(sd, q.sIdent, q.user);
			String rFilter = rm.convertSqlFragsToSql(w.roleFilters);
			if(rFilter.length() > 0) {
				sql.append(" and ").append(rFilter);
				w.hasRoleFilter = true;
			}
		}

		/*
		 * The date restriction arrives already built, because getDateRange needs to know the column
		 * exists and that is a question about the results database, which the caller has open.
		 */
		if(q.dateRange != null && q.dateRange.trim().length() > 0) {
			sql.append(" and ").append(q.dateRange);
			w.startDate = q.startDate;
			w.endDate = q.endDate;
		}

		if(q.advancedFilter != null && q.advancedFilter.trim().length() > 0) {
			SqlFrag frag = new SqlFrag();
			frag.addSqlFragment(q.advancedFilter, false, localisation, 0);
			/*
			 * Validated against the caller's own columns, the same test the read path applies, so a
			 * filter that works for data_query works here and vice versa.
			 */
			for(String filterCol : frag.columns) {
				if(resolve(q.columns, filterCol) == null) {
					throw new ApplicationException(invalidColumn(filterCol));
				}
			}

			sql.append(" and (").append(frag.sql).append(")");
			w.filterFrag = frag;
		}

		if(q.viewOwnDataOnly) {
			sql.append(" and _user = ?");
			w.viewOwnUser = q.user;
		}

		w.sql = sql.toString();
		w.tz = tz;
		return w;
	}

	/* The restriction, and the parameters that go with it, kept together so they cannot be reordered */
	private static class Where {
		String sql;
		String tz;
		ArrayList<SqlFrag> roleFilters;
		boolean hasRoleFilter;
		SqlFrag filterFrag;
		Date startDate;
		Date endDate;
		String viewOwnUser;

		int setParams(PreparedStatement pstmt, int idx) throws Exception {
			if(hasRoleFilter) {
				idx = GeneralUtilityMethods.setArrayFragParams(pstmt, roleFilters, idx, tz);
			}
			if(startDate != null) {
				pstmt.setTimestamp(idx++, GeneralUtilityMethods.startOfDay(startDate, tz));
			}
			if(endDate != null) {
				pstmt.setTimestamp(idx++, GeneralUtilityMethods.endOfDay(endDate, tz));
			}
			if(filterFrag != null) {
				idx = GeneralUtilityMethods.setFragParams(pstmt, filterFrag, idx, tz);
			}
			if(viewOwnUser != null) {
				pstmt.setString(idx++, viewOwnUser);
			}
			return idx;
		}
	}

	/* What to count over: the survey, the table, and everything that narrows it */
	public static class Query {
		public int sId;
		public String sIdent;
		public String tableName;
		public ArrayList<TableColumn> columns;
		public String user;
		public boolean superUser;
		public boolean viewOwnDataOnly;
		public String includeBad = "none";
		public String advancedFilter;
		public String dateName;
		public String dateRange;
		public Date startDate;
		public Date endDate;
	}
}
