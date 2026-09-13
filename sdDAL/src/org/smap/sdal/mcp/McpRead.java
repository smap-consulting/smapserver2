package org.smap.sdal.mcp;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jettison.json.JSONObject;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.TableDataManager;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumn;

/*
 * Reading records, once, for every tool that needs rows.
 *
 * This is the path that applies the role row filters, so anything reading survey data goes through
 * it.  It exists as one class rather than as a method on each tool because the alternative was
 * copying sixty lines of manager arguments per tool, and the argument that matters - passing the
 * caller as the user whose filters apply - is the one that would eventually be copied wrong.
 */
public class McpRead {

	/* What to read */
	public static class Request {
		public Survey survey;
		public String formName;			// a repeating group, or null for the main form
		public String filter;
		public String sort;
		public String direction = "asc";
		public int cursor;
		public int limit = 100;
		public boolean includeMeta = true;
		public String includeDeleted = "none";
		public List<String> select;		// question names to return, or null for all
		public String dateQuestion;
		public java.sql.Date startDate;
		public java.sql.Date endDate;
	}

	/* What came back, and whether there is more of it */
	public static class Result {
		public List<JSONObject> rows = new ArrayList<>();
		public ArrayList<TableColumn> columns;
		public Integer lastKey;
		public boolean more;
		public boolean noData;
	}

	public static Result read(McpToolContext ctx, Request r) throws Exception {

		Result result = new Result();
		Survey survey = r.survey;
		int surveyId = survey.getId();
		boolean includeBad = "yes".equals(r.includeDeleted) || "only".equals(r.includeDeleted);

		/*
		 * Which form to read.  The main form unless a repeating group was named, in which case the
		 * manager joins back up to the top level form so the row filters still reach it.
		 *
		 * The form id matters even for the main form: the columns are looked up per form, so
		 * leaving it at zero finds no questions and returns records made entirely of metadata.
		 */
		Form topForm = GeneralUtilityMethods.getTopLevelForm(ctx.sd, surveyId);
		int fId = topForm.id;
		String tableName = topForm.tableName;
		int parentForm = 0;
		boolean isChildForm = false;
		if(r.formName != null && !r.formName.trim().isEmpty()) {
			int childId = GeneralUtilityMethods.getFormId(ctx.sd, surveyId, r.formName.trim());
			Form child = childId > 0 ? McpData.form(ctx, surveyId, childId) : null;
			if(child == null) {
				throw new ApplicationException("No repeating group called " + r.formName
						+ " in this survey. Read smap://survey/" + survey.getIdent()
						+ "/definition to see which there are.");
			}
			fId = child.id;
			tableName = child.tableName;
			parentForm = child.parentform;
			isChildForm = true;
		}

		ArrayList<TableColumn> columns = McpData.columns(ctx, survey, parentForm, fId,
				tableName, isChildForm, includeBad, r.includeMeta);
		columns = project(ctx, columns, r);
		result.columns = columns;

		/*
		 * One record more than asked for, so the presence of a next page is known rather than
		 * guessed.  Asking for exactly the limit cannot tell a full page from the last one.
		 */
		int fetch = r.limit + 1;

		TableDataManager tdm = new TableDataManager(ctx.localisation, ctx.timezone);
		PreparedStatement pstmt = null;
		int read = 0;

		boolean autoCommit = ctx.cResults.getAutoCommit();
		try {
			pstmt = tdm.getPreparedStatement(
					ctx.sd,
					ctx.cResults,
					columns,
					GeneralUtilityMethods.getUrlPrefix(ctx.request),
					GeneralUtilityMethods.getAttachmentPrefix(ctx.request, false),
					surveyId,
					survey.getIdent(),
					fId,
					tableName,
					0,						// parkey
					null,					// hrk
					ctx.user,				// the user whose row filters apply
					null,					// roles: taken from the user
					r.sort,
					r.direction,
					false,					// mgmt
					false,					// group
					false,					// isDt
					r.cursor,				// start: the primary key to begin at, inclusive
					isChildForm,			// getParkey
					0,						// start_parkey
					ctx.superUser,
					false,					// specificPrikey: records are never addressed by key
					r.includeDeleted,
					"yes",					// include_completed
					null,					// case management settings, not needed to read data
					null,					// custom filter
					null,					// key filters
					ctx.timezone,
					null,					// instanceId: data_get_record does single records
					r.filter,
					r.dateQuestion,
					r.startDate,
					r.endDate);

			if(pstmt == null) {
				result.noData = true;
				return result;
			}

			/*
			 * Postgres only honours a fetch size inside a transaction; outside one the driver
			 * buffers the entire result set before the first row is read.  The query carries no
			 * LIMIT of its own - it is bounded by this loop stopping early - so without this a
			 * question asked against a large survey would pull the whole table into memory to
			 * return a hundred rows.  DataManager does the same thing for the same reason.
			 */
			ctx.cResults.setAutoCommit(false);
			pstmt.setFetchSize(100);
			ResultSet rs = pstmt.executeQuery();
			boolean viewOwnDataOnly = GeneralUtilityMethods.isOnlyViewOwnData(ctx.sd, ctx.user);

			JSONObject row = new JSONObject();
			while(row != null && read < fetch) {
				row = tdm.getNextRecord(ctx.sd, rs, columns,
						GeneralUtilityMethods.getUrlPrefix(ctx.request),
						false,				// group
						false,				// isDt
						fetch,
						true,				// merge select multiples into one value
						false,				// geoJson
						null,				// geomQuestion
						false,				// links
						survey.getIdent(),
						viewOwnDataOnly,
						false);				// view links
				if(row != null) {
					read++;
					if(result.rows.size() < r.limit) {
						result.rows.add(row);
						Integer key = key(row);
						if(key != null) {
							result.lastKey = key;
						}
					} else {
						/*
						 * The extra record is counted but never kept.  It exists only to answer
						 * whether there is another page.
						 */
						result.more = true;
					}
				}
			}
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
			/*
			 * The connection is the dispatcher's and is reused by whatever runs next in this
			 * request, so it is handed back in the state it was borrowed in.
			 */
			try {ctx.cResults.setAutoCommit(autoCommit);} catch (Exception e) {}
		}
		return result;
	}

	/*
	 * Narrow the columns to the ones asked for.
	 *
	 * Done here rather than by dropping fields from the finished rows, so the questions nobody asked
	 * for are never read: a survey with fifty questions and a photo on each costs fifty columns and
	 * fifty built URLs per record otherwise, and for an agent the bill is paid twice, once by the
	 * database and again by the context the answer has to fit in.
	 *
	 * Three things are kept whether or not they were named, because removing them would break the
	 * request rather than narrow it.  The primary key is what paging follows, and the instance id is
	 * how a record is named to every other tool, so a caller who selected neither would get a page
	 * they cannot continue and rows they cannot follow up.  And a column used by the filter or the
	 * sort has to survive: TableDataManager validates the filter against this same list and refuses
	 * a name it cannot find, so selecting q1 while filtering on q3 would fail rather than answer.
	 */
	private static ArrayList<TableColumn> project(McpToolContext ctx, ArrayList<TableColumn> columns,
			Request r) throws Exception {

		if(r.select == null || r.select.isEmpty()) {
			return columns;
		}

		java.util.Set<String> keep = new java.util.HashSet<>();
		keep.add("prikey");
		keep.add("instanceid");

		for(String name : r.select) {
			TableColumn c = match(columns, name);
			if(c == null) {
				throw new ApplicationException("There is no question called \"" + name
						+ "\" in this survey. Read the survey definition resource to see the names.");
			}
			keep.add(c.column_name);
		}

		if(r.sort != null && !r.sort.trim().isEmpty()) {
			TableColumn c = match(columns, r.sort);
			if(c != null) {
				keep.add(c.column_name);
			}
		}

		/*
		 * The filter is parsed here only to learn which columns it names.  TableDataManager parses
		 * it again for real; doing it twice is cheaper than returning an error the caller cannot
		 * make sense of.
		 */
		if(r.filter != null && !r.filter.trim().isEmpty()) {
			SqlFrag frag = new SqlFrag();
			frag.addSqlFragment(r.filter, false, ctx.localisation, 0);
			keep.addAll(frag.columns);
		}

		ArrayList<TableColumn> kept = new ArrayList<>();
		for(TableColumn c : columns) {
			if(keep.contains(c.column_name)) {
				kept.add(c);
			}
		}
		return kept.isEmpty() ? columns : kept;
	}

	/*
	 * A column by any of the names a caller might know it by: the stored name, the question name, or
	 * the name it is displayed under.
	 */
	private static TableColumn match(ArrayList<TableColumn> columns, String name) {
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
			if(wanted.equalsIgnoreCase(c.question_name) || wanted.equalsIgnoreCase(c.displayName)
					|| wanted.equalsIgnoreCase(jsonKey(c))) {
				return c;
			}
		}
		return null;
	}

	/*
	 * The key a column's value actually appears under in a row.
	 *
	 * Not the column name and not always the display name: for everything except the console's own
	 * format, getNextRecord passes the name through translateToKobo first, which renames instanceid
	 * to uuid, _start to start, _end to end and _device to deviceid.  A tool that looks a value up
	 * by the name it saw in the column list therefore finds nothing for exactly those four, silently
	 * - which is how the instance id went missing from every attachment this returned.
	 */
	public static String jsonKey(TableColumn c) {
		String name = c.displayName != null ? c.displayName : c.column_name;
		return GeneralUtilityMethods.translateToKobo(name);
	}

	/* The instance id of a row, under whatever name it arrived */
	public static String instanceId(JSONObject row) {
		String key = GeneralUtilityMethods.translateToKobo("instanceid");
		if(!row.has(key)) {
			return null;
		}
		try {
			return row.getString(key);
		} catch (Exception e) {
			return null;
		}
	}

	/*
	 * The primary key of a row, if it is there.
	 *
	 * Read as a string and parsed rather than through getInt, because the record builder writes
	 * every column as text, so the key arrives as "1" rather than 1.
	 */
	public static Integer key(JSONObject row) {
		if(!row.has("prikey")) {
			return null;
		}
		try {
			return Integer.valueOf(row.get("prikey").toString().trim());
		} catch (Exception e) {
			return null;
		}
	}

	/*
	 * JSONObject serialises through Gson as its internal map field rather than as the object it
	 * represents, so rows are converted to plain maps before they go anywhere near a result.
	 */
	public static Object toPlain(JSONObject row) {
		return new com.google.gson.Gson().fromJson(row.toString(), Object.class);
	}

	public static java.sql.Date parseDate(String value) {
		if(value == null || value.trim().isEmpty()) {
			return null;
		}
		try {
			return java.sql.Date.valueOf(value.trim());
		} catch (IllegalArgumentException e) {
			return null;
		}
	}
}
