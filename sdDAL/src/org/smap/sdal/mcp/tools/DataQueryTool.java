package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.TableDataManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumn;

/*
 * Submitted records for one survey, filtered, sorted and paged.
 *
 * Built on TableDataManager rather than on DataManager.getDataRecords or the hierarchy view, for
 * two separate reasons.
 *
 * Access: TableDataManager is where the role row filters are applied.  The hierarchy view fetches
 * the survey as a super user and applies no row filter at all, so a user whose role restricts them
 * to their own submissions would see every row in the survey through it.
 *
 * Side effects: getDataRecords is written to serve one REST request and end it.  It closes the
 * connection it is handed, and on the way out it writes the call's limit, filter and date range
 * back into the caller's stored console settings - so asking a question here would close the
 * connection the rest of this request needs, and change what that person sees the next time they
 * open the survey in a browser.  Going straight to the manager underneath avoids both.
 */
public class DataQueryTool extends AbstractMcpTool {

	/* Enough to answer a question, small enough to leave room to think about the answer */
	private static final int DEFAULT_LIMIT = 100;

	@Override
	public String getName() {
		return "data_query";
	}

	@Override
	public String getTitle() {
		return "Query survey data";
	}

	@Override
	public String getDescription() {
		return "Returns submitted records for one survey, with filtering, sorting and paging. "
				+ "Use survey_list first to find the survey id, and the resource "
				+ "smap://survey/{ident}/definition to learn the question names. "
				+ "filter is an expression over question names written as "
				+ "${question} = 'value', combined with and, or and not, for example "
				+ "${age} > 30 and ${district} = 'North'. Returns at most "
				+ DEFAULT_LIMIT + " records unless limit says otherwise; when more remain, "
				+ "next_cursor is returned and should be passed back as cursor for the next page. "
				+ "Attachments appear as https URLs that need a browser login; to look at one, read "
				+ "the resource smap://attachment/{survey ident}/{file}, which is the part of that "
				+ "URL after /attachments/.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to read, from survey_list"),
				"filter", property("string",
						"Optional. An expression over question names, such as "
						+ "${status} = 'complete' and ${age} > 30. Every name must be a question "
						+ "in this survey."),
				"sort", property("string",
						"Optional. The question name to sort on. Defaults to submission order, "
						+ "which is the only order that can be paged."),
				"direction", property("string", "Optional. asc or desc. Default asc."),
				"limit", property("integer",
						"Optional. Maximum records to return. Default " + DEFAULT_LIMIT + "."),
				"cursor", property("integer",
						"Optional. next_cursor from a previous call, to read the following page."),
				"form", property("string",
						"Optional. The name of a repeating group, to read its rows instead of the "
						+ "main form."),
				"date_question", property("string",
						"Optional. A date question to restrict by, used with start_date and "
						+ "end_date."),
				"start_date", property("string", "Optional. yyyy-MM-dd, needs date_question."),
				"end_date", property("string", "Optional. yyyy-MM-dd, needs date_question."),
				"include_deleted", property("string",
						"Optional. none, yes or only. Records deleted in Smap are marked rather "
						+ "than removed, so they can still be read. Default none."),
				"include_meta", property("boolean",
						"Include metadata such as upload time, device and instance id. Default "
						+ "true. Paging and data_get_record both need the instance id, so turning "
						+ "this off returns a single page of records that cannot be followed up."));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> records = new LinkedHashMap<>();
		records.put("type", "array");
		records.put("description", "One entry per record, in the order asked for");

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("records", records);
		properties.put("count", property("integer", "How many records this page holds"));
		properties.put("next_cursor", property("integer",
				"Pass back as cursor to read the next page. Absent when there are no more."));
		properties.put("truncated", property("boolean",
				"More records match than were returned, but they cannot be paged to: paging "
				+ "follows submission order, so a custom sort or include_meta false gives one "
				+ "page only."));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		int limit = ctx.cap(intArg(arguments, "limit", DEFAULT_LIMIT));
		if(limit <= 0) {
			limit = DEFAULT_LIMIT;
		}
		int cursor = intArg(arguments, "cursor", 0);
		boolean includeMeta = boolArg(arguments, "include_meta", true);

		String sort = stringArg(arguments, "sort");
		String direction = stringArg(arguments, "direction");
		direction = (direction != null && direction.equalsIgnoreCase("desc")) ? "desc" : "asc";

		String includeDeleted = stringArg(arguments, "include_deleted");
		if(includeDeleted == null || includeDeleted.trim().isEmpty()) {
			includeDeleted = "none";
		}
		boolean includeBad = includeDeleted.equals("yes") || includeDeleted.equals("only");

		/*
		 * Which form to read.  The main form unless a repeating group was named, in which case the
		 * manager joins back up to the top level form so the row filters still reach it.
		 *
		 * The form id matters even for the main form: the columns are looked up per form, so
		 * leaving it at zero finds no questions and returns records made entirely of metadata.
		 */
		Form topForm = GeneralUtilityMethods.getTopLevelForm(ctx.sd, surveyId);
		String formName = stringArg(arguments, "form");
		int fId = topForm.id;
		String tableName = topForm.tableName;
		int parentForm = 0;
		boolean isChildForm = false;
		if(formName != null && !formName.trim().isEmpty()) {
			int childId = GeneralUtilityMethods.getFormId(ctx.sd, surveyId, formName.trim());
			Form child = childId > 0 ? McpData.form(ctx, surveyId, childId) : null;
			if(child == null) {
				return new MCPToolResult("No repeating group called " + formName
						+ " in this survey. Read smap://survey/" + survey.getIdent()
						+ "/definition to see which there are.", true);
			}
			fId = child.id;
			tableName = child.tableName;
			parentForm = child.parentform;
			isChildForm = true;
		}

		ArrayList<TableColumn> columns = McpData.columns(ctx, survey, parentForm, fId,
				tableName, isChildForm, includeBad, includeMeta);

		/*
		 * One record more than asked for, so the presence of a next page is known rather than
		 * guessed.  Asking for exactly the limit cannot tell a full page from the last one.
		 */
		int fetch = limit + 1;

		TableDataManager tdm = new TableDataManager(ctx.localisation, ctx.timezone);
		PreparedStatement pstmt = null;
		List<Object> records = new ArrayList<>();
		Integer lastKey = null;

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
					sort,
					direction,
					false,					// mgmt
					false,					// group
					false,					// isDt
					cursor,					// start: the primary key to begin at, inclusive
					isChildForm,			// getParkey
					0,						// start_parkey
					ctx.superUser,
					false,					// specificPrikey: records are never addressed by key
					includeDeleted,
					"yes",					// include_completed
					null,					// case management settings, not needed to read data
					null,					// custom filter
					null,					// key filters
					ctx.timezone,
					null,					// instanceId: data_get_record does single records
					stringArg(arguments, "filter"),
					stringArg(arguments, "date_question"),
					parseDate(stringArg(arguments, "start_date")),
					parseDate(stringArg(arguments, "end_date")));

			if(pstmt == null) {
				return new MCPToolResult("This survey has no data yet.", false);
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
			while(row != null && records.size() < fetch) {
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
					if(records.size() < limit) {
						records.add(toPlain(row));
						Integer key = key(row);
						if(key != null) {
							lastKey = key;
						}
					} else {
						/*
						 * The extra record is read but never returned.  It exists only to answer
						 * whether there is another page.
						 */
						records.add(null);
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

		boolean more = records.size() > limit;
		List<Object> page = more ? records.subList(0, limit) : records;

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("records", page);
		structured.put("count", page.size());
		/*
		 * Paging follows the primary key, so it only works while the rows come back in key order.
		 * A caller who asked for a different sort gets no cursor rather than one that would silently
		 * skip records.
		 */
		if(more && sort == null && lastKey != null) {
			structured.put("next_cursor", lastKey + 1);
		} else if(more) {
			structured.put("truncated", Boolean.TRUE);
		}

		MCPToolResult result = new MCPToolResult(new com.google.gson.Gson().toJson(structured));
		result.setStructuredContent(structured);
		return result;
	}

	/*
	 * The primary key of a row, if it is there.
	 *
	 * Read as a string and parsed rather than through getInt, because the record builder writes
	 * every column as text, so the key arrives as "1" rather than 1.
	 */
	private Integer key(JSONObject row) {
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
	 * represents, so rows are converted to plain maps before they go anywhere near the result.
	 */
	private Object toPlain(JSONObject row) {
		return new com.google.gson.Gson().fromJson(row.toString(), Object.class);
	}

	private java.sql.Date parseDate(String value) {
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
