package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpRead;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

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
				+ "Pass select to return only the questions you need; a survey can have dozens of "
				+ "questions and reading them all when you want two is slow and fills your "
				+ "context. "
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
				"select", selectProperty(),
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

	/* An array of question names, described so a model offers names rather than a comma joined string */
	private static Map<String, Object> selectProperty() {
		Map<String, Object> items = new LinkedHashMap<>();
		items.put("type", "string");

		Map<String, Object> p = new LinkedHashMap<>();
		p.put("type", "array");
		p.put("items", items);
		p.put("description", "Optional. The question names to return, such as [\"q1\", \"q3\"]. "
				+ "Omit for every question. Questions named in filter or sort are returned as well, "
				+ "and the instance id always is, so the records can be followed up.");
		return p;
	}

	/*
	 * A list argument, tolerating the string a client sends when it flattens an array rather than
	 * refusing it, since the intent is not in doubt.
	 */
	private static List<String> stringList(Map<String, Object> args, String name) {
		Object v = args.get(name);
		if(v == null) {
			return null;
		}
		List<String> out = new ArrayList<>();
		if(v instanceof List) {
			for(Object o : (List<?>) v) {
				if(o != null && !o.toString().trim().isEmpty()) {
					out.add(o.toString().trim());
				}
			}
		} else {
			for(String part : v.toString().split(",")) {
				if(!part.trim().isEmpty()) {
					out.add(part.trim());
				}
			}
		}
		return out.isEmpty() ? null : out;
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

		McpRead.Request r = new McpRead.Request();
		r.survey = survey;
		r.formName = stringArg(arguments, "form");
		r.filter = stringArg(arguments, "filter");
		r.sort = stringArg(arguments, "sort");
		String direction = stringArg(arguments, "direction");
		r.direction = (direction != null && direction.equalsIgnoreCase("desc")) ? "desc" : "asc";
		r.cursor = intArg(arguments, "cursor", 0);
		r.limit = ctx.cap(intArg(arguments, "limit", DEFAULT_LIMIT));
		if(r.limit <= 0) {
			r.limit = DEFAULT_LIMIT;
		}
		r.includeMeta = boolArg(arguments, "include_meta", true);
		r.select = stringList(arguments, "select");
		r.includeDeleted = McpData.includeDeleted(stringArg(arguments, "include_deleted"));
		r.dateQuestion = stringArg(arguments, "date_question");
		r.startDate = McpRead.parseDate(stringArg(arguments, "start_date"));
		r.endDate = McpRead.parseDate(stringArg(arguments, "end_date"));

		McpRead.Result read = McpRead.read(ctx, r);
		if(read.noData) {
			return new MCPToolResult("This survey has no data yet.", false);
		}

		List<Object> records = new ArrayList<>();
		for(JSONObject row : read.rows) {
			records.add(McpRead.toPlain(row));
		}

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("records", records);
		structured.put("count", records.size());
		/*
		 * Paging follows the primary key, so it only works while the rows come back in key order.
		 * A caller who asked for a different sort gets no cursor rather than one that would silently
		 * skip records.
		 */
		if(read.more && r.sort == null && read.lastKey != null) {
			structured.put("next_cursor", read.lastKey + 1);
		} else if(read.more) {
			structured.put("truncated", Boolean.TRUE);
		}

		MCPToolResult result = new MCPToolResult(new com.google.gson.Gson().toJson(structured));
		result.setStructuredContent(structured);
		return result;
	}
}
