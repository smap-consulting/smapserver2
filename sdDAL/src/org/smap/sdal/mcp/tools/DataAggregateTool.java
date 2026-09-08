package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.DataAggregateManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Grouping and summing in the database rather than in the model.
 *
 * "How many submissions per district" is a question an agent should be able to ask without reading
 * every record to work it out.  Smap had no server side aggregation - the console fetches rows and
 * adds them up in the browser - so this is the first path that answers it, and it is the reason the
 * increment is called data and analysis rather than just data.
 */
public class DataAggregateTool extends AbstractMcpTool {

	/* Enough groups to see a distribution, few enough to read */
	private static final int DEFAULT_LIMIT = 50;

	@Override
	public String getName() {
		return "data_aggregate";
	}

	@Override
	public String getTitle() {
		return "Group and summarise survey data";
	}

	@Override
	public String getDescription() {
		return "Groups records by the answer to one question and returns a summary per group, "
				+ "computed in the database rather than by reading the records. Use this for "
				+ "questions like how many submissions per district, or the average age by region. "
				+ "function defaults to count; sum, avg, min and max need value_question and only "
				+ "work on numeric answers. Groups come back with the largest value first.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to summarise, from survey_list"),
				"group_question", property("string",
						"The question whose answer forms the groups, such as district"),
				"function", property("string",
						"Optional. One of " + DataAggregateManager.functions() + ". Default count."),
				"value_question", property("string",
						"The numeric question to sum or average. Required for every function "
						+ "except count."),
				"filter", property("string",
						"Optional. Restrict which records are summarised, written as "
						+ "${question} = 'value'."),
				"date_question", property("string",
						"Optional. A date question to restrict by, used with start_date and "
						+ "end_date."),
				"start_date", property("string", "Optional. yyyy-MM-dd, needs date_question."),
				"end_date", property("string", "Optional. yyyy-MM-dd, needs date_question."),
				"include_deleted", property("string",
						"Optional. none, yes or only. Default none."),
				"limit", property("integer",
						"Optional. Most groups to return. Default " + DEFAULT_LIMIT + "."));
		schema.put("required", new String[] { "survey_id", "group_question" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> groups = new LinkedHashMap<>();
		groups.put("type", "array");
		groups.put("description",
				"One entry per distinct answer: group, value (the function's result) and records");

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("groups", groups);
		properties.put("function", property("string", "The function that was applied"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		String groupBy = stringArg(arguments, "group_question");
		if(surveyId <= 0 || groupBy == null || groupBy.trim().isEmpty()) {
			return new MCPToolResult(
					"Both survey_id and group_question are required. Read "
					+ "smap://survey/{ident}/definition to see the question names.", true);
		}
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		DataAggregateManager.Query q = McpData.aggregateQuery(ctx, survey, arguments);
		if(q == null) {
			return new MCPToolResult("This survey has no data yet.", false);
		}

		String function = stringArg(arguments, "function");
		int limit = ctx.cap(intArg(arguments, "limit", DEFAULT_LIMIT));
		if(limit <= 0) {
			limit = DEFAULT_LIMIT;
		}

		/*
		 * Nothing is caught here.  A mistake in the arguments arrives as an ApplicationException,
		 * whose message was written for whoever asked, and the dispatcher returns that as it is.
		 * Anything else becomes a reference in the log, which is what should happen to a message
		 * nobody wrote for this audience - a database error naming a column is a description of the
		 * schema, not of what the caller got wrong.
		 */
		List<Map<String, Object>> rows = new DataAggregateManager(ctx.localisation, ctx.timezone)
				.aggregate(ctx.sd, ctx.cResults, q, groupBy, function,
						stringArg(arguments, "value_question"), limit);

		String applied = function == null ? "count" : function.toLowerCase();

		StringBuilder text = new StringBuilder();
		if(rows.isEmpty()) {
			text.append("No records match.");
		} else {
			text.append(applied).append(" by ").append(groupBy).append(":");
			for(Map<String, Object> row : rows) {
				Object group = row.get("group");
				text.append("\n- ")
						.append(group == null || group.toString().isEmpty() ? "(no answer)" : group)
						.append(": ").append(row.get("value"));
			}
		}

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("groups", rows);
		structured.put("function", applied);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(structured);
		return result;
	}
}
