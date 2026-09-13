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
 * How many records match, which data_query cannot answer.
 *
 * A read that returns rows can only report how many it returned, and the page limit means that is
 * usually not the number the question was about.  Counting in the database answers "how much is
 * there" without a row per record reaching the model.
 */
public class DataCountTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_count";
	}

	@Override
	public String getTitle() {
		return "Count survey records";
	}

	@Override
	public String getDescription() {
		return "Counts the submitted records matching a filter, without returning them. Use this "
				+ "rather than data_query when the question is how many, because data_query only "
				+ "reports how many it returned, which is capped. filter is an expression over "
				+ "question names written as ${question} = 'value', combined with and, or and not.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to count, from survey_list"),
				"filter", property("string",
						"Optional. An expression over question names, such as "
						+ "${status} = 'complete'. Omit to count every record."),
				"date_question", property("string",
						"Optional. A date question to restrict by, used with start_date and "
						+ "end_date."),
				"start_date", property("string", "Optional. yyyy-MM-dd, needs date_question."),
				"end_date", property("string", "Optional. yyyy-MM-dd, needs date_question."),
				"include_deleted", property("string",
						"Optional. none, yes or only. Default none."));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("count", property("integer", "How many records match"));

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

		DataAggregateManager.Query q = McpData.aggregateQuery(ctx, survey, arguments);
		if(q == null) {
			return new MCPToolResult("This survey has no data yet.", false);
		}

		/* See DataAggregateTool: argument mistakes carry their own message, anything else must not */
		long count = new DataAggregateManager(ctx.localisation, ctx.timezone)
				.count(ctx.sd, ctx.cResults, q);

		/*
		 * Say what was left out, rather than returning a bare number that quietly disagrees with the
		 * total someone can see elsewhere.  Cheap to say and it costs no second query.
		 *
		 * Not called deleted, because the flag covers two different things.  A record marked _bad is
		 * either one somebody deleted or an earlier version of a record that has since been updated:
		 * an update writes a new row and marks the old one with a reason like "Merged with 11".
		 * Excluding those is not hiding data, it is refusing to count one record twice, and saying
		 * "deleted" would send a reader looking for something that was never lost.
		 */
		String excluded = "none".equals(q.includeBad)
				? " Superseded and deleted records are not counted, so a record updated since it"
					+ " was submitted counts once; pass include_deleted to include them."
				: "";

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("count", count);
		structured.put("include_deleted", q.includeBad);

		MCPToolResult result = new MCPToolResult(count + " record(s) match." + excluded);
		result.setStructuredContent(structured);
		return result;
	}
}
