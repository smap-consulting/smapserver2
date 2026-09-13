package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.DataItemChangeEvent;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * What has happened to one record since it arrived.
 *
 * History is kept per thread rather than per submission: correcting a record writes a new instance
 * that joins the same thread, so asking about any instance in it returns the whole story rather than
 * the fragment that instance took part in.
 *
 * The console reaches this through a survey level check only.  That is not enough here for the same
 * reason it was not enough for reading records: a role can restrict a caller to their own rows, and
 * the history of a record is the record, so the row filters are applied before the events are read.
 */
public class DataAuditTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_audit";
	}

	@Override
	public String getTitle() {
		return "Record history";
	}

	@Override
	public String getDescription() {
		return "Returns the history of one record: when it was submitted, every later change with "
				+ "the old and new values, who made it, and any task or notification that touched "
				+ "it. Records are named by instance id, which data_query returns as instanceid. "
				+ "Corrections create a new instance in the same thread, so this returns the whole "
				+ "history rather than one submission's part in it.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the record was submitted to"),
				"instance_id", property("string", "The record's instance id, from data_query"));
		schema.put("required", new String[] { "survey_id", "instance_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> events = new LinkedHashMap<>();
		events.put("type", "array");
		events.put("description", "Events on this record, most recent first");

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("events", events);
		properties.put("count", property("integer", "How many events there are"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		String instanceId = stringArg(arguments, "instance_id");
		if(surveyId <= 0 || instanceId == null || instanceId.trim().isEmpty()) {
			return new MCPToolResult(
					"Both survey_id and instance_id are required. Use data_query to find them.",
					true);
		}

		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		/*
		 * The same answer whether the record is outside the caller's row filters or does not exist,
		 * so that asking cannot be used to find out which records are there.
		 */
		if(!McpData.canSeeRecord(ctx, survey, instanceId)) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
		}

		String tableName = GeneralUtilityMethods.getMainResultsTable(ctx.sd, ctx.cResults, surveyId);
		String thread = GeneralUtilityMethods.getThread(ctx.cResults, tableName, instanceId);

		RecordEventManager rem = new RecordEventManager();
		ArrayList<DataItemChangeEvent> events = rem.getChangeEvents(ctx.sd, ctx.timezone,
				tableName, thread, false);

		StringBuilder text = new StringBuilder();
		if(events == null || events.isEmpty()) {
			text.append("No history recorded for this record.");
		} else {
			text.append(events.size()).append(" event(s), most recent first.");
		}

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("events", events == null ? new ArrayList<>() : events);
		structured.put("count", events == null ? 0 : events.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(structured);
		return result;
	}
}
