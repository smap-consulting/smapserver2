package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpRead;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumn;

/*
 * Change the answers on one record.
 *
 * Does not ask first. Each change is written to the record's history with the value before and the
 * value after, so it can be read back and put right, and it touches one record. The question is
 * saved for the things that leave the building.
 *
 * Goes through the same manager the console uses, which is what makes the change look like every
 * other change: the history entry, the update notifications that fire on a console_update trigger,
 * and the linked form housekeeping all happen because that code is doing them, not because this
 * tool remembered to.
 */
public class DataUpdateRecordTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_update_record";
	}

	@Override
	public String getTitle() {
		return "Change a record";
	}

	@Override
	public String getDescription() {
		return "Changes the answers on one submitted record. Give the new answers keyed by question "
				+ "name; anything not named is left alone. The value before and after is written to "
				+ "the record's history, so data_audit will show what changed and it can be put "
				+ "back. Records are named by instance id, which data_query returns as instanceid.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.WRITE;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "data_update_record again with the previous values, which data_audit records as "
				+ "oldVal for every change";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> answers = new LinkedHashMap<>();
		answers.put("type", "object");
		answers.put("description", "The new answers, keyed by question name. Questions not named "
				+ "here keep the values they have.");

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("survey_id", property("integer", "The survey the record belongs to"));
		properties.put("instance_id", property("string", "The record to change, from data_query"));
		properties.put("answers", answers);

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "survey_id", "instance_id", "answers" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("updated", property("boolean", "Whether the record was changed"));
		properties.put("instanceid", property("string", "The record that was changed"));
		properties.put("questions", property("array", "The questions that were given new answers"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	@SuppressWarnings("unchecked")
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		String instanceId = stringArg(arguments, "instance_id");
		Object answersArg = arguments.get("answers");

		if(surveyId <= 0 || instanceId == null || instanceId.trim().isEmpty()
				|| !(answersArg instanceof Map) || ((Map<String, Object>) answersArg).isEmpty()) {
			return new MCPToolResult("A survey_id, an instance_id and some answers are required. "
					+ "Use data_query to find the record.", true);
		}
		instanceId = instanceId.trim();

		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		/*
		 * Seeing the survey is not permission to change a record in it, and a record outside the
		 * caller's row filters is answered the same way as one that does not exist.
		 */
		if(!McpData.canSeeRecord(ctx, survey, instanceId)) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
		}

		Form topForm = GeneralUtilityMethods.getTopLevelForm(ctx.sd, surveyId);
		ArrayList<TableColumn> columns = McpData.columns(ctx, survey, 0, topForm.id,
				topForm.tableName, false, false, false);

		/*
		 * Every name is resolved against the survey before anything is written, so a misspelling is
		 * a refusal rather than a change that silently misses.
		 */
		List<Map<String, Object>> updates = new ArrayList<>();
		List<String> changed = new ArrayList<>();
		List<String> unknown = new ArrayList<>();

		for(Map.Entry<String, Object> e : ((Map<String, Object>) answersArg).entrySet()) {
			TableColumn c = column(columns, e.getKey());
			if(c == null) {
				unknown.add(e.getKey());
				continue;
			}
			String value = e.getValue() == null ? "" : e.getValue().toString();

			Map<String, Object> update = new LinkedHashMap<>();
			update.put("name", c.question_name != null ? c.question_name : c.column_name);
			update.put("displayName", c.displayName);
			update.put("value", value);
			/*
			 * An empty answer is a clearing rather than the text "", which is the distinction the
			 * console draws and the one the history reads back correctly.
			 */
			update.put("clear", value.isEmpty());
			updates.add(update);
			changed.add(c.question_name != null ? c.question_name : c.column_name);
		}

		if(!unknown.isEmpty()) {
			return new MCPToolResult("This survey has no question called " + String.join(", ", unknown)
					+ ". Read smap://survey/" + survey.getIdent() + "/definition to see the names.",
					true);
		}

		/*
		 * The values as they are now, so the history can say what they were.
		 *
		 * The manager only looks them up for a bulk change; for a single one it expects the caller
		 * to supply them, because the console is editing a row it already has on screen. Nothing
		 * here has that, so it reads them. Without this the change is recorded with a new value and
		 * no old one, and the undo this tool promises would have nothing to undo to.
		 */
		Map<String, String> before = currentValues(ctx, topForm.tableName, instanceId, updates, columns);
		for(Map<String, Object> update : updates) {
			String current = before.get(update.get("name"));
			if(current != null) {
				update.put("currentValue", current);
			}
		}

		ActionManager am = new ActionManager(ctx.localisation, ctx.timezone, ctx.clientId);
		am.processUpdateGroupSurvey(
				ctx.request,
				ctx.sd,
				ctx.cResults,
				ctx.user,
				surveyId,
				instanceId,
				0,					// no sub form primary key: the top level record
				GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId),
				null,				// groupForm: the main form
				new Gson().toJson(updates),
				false,				// not a bulk change
				GeneralUtilityMethods.getUrlPrefix(ctx.request));

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("updated", Boolean.TRUE);
		structured.put("instanceid", instanceId);
		structured.put("questions", changed);

		MCPToolResult result = new MCPToolResult("Changed " + String.join(", ", changed)
				+ ". data_audit shows the values before and after.");
		result.setStructuredContent(structured);
		return result;
	}

	/*
	 * What the record says now for the questions about to change.
	 *
	 * Each column comes from the survey's own definition rather than from the caller, so the names
	 * written into the query are ones this server chose. Read in one pass: a record being changed
	 * is being read anyway, and doing it per question would be a query each.
	 */
	private Map<String, String> currentValues(McpToolContext ctx, String tableName, String instanceId,
			List<Map<String, Object>> updates, ArrayList<TableColumn> columns) throws Exception {

		Map<String, String> values = new LinkedHashMap<>();
		if(updates.isEmpty()) {
			return values;
		}

		List<String> names = new ArrayList<>();
		StringBuilder select = new StringBuilder("select ");
		for(Map<String, Object> update : updates) {
			TableColumn c = column(columns, (String) update.get("name"));
			if(c == null || !GeneralUtilityMethods.hasColumn(ctx.cResults, tableName, c.column_name)) {
				continue;		// A question with no column of its own has no previous value to read
			}
			if(!names.isEmpty()) {
				select.append(", ");
			}
			select.append(c.column_name);
			names.add((String) update.get("name"));
		}
		if(names.isEmpty()) {
			return values;
		}
		select.append(" from ").append(tableName).append(" where instanceid = ?");

		try (java.sql.PreparedStatement pstmt = ctx.cResults.prepareStatement(select.toString())) {
			pstmt.setString(1, instanceId);
			java.sql.ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				for(int i = 0; i < names.size(); i++) {
					values.put(names.get(i), rs.getString(i + 1));
				}
			}
		}
		return values;
	}

	/* A question of this survey, by any of the names a caller might know it by */
	private TableColumn column(ArrayList<TableColumn> columns, String name) {
		if(name == null) {
			return null;
		}
		String wanted = name.trim();
		for(TableColumn c : columns) {
			if(wanted.equalsIgnoreCase(c.question_name) || wanted.equalsIgnoreCase(c.column_name)
					|| wanted.equalsIgnoreCase(McpRead.jsonKey(c))) {
				return c;
			}
		}
		return null;
	}
}
