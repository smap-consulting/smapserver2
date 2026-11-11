package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.ws.rs.core.Response;

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
		return "Changes the answers on one record; anything not named is left alone. The value before and "
				+ "after is written to the record's history, so data_audit shows it and it can be put back. "
				+ "To change answers in one row of a repeating group, name the group in form and the row "
				+ "in row.";
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
				+ "here keep the values they have, and so does a question given null - use null for "
				+ "an answer you do not have. An empty string clears the answer instead.");

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("survey_id", property("integer", "The survey the record belongs to"));
		properties.put("instance_id", property("string", "The record to change, from data_query"));
		properties.put("answers", answers);
		properties.put("form", property("string", "Optional. The name of a repeating group of the main "
				+ "form, to change answers in one of its rows rather than in the record itself. Needs row."));
		properties.put("row", property("integer", "With form, the row to change, counting from 1 in the "
				+ "order data_get_record lists the group's rows."));

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
		if(!McpData.recordExists(ctx, survey, instanceId)) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
		}

		/*
		 * A row of a repeating group, when one is named.
		 *
		 * Only groups of the main form, and the row is named by its position in the record rather
		 * than by its primary key: a position can only reach this record's rows, while a primary key
		 * is a sequential number that could name a row of any record.
		 */
		String formName = stringArg(arguments, "form");
		if(formName != null && formName.trim().isEmpty()) {
			formName = null;
		}
		Form targetForm = topForm;
		int rowPrikey = 0;
		int rowNumber = intArg(arguments, "row", 0);
		if(formName != null) {
			targetForm = repeatOfMainForm(ctx, surveyId, topForm.id, formName.trim());
			if(targetForm == null) {
				return new MCPToolResult("\"" + survey.getDisplayName() + "\" has no repeating group called "
						+ formName.trim() + " in its main form. Only groups of the main form can be changed "
						+ "here. Nothing was changed.", true);
			}
			if(rowNumber <= 0) {
				return new MCPToolResult("Name the row of " + targetForm.name + " to change in row, counting "
						+ "from 1. Nothing was changed.", true);
			}
			int rows = countRows(ctx, topForm.tableName, targetForm.tableName, instanceId);
			if(rowNumber > rows) {
				return new MCPToolResult("This record has " + rows + (rows == 1 ? " row" : " rows")
						+ " of " + targetForm.name + ", so there is no row " + rowNumber
						+ ". Nothing was changed.", true);
			}
			rowPrikey = rowKey(ctx, topForm.tableName, targetForm.tableName, instanceId, rowNumber);
		} else if(rowNumber > 0) {
			return new MCPToolResult("row needs form, the repeating group the row belongs to. "
					+ "Nothing was changed.", true);
		}

		ArrayList<TableColumn> columns = McpData.columns(ctx, survey, targetForm.parentform, targetForm.id,
				targetForm.tableName, targetForm.parentform != 0, false, false);

		/*
		 * The columns the update will actually be able to write, which are not the same as this
		 * survey's.
		 *
		 * processUpdateGroupSurvey resolves what to write from the top level form of the BUNDLE, not
		 * of the survey it was handed, so a question belonging to a later stage form in the bundle is
		 * not in its list and is dropped.  Silently: the record was unchanged and the history had no
		 * entry, while this tool reported the questions as updated.
		 *
		 * So the check is made against the same list the manager uses.  A question this survey has
		 * and the bundle root does not is refused, and told apart from a misspelling, because the two
		 * need completely different things from whoever is reading.
		 */
		ArrayList<TableColumn> writable = columns;
		String bundleIdent = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);
		if(bundleIdent != null && !bundleIdent.equals(survey.getIdent())) {
			int bundleSurveyId = GeneralUtilityMethods.getSurveyId(ctx.sd, bundleIdent);
			if(bundleSurveyId > 0) {
				Survey bundleSurvey = McpData.surveyById(ctx, bundleSurveyId);
				if(bundleSurvey != null) {
					Form bundleTop = GeneralUtilityMethods.getTopLevelForm(ctx.sd, bundleSurveyId);
					Form bundleTarget = formName == null ? bundleTop
							: repeatOfMainForm(ctx, bundleSurveyId, bundleTop.id, targetForm.name);
					writable = bundleTarget == null ? new ArrayList<>()
							: McpData.columns(ctx, bundleSurvey, bundleTarget.parentform, bundleTarget.id,
									bundleTarget.tableName, bundleTarget.parentform != 0, false, false);
				}
			}
		}

		/*
		 * Every name is resolved against the survey before anything is written, so a misspelling is
		 * a refusal rather than a change that silently misses.
		 */
		List<Map<String, Object>> updates = new ArrayList<>();
		List<String> changed = new ArrayList<>();
		List<String> unknown = new ArrayList<>();

		List<String> notWritable = new ArrayList<>();
		List<String> conversations = new ArrayList<>();
		List<String> retained = new ArrayList<>();

		for(Map.Entry<String, Object> e : ((Map<String, Object>) answersArg).entrySet()) {
			TableColumn c = column(columns, e.getKey());
			if(c == null) {
				unknown.add(e.getKey());
				continue;
			}
			if("conversation".equals(c.type)) {
				conversations.add(e.getKey());
				continue;
			}
			if(column(writable, e.getKey()) == null) {
				notWritable.add(e.getKey());
				continue;
			}
			String name = c.question_name != null ? c.question_name : c.column_name;

			/*
			 * Null and "" are two different instructions, and the difference matters most to a caller
			 * assembling answers from something that has gaps in it.
			 *
			 * Null says nothing about this question, so the record keeps what it has - the question is
			 * left out of the update entirely rather than written as blank.  "" says the answer is to
			 * be taken away, which is a clearing rather than the text "", the distinction the console
			 * draws and the one the history reads back correctly.
			 */
			if(e.getValue() == null) {
				retained.add(name);
				continue;
			}
			String value = text(e.getValue());

			Map<String, Object> update = new LinkedHashMap<>();
			update.put("name", name);
			update.put("displayName", c.displayName);
			update.put("value", value);
			update.put("clear", value.isEmpty());
			updates.add(update);
			changed.add(name);
		}

		if(!unknown.isEmpty()) {
			return new MCPToolResult("This survey has no question called " + String.join(", ", unknown)
					+ ". Read smap://survey/" + survey.getIdent() + "/definition to see the names.",
					true);
		}

		if(!conversations.isEmpty()) {
			return new MCPToolResult(String.join(", ", conversations)
					+ (conversations.size() == 1 ? " is a conversation" : " are conversations")
					+ ". A conversation only changes when a message is received or sent, never by "
					+ "updating the record. Nothing was changed.", true);
		}

		if(!notWritable.isEmpty()) {
			/*
			 * A real question on a real record that this operation cannot reach.  Said as what it is
			 * rather than as "no such question", which would send somebody looking for a typo that is
			 * not there.
			 */
			return new MCPToolResult(String.join(", ", notWritable)
					+ (notWritable.size() == 1 ? " is a question on \"" : " are questions on \"")
					+ survey.getDisplayName() + "\", but updating a record reaches only the questions "
					+ "of \"" + bundleIdent + "\", the survey this one shares its record with. "
					+ "Nothing was changed.\n\nA later stage of a process is recorded by filling in "
					+ "its own form against the record, not by updating it - which is done in the "
					+ "console. Questions that belong to the first form of the bundle can be updated "
					+ "here.", true);
		}

		if(updates.isEmpty()) {
			/*
			 * Every question given was null, so every one of them was a "leave this alone".  Reported
			 * rather than run: processUpdateGroupSurvey with an empty list writes a change event for a
			 * change that did not happen, and the record would read as having been edited.
			 */
			return new MCPToolResult("Nothing was changed. " + String.join(", ", retained)
					+ (retained.size() == 1 ? " was given null, which means keep the value it has."
							: " were given null, which means keep the values they have.")
					+ " To take an answer away, give it as an empty string.", false);
		}

		/*
		 * The values as they are now, so the history can say what they were.
		 *
		 * The manager only looks them up for a bulk change; for a single one it expects the caller
		 * to supply them, because the console is editing a row it already has on screen. Nothing
		 * here has that, so it reads them. Without this the change is recorded with a new value and
		 * no old one, and the undo this tool promises would have nothing to undo to.
		 */
		Map<String, String> before = formName == null
				? currentValues(ctx, topForm.tableName, "instanceid", instanceId, updates, columns)
				: currentValues(ctx, targetForm.tableName, "prikey", rowPrikey, updates, columns);
		for(Map<String, Object> update : updates) {
			String current = before.get(update.get("name"));
			if(current != null) {
				update.put("currentValue", current);
			}
		}

		ActionManager am = new ActionManager(ctx.localisation, ctx.timezone, ctx.clientId);
		Response response = am.processUpdateGroupSurvey(
				ctx.request,
				ctx.sd,
				ctx.cResults,
				ctx.user,
				surveyId,
				instanceId,
				rowPrikey,			// the row of a repeating group, or 0 for the record itself
				GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId),
				formName == null ? null : targetForm.name,
				new Gson().toJson(updates),
				false,				// not a bulk change
				GeneralUtilityMethods.getUrlPrefix(ctx.request));
		String failure = failure(response);
		if(failure != null) {
			return new MCPToolResult(failure + " Nothing was changed.", true);
		}

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("updated", Boolean.TRUE);
		structured.put("instanceid", instanceId);
		structured.put("questions", changed);
		structured.put("retained", retained);

		if(formName != null) {
			structured.put("form", targetForm.name);
			structured.put("row", rowNumber);
		}

		StringBuilder text = new StringBuilder("Changed " + String.join(", ", changed)
				+ (formName == null ? "" : " in row " + rowNumber + " of " + targetForm.name)
				+ ". data_audit shows the values before and after.");
		if(!retained.isEmpty()) {
			text.append("\n\n").append(String.join(", ", retained))
					.append(retained.size() == 1 ? " was given null, so it keeps the value it had."
							: " were given null, so they keep the values they had.");
		}
		MCPToolResult result = new MCPToolResult(text.toString());
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
	private Map<String, String> currentValues(McpToolContext ctx, String tableName, String keyColumn, Object key,
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
		select.append(" from ").append(tableName).append(" where ").append(keyColumn).append(" = ?");

		try (java.sql.PreparedStatement pstmt = ctx.cResults.prepareStatement(select.toString())) {
			pstmt.setObject(1, key);
			java.sql.ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				for(int i = 0; i < names.size(); i++) {
					values.put(names.get(i), rs.getString(i + 1));
				}
			}
		}
		return values;
	}

	/* The message of an update that did not succeed, or null */
	static String failure(Response response) {
		if(response == null || response.getStatus() < 300) {
			return null;
		}
		Object entity = response.getEntity();
		return entity == null || entity.toString().trim().isEmpty() ? "The change could not be made."
				: entity.toString();
	}

	/* A repeating group directly in the main form, by name */
	private Form repeatOfMainForm(McpToolContext ctx, int sId, int topFormId, String name) throws Exception {
		String sql = "select f_id, parentform, table_name, name from form "
				+ "where s_id = ? and parentform = ? and not reference and lower(name) = lower(?)";
		try (java.sql.PreparedStatement pstmt = ctx.sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			pstmt.setInt(2, topFormId);
			pstmt.setString(3, name);
			java.sql.ResultSet rs = pstmt.executeQuery();
			if(!rs.next()) {
				return null;
			}
			Form f = new Form();
			f.id = rs.getInt(1);
			f.parentform = rs.getInt(2);
			f.tableName = rs.getString(3);
			f.name = rs.getString(4);
			return f;
		}
	}

	/* How many rows of a repeating group a record has */
	private int countRows(McpToolContext ctx, String topTable, String repeatTable, String instanceId)
			throws Exception {
		if(!GeneralUtilityMethods.tableExists(ctx.cResults, repeatTable)) {
			return 0;
		}
		String sql = "select count(*) from " + repeatTable + " r, " + topTable + " m "
				+ "where r.parkey = m.prikey and m.instanceid = ?";
		try (java.sql.PreparedStatement pstmt = ctx.cResults.prepareStatement(sql)) {
			pstmt.setString(1, instanceId);
			java.sql.ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	/* The primary key of the nth row of a repeating group in a record, in the order the rows are read */
	private int rowKey(McpToolContext ctx, String topTable, String repeatTable, String instanceId, int n)
			throws Exception {
		String sql = "select r.prikey from " + repeatTable + " r, " + topTable + " m "
				+ "where r.parkey = m.prikey and m.instanceid = ? "
				+ "order by r.prikey asc offset ? limit 1";
		try (java.sql.PreparedStatement pstmt = ctx.cResults.prepareStatement(sql)) {
			pstmt.setString(1, instanceId);
			pstmt.setInt(2, n - 1);
			java.sql.ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
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
