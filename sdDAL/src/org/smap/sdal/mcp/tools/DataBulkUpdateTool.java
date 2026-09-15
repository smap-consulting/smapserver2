package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
 * Change the same answers on many records at once.
 *
 * Every record the change touches records the same change set identifier, so what happened is one
 * action rather than a pile of unrelated edits that share a timestamp, and data_bulk_undo can put
 * the whole of it back.
 *
 * Asks first once the change is large enough that nobody could check it by eye afterwards. Below
 * that a mistake is a nuisance to undo and above it undoing is a project, even with the undo tool -
 * which is a reason to be sure, not a reason to skip asking.
 */
public class DataBulkUpdateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_bulk_update";
	}

	@Override
	public String getTitle() {
		return "Change many records";
	}

	@Override
	public String getDescription() {
		return "Sets the same answers on every record matching a filter. Check data_count with the same "
				+ "filter first. Every record is tagged with one change set id that data_bulk_undo puts back. A "
				+ "large change asks for confirmation.";
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
		return "data_bulk_undo with the change set id this returns, which puts every record back to "
				+ "the value it held";
	}

	@Override
	public Confirmation getConfirmation() {
		return Confirmation.CONDITIONAL;
	}

	@Override
	public Map<String, Object> getAnnotations() {
		Map<String, Object> a = super.getAnnotations();
		a.put("destructiveHint", Boolean.TRUE);
		return a;
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> answers = new LinkedHashMap<>();
		answers.put("type", "object");
		answers.put("description", "The answers to set on every matching record, keyed by question "
				+ "name. Questions not named keep the values they have, and so does a question "
				+ "given null. An empty string clears the answer instead.");

		Map<String, Object> acknowledge = new LinkedHashMap<>();
		acknowledge.put("type", "object");
		acknowledge.put("description", "Only needed when this client cannot ask the user to approve "
				+ "mid-request. Repeat the number of records as {\"records\": n}. It must match "
				+ "what the filter actually selects, so it cannot be guessed, and repeating it puts "
				+ "the size of the change in front of whoever approves the call.");

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("survey_id", property("integer", "The survey to change records in"));
		properties.put("filter", property("string",
				"Which records to change, written as ${question} = 'value'. Required: there is no "
				+ "way to ask for every record, because that is rarely what anyone means."));
		properties.put("answers", answers);
		properties.put("acknowledge", acknowledge);

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "survey_id", "filter", "answers" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("updated", property("integer", "How many records were changed"));
		properties.put("change_set", property("string",
				"Give this to data_bulk_undo to put them all back"));
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
		String filter = stringArg(arguments, "filter");
		Object answersArg = arguments.get("answers");

		if(surveyId <= 0 || filter == null || filter.trim().isEmpty()
				|| !(answersArg instanceof Map) || ((Map<String, Object>) answersArg).isEmpty()) {
			return new MCPToolResult("A survey_id, a filter and some answers are required.", true);
		}

		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		Form topForm = GeneralUtilityMethods.getTopLevelForm(ctx.sd, surveyId);
		ArrayList<TableColumn> columns = McpData.columns(ctx, survey, 0, topForm.id,
				topForm.tableName, false, false, true);

		/*
		 * What the update can actually write, which is not the same as what this survey has.
		 *
		 * processUpdateGroupSurvey resolves its columns from the top level form of the BUNDLE, so a
		 * question belonging to a later stage form is dropped without a word.  Here that would be
		 * worse than in a single update: the answer names a count of records changed, and every one
		 * of them would be unchanged.
		 */
		ArrayList<TableColumn> writable = columns;
		String bundleIdent = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);
		if(bundleIdent != null && !bundleIdent.equals(survey.getIdent())) {
			int bundleSurveyId = GeneralUtilityMethods.getSurveyId(ctx.sd, bundleIdent);
			if(bundleSurveyId > 0) {
				Survey bundleSurvey = McpData.surveyById(ctx, bundleSurveyId);
				if(bundleSurvey != null) {
					Form bundleTop = GeneralUtilityMethods.getTopLevelForm(ctx.sd, bundleSurveyId);
					writable = McpData.columns(ctx, bundleSurvey, 0, bundleTop.id,
							bundleTop.tableName, false, false, true);
				}
			}
		}

		/*
		 * Which records, read through the row filtered path, so a caller only ever changes records
		 * they could have read. The filter is validated there too, so a bad one is refused before
		 * anything is written.
		 */
		McpRead.Request r = new McpRead.Request();
		r.survey = survey;
		r.filter = filter;
		r.limit = ctx.cap(0);
		r.includeMeta = true;
		McpRead.Result read = McpRead.read(ctx, r);
		if(read.noData) {
			return new MCPToolResult("This survey has no data yet.", false);
		}

		List<String> instances = new ArrayList<>();
		for(org.codehaus.jettison.json.JSONObject row : read.rows) {
			String id = McpRead.instanceId(row);
			if(id != null) {
				instances.add(id);
			}
		}
		if(instances.isEmpty()) {
			return new MCPToolResult("No records match that filter, so nothing was changed.", false);
		}

		/*
		 * Above the threshold this stops and asks, because a change nobody can check afterwards is
		 * one somebody should agree to first.
		 */
		if(instances.size() > BULK_THRESHOLD) {
			if(refused(ctx)) {
				return new MCPToolResult("Not changed. Nothing was written.", false);
			}
			if(!approved(ctx)) {
				String what = "Change " + instances.size() + " records in \""
						+ survey.getDisplayName() + "\"?\n\nEvery one of them will be set to the "
						+ "same answers. They can all be put back with data_bulk_undo.";
				if(ctx.canElicit()) {
					return ask(ctx, what);
				}
				Map<String, Object> ack = arguments.get("acknowledge") instanceof Map
						? (Map<String, Object>) arguments.get("acknowledge")
						: null;
				Integer claimed = ack == null ? null : count(ack);
				if(claimed == null || claimed != instances.size()) {
					return new MCPToolResult(what
							+ "\n\nThis client cannot ask you to approve that mid-request. Show the "
							+ "person the number, and if they agree call again with acknowledge set "
							+ "to {\"records\": " + instances.size() + "}.", true);
				}
			}
		}

		/* Names resolved against the survey before anything is written */
		List<Map<String, Object>> updates = new ArrayList<>();
		List<String> changed = new ArrayList<>();
		List<String> unknown = new ArrayList<>();
		List<String> notWritable = new ArrayList<>();
		List<String> retained = new ArrayList<>();
		for(Map.Entry<String, Object> e : ((Map<String, Object>) answersArg).entrySet()) {
			TableColumn c = column(columns, e.getKey());
			if(c == null) {
				unknown.add(e.getKey());
				continue;
			}
			if(column(writable, e.getKey()) == null) {
				notWritable.add(e.getKey());
				continue;
			}
			String name = c.question_name != null ? c.question_name : c.column_name;

			/*
			 * Null means leave this question as it is on every record, so it is left out of the update
			 * altogether; "" means take the answer away.  Across many records the difference is the
			 * whole change: written as blank, a null would empty a question on every record matched.
			 */
			if(e.getValue() == null) {
				retained.add(name);
				continue;
			}
			String value = e.getValue().toString();

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

		if(!notWritable.isEmpty()) {
			return new MCPToolResult(String.join(", ", notWritable)
					+ (notWritable.size() == 1 ? " is a question on \"" : " are questions on \"")
					+ survey.getDisplayName() + "\", but a bulk change reaches only the questions of "
					+ "\"" + bundleIdent + "\", the survey this one shares its record with. No record "
					+ "was changed.", true);
		}

		if(updates.isEmpty()) {
			return new MCPToolResult("No record was changed. " + String.join(", ", retained)
					+ (retained.size() == 1 ? " was given null, which means keep the value it has."
							: " were given null, which means keep the values they have.")
					+ " To take an answer away, give it as an empty string.", false);
		}

		/*
		 * One identifier for the whole change. Generated before the first record is touched, so
		 * every event carries it even if the run stops part way - a half finished bulk change is
		 * exactly when being able to undo the part that happened matters most.
		 */
		String changeSet = UUID.randomUUID().toString();
		String updateString = new Gson().toJson(updates);
		String groupSurvey = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);
		String urlPrefix = GeneralUtilityMethods.getUrlPrefix(ctx.request);

		ActionManager am = new ActionManager(ctx.localisation, ctx.timezone, ctx.clientId, changeSet);
		int updated = 0;
		for(String instanceId : instances) {
			/*
			 * bulk true, which is what makes the manager read each record's previous value before
			 * writing, and what makes a select multiple merge rather than replace.
			 */
			am.processUpdateGroupSurvey(ctx.request, ctx.sd, ctx.cResults, ctx.user, surveyId,
					instanceId, 0, groupSurvey, null, updateString, true, urlPrefix);
			updated++;
		}

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("updated", updated);
		structured.put("change_set", changeSet);
		structured.put("questions", changed);
		structured.put("retained", retained);

		StringBuilder text = new StringBuilder("Changed " + updated + " record(s). "
				+ "To put them back, call data_bulk_undo with change_set " + changeSet + ".");
		if(!retained.isEmpty()) {
			text.append("\n\n").append(String.join(", ", retained))
					.append(retained.size() == 1 ? " was given null, so every record keeps the value "
							+ "it had." : " were given null, so every record keeps the values it had.");
		}
		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(structured);
		return result;
	}

	private static Integer count(Map<String, Object> ack) {
		Object v = ack.get("records");
		if(v instanceof Number) {
			return ((Number) v).intValue();
		}
		try {
			return v == null ? null : Integer.valueOf(v.toString().trim());
		} catch (NumberFormatException e) {
			return null;
		}
	}

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
