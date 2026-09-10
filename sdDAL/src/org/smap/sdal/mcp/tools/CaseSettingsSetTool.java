package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.CaseManagementSettings;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Turn a survey's records into cases, or change how its cases work.
 *
 * A survey becomes a case survey by naming the question that holds a case's status and the value of
 * that question which means the case is finished.  Nothing else changes: the records are the same
 * records, and `_assigned` and `_case_closed` are already beside them.  What this does is tell the
 * server which answer to watch, after which submitting or updating that question moves a case along
 * and the server writes the closing date itself.
 *
 * Which is why the checking here matters more than usual.  A status question that does not exist, or
 * a final status no answer can ever hold, leaves a survey that looks like it manages cases and has
 * no case that ever closes - and nothing complains, because every part of it is individually
 * plausible.  A person setting this up in the console picks from lists and cannot make either
 * mistake; a model can, so the lists are checked here.
 */
public class CaseSettingsSetTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "case_settings_set";
	}

	@Override
	public String getTitle() {
		return "Set up cases";
	}

	@Override
	public String getDescription() {
		return "Makes a survey's records into cases, or changes how its cases work, by naming the "
				+ "question that holds a case's status and the answer that means it is finished. "
				+ "After this, setting that question to that value closes a case and the server "
				+ "records when. Both are checked against the survey's own questions and choices. "
				+ "Returns the previous settings, so this can be put back.";
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
		return "case_settings_set again with the previous settings, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey whose records become cases"),
				"status_question", property("string",
						"The question holding a case's status, by name. survey_questions lists "
						+ "them. Pass an empty string to stop the survey managing cases."),
				"final_status", property("string",
						"The answer to that question which means the case is finished. If the "
						+ "question offers choices this has to be one of them."),
				"criticality_question", property("string",
						"Optional. The question holding how urgent a case is."));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey listed = McpData.surveyById(ctx, surveyId);
		if(listed == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String groupIdent = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);
		CaseManager cm = new CaseManager(ctx.localisation);

		/* What it was, so the caller can put it back */
		CMS existing = cm.getCaseManagementSettings(ctx.sd, groupIdent);
		CaseManagementSettings previousSettings = existing == null || existing.settings == null
				? new CaseManagementSettings() : existing.settings;
		Map<String, Object> previous = new LinkedHashMap<>();
		previous.put("statusQuestion", previousSettings.statusQuestion);
		previous.put("finalStatus", previousSettings.finalStatus);
		previous.put("criticalityQuestion", previousSettings.criticalityQuestion);

		CaseManagementSettings settings = new CaseManagementSettings();
		settings.name = previousSettings.name;
		settings.description = previousSettings.description;

		String statusWanted = stringArg(arguments, "status_question");
		String finalStatus = stringArg(arguments, "final_status");
		String criticalityWanted = stringArg(arguments, "criticality_question");

		/*
		 * An empty status question is how a survey stops managing cases, and it is the one path
		 * through here that does not need the rest to be checked.
		 */
		boolean turningOff = statusWanted != null && statusWanted.trim().isEmpty();

		if(!turningOff) {
			if(statusWanted == null) {
				statusWanted = previousSettings.statusQuestion;
			}
			if(statusWanted == null || statusWanted.trim().isEmpty()) {
				return new MCPToolResult("A status_question is required: the question that holds a "
						+ "case's status. survey_questions lists them.", true);
			}
			statusWanted = statusWanted.trim();

			Survey design = McpData.questions(ctx, surveyId);
			Question status = find(design, statusWanted);
			if(status == null) {
				return new MCPToolResult("\"" + listed.getDisplayName() + "\" has no question "
						+ "called " + statusWanted + ". survey_questions lists the ones it has.",
						true);
			}
			settings.statusQuestion = column(status);

			if(criticalityWanted == null) {
				criticalityWanted = previousSettings.criticalityQuestion;
			}
			if(criticalityWanted != null && !criticalityWanted.trim().isEmpty()) {
				Question criticality = find(design, criticalityWanted.trim());
				if(criticality == null) {
					return new MCPToolResult("\"" + listed.getDisplayName() + "\" has no question "
							+ "called " + criticalityWanted.trim() + ".", true);
				}
				settings.criticalityQuestion = column(criticality);
			}

			if(finalStatus == null) {
				finalStatus = previousSettings.finalStatus;
			}
			if(finalStatus == null || finalStatus.trim().isEmpty()) {
				return new MCPToolResult("A final_status is required: the answer to " + statusWanted
						+ " that means a case is finished. Without one no case is ever closed.",
						true);
			}
			finalStatus = finalStatus.trim();

			/*
			 * If the status question offers choices then the final status has to be one of them.
			 * Anything else is a value the question can never hold, which would leave every case
			 * open for ever while looking entirely configured.
			 */
			if(status.list_name != null && !status.list_name.isEmpty()) {
				Survey withLists = McpData.optionLists(ctx, surveyId);
				OptionList list = withLists.surveyData.optionLists == null
						? null : withLists.surveyData.optionLists.get(status.list_name);
				if(list != null && list.options != null && !list.options.isEmpty()) {
					List<String> values = new ArrayList<>();
					boolean found = false;
					for(Option o : list.options) {
						values.add(o.value);
						if(finalStatus.equals(o.value)) {
							found = true;
						}
					}
					if(!found) {
						return new MCPToolResult(statusWanted + " cannot be answered \"" + finalStatus
								+ "\". Its choices are: " + String.join(", ", values)
								+ ". A final status that is not one of them would leave every case "
								+ "open for ever.", true);
					}
				}
			}
			settings.finalStatus = finalStatus;
		}

		cm.updateSettings(ctx.sd, ctx.user, groupIdent, settings);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("survey", listed.getDisplayName());
		data.put("usesCaseManagement", !turningOff);
		data.put("statusQuestion", settings.statusQuestion);
		data.put("finalStatus", settings.finalStatus);
		data.put("criticalityQuestion", settings.criticalityQuestion);
		data.put("previous", previous);

		StringBuilder text = new StringBuilder();
		if(turningOff) {
			text.append("\"").append(listed.getDisplayName()).append("\" no longer manages cases. "
					+ "Its records are unchanged - nothing was deleted, and what was already closed "
					+ "stays closed.");
		} else {
			text.append("Records in \"").append(listed.getDisplayName()).append("\" are now cases. ")
					.append(settings.statusQuestion).append(" holds the status, and a case is "
							+ "finished when it is set to \"").append(settings.finalStatus)
					.append("\".");
			if(settings.criticalityQuestion != null) {
				text.append(" ").append(settings.criticalityQuestion)
						.append(" holds how urgent it is.");
			}
			text.append("\n\nClosing a case is now setting that question to that value with "
					+ "data_update_record; the server records when. Cases can be given to people "
					+ "with case_assign.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/* By question name, or by column name, since a caller may have either in hand */
	private Question find(Survey design, String wanted) {
		if(design.surveyData.forms == null) {
			return null;
		}
		for(Form f : design.surveyData.forms) {
			if(f.questions == null) {
				continue;
			}
			for(Question q : f.questions) {
				if(wanted.equalsIgnoreCase(q.name)
						|| (q.columnName != null && wanted.equalsIgnoreCase(q.columnName))) {
					return q;
				}
			}
		}
		return null;
	}

	private String column(Question q) {
		return q.columnName != null && !q.columnName.isEmpty() ? q.columnName : q.name;
	}
}
