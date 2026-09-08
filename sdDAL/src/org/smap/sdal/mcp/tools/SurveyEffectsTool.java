package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SubmissionEffectsManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * What a survey will set off when a record is added to it.
 *
 * Read only, and useful on its own: "what happens if I submit to this survey" is a fair question to
 * ask before touching anything.  It is also the number that has to appear in front of somebody
 * approving a submission, so it exists as a tool rather than only as something the write path
 * consults, and can be checked long before there is a write path to check it from.
 */
public class SurveyEffectsTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_submission_effects";
	}

	@Override
	public String getTitle() {
		return "What a submission will set off";
	}

	@Override
	public String getDescription() {
		return "Reports what adding a record to a survey would send and create: how many emails, "
				+ "SMS messages, webhook calls and tasks, and which notifications and task groups "
				+ "are responsible. The numbers are the most that can happen, because each "
				+ "notification and task rule is also filtered on the record itself. Ask this "
				+ "before submitting data on someone's behalf, and show the answer to them: "
				+ "messages that go out cannot be recalled.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to report on, from survey_list"));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("emails", property("integer", "Email recipients, at most"));
		properties.put("sms_messages", property("integer", "SMS messages, at most"));
		properties.put("webhooks", property("integer", "Webhook calls, at most"));
		properties.put("tasks", property("integer", "Tasks created, at most"));
		properties.put("notifications", property("array", "The notifications responsible"));
		properties.put("task_groups", property("array", "The task groups responsible"));
		properties.put("upper_bound", property("boolean",
				"Always true: record level filters may reduce these numbers"));

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

		SubmissionEffectsManager sem = new SubmissionEffectsManager(ctx.localisation);
		SubmissionEffectsManager.Effects e = sem.predict(ctx.sd, surveyId, survey.getIdent());

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("emails", e.emails);
		structured.put("sms_messages", e.smsMessages);
		structured.put("webhooks", e.webhooks);
		structured.put("tasks", e.tasks);
		structured.put("notifications", e.notifications);
		structured.put("task_groups", e.taskGroups);
		structured.put("upper_bound", Boolean.TRUE);

		MCPToolResult result = new MCPToolResult(e.describe());
		result.setStructuredContent(structured);
		return result;
	}
}
