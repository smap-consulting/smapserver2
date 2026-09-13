package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.RecordSubmitManager;
import org.smap.sdal.managers.SubmissionEffectsManager;
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
 * Add a record to a survey.
 *
 * This is the tool that asks before it acts, and the reason the asking exists. The record itself is
 * recoverable - it can be deleted like any other - but a submission is not only a record: it can
 * send email and SMS, call a webhook and create tasks that people go and do. None of that can be
 * recalled, so what is put in front of the person approving is not "may I write a row" but the count
 * of what is about to leave the building.
 *
 * A survey that sends nothing and creates nothing is submitted to without asking. The question is
 * reserved for the case that warrants it, so that it still means something when it appears.
 */
public class DataSubmitTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_submit";
	}

	@Override
	public String getTitle() {
		return "Add a record";
	}

	@Override
	public String getDescription() {
		return "Submits a new record to a survey, as though it had been filled in on a form. "
				+ "Give answers keyed by question name. Check survey_submission_effects first: a "
				+ "submission can send email and SMS and create tasks, and none of that can be "
				+ "recalled, so if this survey sends anything you will be asked to confirm before "
				+ "it goes. The record is queued and appears in the data a moment later, not "
				+ "immediately.";
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
		return "data_delete_record on the new record. Anything the submission sent cannot be "
				+ "recalled, which is why it is confirmed first";
	}

	@Override
	public Confirmation getConfirmation() {
		return Confirmation.CONDITIONAL;
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> answers = new LinkedHashMap<>();
		answers.put("type", "object");
		answers.put("description", "The answers, keyed by question name, as they would have been "
				+ "typed into the form. Read smap://survey/{ident}/definition for the names.");

		Map<String, Object> schema = new LinkedHashMap<>();
		Map<String, Object> properties = new LinkedHashMap<>();
		Map<String, Object> acknowledge = new LinkedHashMap<>();
		acknowledge.put("type", "object");
		acknowledge.put("description", "Only needed when this client cannot ask the user to approve "
				+ "mid-request, and only when the survey sends something. Call "
				+ "survey_submission_effects, show the person what it says, and repeat the numbers "
				+ "here as emails, sms_messages, webhooks and tasks. They must match, so they "
				+ "cannot be guessed, and repeating them puts what is about to be sent in front of "
				+ "whoever approves the call.");

		properties.put("survey_id", property("integer", "The survey to add a record to"));
		properties.put("answers", answers);
		properties.put("acknowledge", acknowledge);
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "survey_id", "answers" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("submitted", property("boolean", "Whether the record was accepted"));
		properties.put("instanceid", property("string", "The new record's instance id"));
		properties.put("queued", property("boolean",
				"Always true: the record is applied by the subscriber a moment later"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	@SuppressWarnings("unchecked")
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		Object answersArg = arguments.get("answers");
		if(surveyId <= 0 || !(answersArg instanceof Map) || ((Map<String, Object>) answersArg).isEmpty()) {
			return new MCPToolResult(
					"A survey_id and some answers are required. Read the survey definition "
					+ "resource to see the question names.", true);
		}
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		/*
		 * Answers are matched to real questions before anything else happens, so a misspelled name
		 * is a refusal rather than a record quietly missing an answer. The names come from the
		 * caller and the values reach an XML document, so what is accepted is only what the survey
		 * actually asks.
		 */
		Map<String, String> values = new LinkedHashMap<>();
		List<String> unknown = new java.util.ArrayList<>();
		Form topForm = GeneralUtilityMethods.getTopLevelForm(ctx.sd, surveyId);
		java.util.ArrayList<TableColumn> columns = McpData.columns(ctx, survey, 0, topForm.id,
				topForm.tableName, false, false, false);

		for(Map.Entry<String, Object> e : ((Map<String, Object>) answersArg).entrySet()) {
			String question = questionFor(columns, e.getKey());
			if(question == null) {
				unknown.add(e.getKey());
			} else {
				values.put(question, e.getValue() == null ? "" : e.getValue().toString());
			}
		}
		if(!unknown.isEmpty()) {
			return new MCPToolResult("This survey has no question called " + String.join(", ", unknown)
					+ ". Read smap://survey/" + survey.getIdent()
					+ "/definition to see the names.", true);
		}

		/*
		 * What this submission would set off. Worked out before anything is written, because it is
		 * the thing being approved: the row is recoverable and the messages are not.
		 */
		SubmissionEffectsManager sem = new SubmissionEffectsManager(ctx.localisation);
		SubmissionEffectsManager.Effects effects = sem.predict(ctx.sd, surveyId, survey.getIdent());

		if(!effects.isEmpty()) {
			if(refused(ctx)) {
				return new MCPToolResult("Not submitted. Nothing was sent.", false);
			}
			if(!approved(ctx)) {
				/*
				 * Ask properly where the client can carry a question.
				 */
				if(ctx.canElicit()) {
					return ask(ctx, "Add a record to \"" + survey.getDisplayName() + "\"?\n\n"
							+ effects.describe()
							+ "\n\nThe record itself can be deleted afterwards. Anything sent "
							+ "cannot be taken back.");
				}

				/*
				 * Where it cannot, the caller has to say what it is about to set off.
				 *
				 * Asking mid-request needs the 2026-07-28 protocol, and almost nothing speaks it
				 * yet: the clients in the field still open with the handshake that revision removed.
				 * Refusing outright would mean no survey that sends anything could ever be submitted
				 * to, which is most of the ones worth submitting to.
				 *
				 * So the caller repeats the counts instead. They are checked against what this
				 * server just worked out, so they cannot be guessed or carried over from a survey
				 * that has since changed, and restating them is what puts the consequence into the
				 * conversation where the person approving the call can read it. Weaker than being
				 * asked, and honest about being the substitute rather than the thing.
				 */
				Map<String, Object> ack = arguments.get("acknowledge") instanceof Map
						? (Map<String, Object>) arguments.get("acknowledge")
						: null;

				if(ack == null || !effects.matches(count(ack, "emails"), count(ack, "sms_messages"),
						count(ack, "webhooks"), count(ack, "tasks"))) {
					return new MCPToolResult(
							"Not submitted. " + effects.describe()
							+ "\n\nThis client cannot ask you to approve that mid-request. Show "
							+ "the person these numbers, and if they agree call again with "
							+ "acknowledge set to {\"emails\": " + effects.emails
							+ ", \"sms_messages\": " + effects.smsMessages
							+ ", \"webhooks\": " + effects.webhooks
							+ ", \"tasks\": " + effects.tasks + "}.", true);
				}
			}
		}

		RecordSubmitManager rsm = new RecordSubmitManager(ctx.localisation, ctx.timezone);
		RecordSubmitManager.Submitted submitted = rsm.submit(
				ctx.sd,
				ctx.cResults,
				survey,
				values,
				ctx.user,
				ctx.clientId,
				GeneralUtilityMethods.getBasePath(ctx.request),
				ctx.request.getServerName());

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("submitted", Boolean.TRUE);
		structured.put("instanceid", submitted.instanceId);
		structured.put("queued", Boolean.TRUE);

		StringBuilder text = new StringBuilder("Record accepted, instance id ")
				.append(submitted.instanceId)
				.append(". It is queued and will appear in the data shortly.");
		if(!effects.isEmpty()) {
			text.append(" ").append(effects.describe());
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(structured);
		return result;
	}

	/* A number from the acknowledgement, however the client spelled it */
	private static Integer count(Map<String, Object> ack, String name) {
		Object v = ack.get(name);
		if(v instanceof Number) {
			return ((Number) v).intValue();
		}
		try {
			return v == null ? null : Integer.valueOf(v.toString().trim());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/*
	 * The question a caller's name refers to.
	 *
	 * Matched against the survey's own questions and answered with the question name rather than the
	 * column name, because what is being built is a form instance and the form knows its questions
	 * by the names the designer gave them.
	 */
	private String questionFor(java.util.ArrayList<TableColumn> columns, String name) {
		if(name == null) {
			return null;
		}
		String wanted = name.trim();
		for(TableColumn c : columns) {
			if(wanted.equalsIgnoreCase(c.question_name) || wanted.equalsIgnoreCase(c.column_name)
					|| wanted.equalsIgnoreCase(McpRead.jsonKey(c))) {
				return c.question_name != null ? c.question_name : c.column_name;
			}
		}
		return null;
	}
}
