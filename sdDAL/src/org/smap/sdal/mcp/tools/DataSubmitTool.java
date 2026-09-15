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
		return "Fills in a form, as though somebody had done it: a new record, or - with continues - "
				+ "the next stage of a record that already exists, which is how a case management "
				+ "process is carried forward. Check survey_submission_effects first: a submission "
				+ "can send email and SMS that cannot be recalled. The record appears a moment later.";
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
				+ "typed into the form. Read smap://survey/{ident}/definition for the names. A "
				+ "question given null is left unanswered, and when continues is used it keeps the "
				+ "value the record already has; an empty string takes that answer away.");

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

		properties.put("survey_id", property("integer", "The survey whose form is being filled in"));
		properties.put("answers", answers);
		properties.put("continues", property("string",
				"Optional. The instance id of a record this form is being filled in against, rather "
						+ "than starting a new one. The two forms have to share a record - that is, "
						+ "be in the same bundle - and the answers given here are written onto that "
						+ "record. Anything this form does not ask is left as it was."));
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
		properties.put("instanceid", property("string", "The instance id of this submission"));
		properties.put("continues", property("string",
				"The record carried forward, when one was named"));
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
			} else if(e.getValue() != null) {
				values.put(question, e.getValue().toString());
			}
			/*
			 * A null answer is left out of the document entirely rather than written as empty.
			 *
			 * On a new record the two look the same, but when this form is filled in against an
			 * existing one they are opposites: the record is loaded into the form first, so a question
			 * written as empty takes away what an earlier stage put there, while one left out keeps
			 * it.  An empty string is still passed through, because taking an answer away is a
			 * legitimate thing for a later stage to do.
			 */
		}
		if(!unknown.isEmpty()) {
			return new MCPToolResult("This survey has no question called " + String.join(", ", unknown)
					+ ". Read smap://survey/" + survey.getIdent()
					+ "/definition to see the names.", true);
		}

		/*
		 * The record this form is being filled in against, when it is continuing one.
		 *
		 * Checked here rather than left to the subscriber, which would find no such record and write
		 * a new one - a silent fork, and the hardest kind of mistake to notice because both records
		 * look right on their own.
		 *
		 * Both conditions matter. The record has to exist and be one this caller may see, for the
		 * reason every other record tool checks. And the two forms have to share a record: writing
		 * this form's answers onto a record in a different results table is not a thing that can
		 * happen, and asking for it is a misunderstanding worth naming rather than a failure.
		 */
		String continues = stringArg(arguments, "continues");
		if(continues != null) {
			continues = continues.trim();
			if(continues.isEmpty()) {
				continues = null;
			}
		}
		if(continues != null) {
			if(!McpData.canSeeRecord(ctx, survey, continues)) {
				return new MCPToolResult("There is no record " + continues + " that you can reach "
						+ "through \"" + survey.getDisplayName() + "\". A form can only be filled in "
						+ "against a record it shares - the two have to be in the same bundle. "
						+ "Nothing was submitted.", true);
			}
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
				ctx.request.getServerName(),
				continues);

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("submitted", Boolean.TRUE);
		structured.put("instanceid", submitted.instanceId);
		structured.put("continues", continues == null ? "" : continues);
		structured.put("queued", Boolean.TRUE);

		StringBuilder text = new StringBuilder();
		if(continues == null) {
			text.append("Record accepted, instance id ").append(submitted.instanceId)
					.append(". It is queued and will appear in the data shortly.");
		} else {
			/*
			 * Said as what happened to the record rather than as a new submission, because that is
			 * what it is to whoever asked: the same case, one stage further on.
			 */
			text.append("\"").append(survey.getDisplayName())
					.append("\" filled in against record ").append(continues)
					.append(". It is queued, and the answers appear on that record shortly. The "
							+ "record keeps what the earlier stages put on it: this form is filled "
							+ "in over the record as it stands, so a question you did not answer "
							+ "keeps the value it had.");
		}
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
