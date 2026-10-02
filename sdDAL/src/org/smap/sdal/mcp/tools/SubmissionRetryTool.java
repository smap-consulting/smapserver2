package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SubmissionEffectsManager;
import org.smap.sdal.managers.UploadEventManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Apply failed submissions again: the "Re-apply failed uploads" button on the monitor page.
 *
 * The usual reason is that the survey or its table has been fixed since - a column changed type, a
 * question added - and the submissions that failed against the old shape will now go in.  Nothing is
 * applied here: the uploads are put back in the subscriber's queue, and submission_events shows
 * how each one went a moment later.
 *
 * A submission that applies sets off whatever the survey sets off, exactly as if it had just
 * arrived, so this asks first in the same way data_submit does.  Every retried record has the
 * effects of one submission.
 *
 * The survey comes from the caller's own list, so a survey they cannot reach is not found.
 */
public class SubmissionRetryTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "submission_retry";
	}

	@Override
	public String getTitle() {
		return "Re-apply failed submissions";
	}

	@Override
	public String getDescription() {
		return "Queues a survey's failed submissions to be applied again, once whatever made them "
				+ "fail has been fixed. Duplicates are not retried. Check submission_events "
				+ "afterwards; a retried submission sends whatever a new one would.";
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
		return "data_delete_record on each record that was applied. Anything the submissions sent "
				+ "cannot be recalled, which is why it is confirmed first";
	}

	@Override
	public Confirmation getConfirmation() {
		return Confirmation.CONDITIONAL;
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> ids = new LinkedHashMap<>();
		ids.put("type", "array");
		ids.put("description", "Optional. Only these uploads, by upload_id from submission_events. "
				+ "Omit for every failed upload of the survey.");
		Map<String, Object> items = new LinkedHashMap<>();
		items.put("type", "integer");
		ids.put("items", items);

		Map<String, Object> acknowledge = new LinkedHashMap<>();
		acknowledge.put("type", "object");
		acknowledge.put("description", "Only needed when this client cannot ask the user to approve "
				+ "mid-request, and only when the survey sends something. Repeat the numbers "
				+ "survey_submission_effects gives as emails, sms_messages, webhooks and tasks.");

		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey whose failed submissions to apply again"),
				"upload_ids", ids,
				"acknowledge", acknowledge);
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	@SuppressWarnings("unchecked")
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required.", true);
		}
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		List<Integer> ueIds = null;
		Object idsArg = arguments.get("upload_ids");
		if(idsArg instanceof List) {
			ueIds = new ArrayList<>();
			for(Object o : (List<Object>) idsArg) {
				if(!(o instanceof Number)) {
					return new MCPToolResult("upload_ids holds numbers, from submission_events.", true);
				}
				ueIds.add(((Number) o).intValue());
			}
		} else if(idsArg != null) {
			return new MCPToolResult("upload_ids is a list, such as [453, 454].", true);
		}

		/*
		 * What applying them would set off, worked out before anything changes, because that is
		 * what is being approved
		 */
		SubmissionEffectsManager sem = new SubmissionEffectsManager(ctx.localisation);
		SubmissionEffectsManager.Effects effects = sem.predict(ctx.sd, surveyId, survey.getIdent());

		if(!effects.isEmpty()) {
			if(refused(ctx)) {
				return new MCPToolResult("Not retried. Nothing was sent.", false);
			}
			if(!approved(ctx)) {
				if(ctx.canElicit()) {
					return ask(ctx, "Apply the failed submissions to \"" + survey.getDisplayName()
							+ "\" again?\n\n" + effects.describe() + " That is for each record retried."
							+ "\n\nThe records can be deleted afterwards. Anything sent cannot be "
							+ "taken back.");
				}

				/* The same substitute for being asked that data_submit uses, and as weak */
				Map<String, Object> ack = arguments.get("acknowledge") instanceof Map
						? (Map<String, Object>) arguments.get("acknowledge")
						: null;
				if(ack == null || !effects.matches(count(ack, "emails"), count(ack, "sms_messages"),
						count(ack, "webhooks"), count(ack, "tasks"))) {
					return new MCPToolResult(
							"Not retried. " + effects.describe() + " That is for each record retried."
							+ "\n\nThis client cannot ask you to approve that mid-request. Show "
							+ "the person these numbers, and if they agree call again with "
							+ "acknowledge set to {\"emails\": " + effects.emails
							+ ", \"sms_messages\": " + effects.smsMessages
							+ ", \"webhooks\": " + effects.webhooks
							+ ", \"tasks\": " + effects.tasks + "}.", true);
				}
			}
		}

		List<Integer> queued = UploadEventManager.retryFailed(ctx.sd, survey.getIdent(), ueIds);

		StringBuilder text = new StringBuilder();
		if(queued.isEmpty()) {
			text.append(ueIds == null
					? "\"" + survey.getDisplayName() + "\" has no failed submissions to retry."
					: "None of those uploads is a failed submission of \"" + survey.getDisplayName()
							+ "\". Duplicates are not retried.");
		} else {
			text.append(queued.size()).append(" failed submission(s) to \"")
					.append(survey.getDisplayName()).append("\" queued to be applied again: ")
					.append(queued.toString().replaceAll("[\\[\\]]", ""))
					.append(". submission_events shows how each one went, a moment from now.");
			if(!effects.isEmpty()) {
				text.append(" ").append(effects.describe())
						.append(" That is for each record retried.");
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("survey_id", surveyId);
		data.put("queued", queued);
		data.put("count", queued.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
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
}
