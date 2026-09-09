package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.ChangeSet;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Remove a question from a survey.
 *
 * Two different things happen underneath, and which one depends on whether the question has been
 * published:
 *
 *   published   - soft deleted.  The row stays, the results column stays, and the answers already
 *                 given stay with it.  Adding a question of the same name back to the same form
 *                 reuses that column, so the data comes back with it.
 *   unpublished - deleted outright, along with its labels and hints.  Nothing is lost that was not
 *                 already only a definition, because an unpublished question has never held an
 *                 answer.
 *
 * Either way this is reversible, which is why it does not stop to ask.  What it will not do is
 * delete a group: QuestionManager takes a begin group to mean the group and everything inside it,
 * so one call would remove questions the caller never named.
 */
public class SurveyDeleteQuestionTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_delete_question";
	}

	@Override
	public String getTitle() {
		return "Delete a question";
	}

	@Override
	public String getDescription() {
		return "Removes a question from a survey. A question that has already collected answers is "
				+ "marked deleted rather than removed, and its answers are kept: adding a question "
				+ "of the same name back to the same form brings them back. A question that never "
				+ "collected anything is removed outright. Groups and repeats cannot be deleted "
				+ "here, because deleting a group deletes everything inside it.";
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
		return "survey_add_question puts it back, with its answers if it had any";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to remove the question from"),
				"name", property("string", "The name of the question to remove"),
				"form", property("string",
						"Optional. The form it is in. Defaults to the main form."));
		schema.put("required", new String[] { "survey_id", "name" });
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
		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A question name is required.", true);
		}
		name = name.trim();

		Survey s = McpData.questions(ctx, surveyId);
		String formWanted = stringArg(arguments, "form");
		Form form = null;
		for(Form f : s.surveyData.forms) {
			if(formWanted == null ? f.parentform == 0 : formWanted.equalsIgnoreCase(f.name)) {
				form = f;
				break;
			}
		}
		if(form == null) {
			return new MCPToolResult(formWanted == null
					? "This survey has no main form, which should not happen."
					: "This survey has no form called \"" + formWanted + "\". survey_get lists "
							+ "the ones it has.", true);
		}

		Question found = null;
		if(form.questions != null) {
			for(Question q : form.questions) {
				if(name.equalsIgnoreCase(q.name)) {
					found = q;
					break;
				}
			}
		}
		if(found == null) {
			return new MCPToolResult("\"" + form.name + "\" has no question called " + name
					+ ". survey_questions lists the ones it has.", true);
		}
		if(found.type != null && (found.type.startsWith("begin ") || found.type.startsWith("end "))) {
			return new MCPToolResult(name + " is a " + found.type + ", and deleting it would delete "
					+ "every question inside it as well. Delete those individually, or do it in the "
					+ "console where you can see what goes.", true);
		}

		/*
		 * Whether the answers survive is decided by publication, and the caller is told which
		 * happened rather than left to infer it from a word like deleted that covers both.
		 */
		boolean published = found.published;

		Question q = new Question();
		q.fId = form.id;
		q.name = found.name;
		q.type = found.type;
		q.published = published;

		ChangeItem ci = new ChangeItem();
		ci.question = q;
		/*
		 * On the item, not only on the change set. ChangeElement, which is what gets written to the
		 * change log, reads source from the item; setting it on the set alone is dropped silently
		 * and the change records itself as having come from the editor.
		 */
		ci.source = "mcp";

		ChangeSet cs = new ChangeSet();
		cs.changeType = "question";
		cs.type = "question";
		cs.action = "delete";
		cs.source = "mcp";
		cs.items = new ArrayList<>();
		cs.items.add(ci);

		ArrayList<ChangeSet> changes = new ArrayList<>();
		changes.add(cs);

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.applyChangeSetArray(ctx.sd, ctx.cResults, surveyId, ctx.user, changes, true);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("survey", listed.getDisplayName());
		data.put("form", form.name);
		data.put("name", name);
		data.put("answersKept", published);

		StringBuilder text = new StringBuilder();
		text.append("Removed ").append(name).append(" from ").append(form.name)
				.append(" in \"").append(listed.getDisplayName()).append("\".");
		if(published) {
			text.append("\n\nIt had collected answers, so it is marked deleted rather than removed "
					+ "and they are still there. Adding a question called ").append(name)
					.append(" back to ").append(form.name).append(" brings them back with it.");
		} else {
			text.append("\n\nIt had never collected an answer, so it is gone from the design "
					+ "entirely. survey_add_question can recreate it.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
