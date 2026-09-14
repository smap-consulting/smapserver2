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
import org.smap.sdal.mcp.McpSurveyLayout;
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
 * Either way this is reversible, which is why a plain question does not stop to ask.
 *
 * A group or a repeat is different, and follows the editor's own rules rather than a rule of its own:
 *
 *   A **group** takes everything inside it.  QuestionManager reads a begin group as the group and its
 *   contents, finds the end by counting nesting rather than by name - names go out of step when a
 *   group is renamed - and deletes each member by the same published or unpublished rule.  The end
 *   group itself always goes outright, having never held an answer.
 *
 *   A **repeat** takes its form with it, and that form owns a results table.
 *
 *   A group with **no end** takes nothing.  That is a form in a bad state rather than a group, and
 *   removing the heading on its own is how it is cleaned up - so it is allowed, and does not ask,
 *   because with no end there is nothing identifiable to ask about.
 *
 * So both ask first, and say what is inside before they do.  A group named in a request is a heading
 * to the person asking and a container to the server, and the difference is however many questions
 * happen to lie between the markers.
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
		return "Removes a question. One that has collected answers is marked deleted and its answers "
				+ "kept - adding the same name back to the same form restores them. Deleting a group or a "
				+ "repeat takes everything inside it, so those say what goes and ask first.";
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
						"Optional. The form it is in. Defaults to the main form."),
				/*
				 * Declared as a property, not put on the schema object beside "properties".  A
				 * schema key that is not inside properties is not an argument at all: a strict
				 * client drops it, and the confirmation it is meant to carry can never arrive.
				 */
				"acknowledge", property("object",
						"Only when deleting a group or repeat and your client cannot show an "
								+ "approval prompt. After the person has agreed, call again with "
								+ "{\"question\": \"<the group name>\"}."));
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
		if(found.type != null && found.type.startsWith("end ")) {
			return new MCPToolResult(name + " is the end of a group, not a question. Delete the "
					+ "group itself and its end goes with it.", true);
		}

		/*
		 * A group or a repeat takes things with it, so the caller is told what before being asked.
		 * Read from the layout rather than from the survey model, because what is "inside" a group is
		 * a matter of sequence between two markers and the model does not report sequences.
		 */
		McpSurveyLayout layout = new McpSurveyLayout(ctx.sd, surveyId);
		McpSurveyLayout.Item item = layout.find(name);
		boolean isContainer = item != null && (item.isGroup() || item.isRepeat());
		List<String> contents = new ArrayList<>();
		List<String> withAnswers = new ArrayList<>();

		/*
		 * A group with no end is a form in a bad state, and this is how it is cleaned up.
		 *
		 * There is nothing to ask about: with no end there is no way to tell what is inside, so
		 * QuestionManager removes the begin group on its own and leaves everything after it where it
		 * is.  Only one structural question goes, and it never held an answer.  Refusing would leave
		 * the broken group with no way out of it except the console.
		 */
		boolean unterminated = item != null && item.isGroup() && layout.endOf(item) == null;

		if(isContainer && !unterminated) {
			if(item.isGroup()) {
				for(McpSurveyLayout.Item i : layout.contentsOf(item)) {
					if(i.isEndGroup()) {
						continue;
					}
					contents.add(i.name);
					if(i.published) {
						withAnswers.add(i.name);
					}
				}
			} else {
				for(McpSurveyLayout.Item i : layout.all()) {
					if(i.fId != item.fId && i.formName != null
							&& i.formName.equalsIgnoreCase(item.name)) {
						contents.add(i.name);
						if(i.published) {
							withAnswers.add(i.name);
						}
					}
				}
			}

			if(refused(ctx)) {
				return new MCPToolResult("Left alone. \"" + name + "\" and everything in it are "
						+ "unchanged.", false);
			}
			if(!approved(ctx)) {
				StringBuilder what = new StringBuilder("Delete the ")
						.append(item.isRepeat() ? "repeat" : "group").append(" \"").append(name)
						.append("\"?");
				if(contents.isEmpty()) {
					what.append("\n\nIt is empty, so nothing else goes with it.");
				} else {
					what.append("\n\n").append(contents.size())
							.append(contents.size() == 1 ? " question goes with it: "
									: " questions go with it: ")
							.append(String.join(", ", contents)).append(".");
				}
				if(!withAnswers.isEmpty()) {
					what.append("\n\n").append(String.join(", ", withAnswers))
							.append(withAnswers.size() == 1 ? " already holds answers"
									: " already hold answers")
							.append(", so ").append(withAnswers.size() == 1 ? "it is" : "they are")
							.append(" marked deleted rather than removed and the data stays. The "
									+ "rest go outright.");
				}
				if(item.isRepeat()) {
					what.append("\n\nThe repeat's own table goes with it.");
				}
				if(ctx.canElicit()) {
					return ask(ctx, what.toString());
				}
				Object ackArg = arguments.get("acknowledge");
				String claimed = null;
				if(ackArg instanceof Map) {
					Object v = ((Map<?, ?>) ackArg).get("question");
					claimed = v == null ? null : v.toString().trim();
				}
				if(claimed == null || !claimed.equalsIgnoreCase(name)) {
					return new MCPToolResult(what
							+ "\n\nThis client cannot ask you to approve that mid-request. Put it to "
							+ "the person responsible, and if they agree call again with acknowledge "
							+ "set to {\"question\": \"" + name + "\"}.", true);
				}
			}
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
		/*
		 * The question id, which matters for a repeat and for nothing else.
		 *
		 * Deleting a repeat deletes the form it created, found by "parentquestion = q.id".  Leaving
		 * the id unset made that zero, and a main form's parentquestion IS zero - so deleting one
		 * scratch repeat deleted the main form of the survey, and every question in it was orphaned
		 * against a form row that no longer existed.
		 */
		q.id = found.id;

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
		data.put("alsoDeleted", contents);
		if(unterminated) {
			data.put("unterminatedGroup", Boolean.TRUE);
		}

		StringBuilder text = new StringBuilder();
		text.append("Removed ").append(name).append(" from ").append(form.name)
				.append(" in \"").append(listed.getDisplayName()).append("\".");

		if(unterminated) {
			/*
			 * Said plainly, because the form looks different afterwards and nothing else would
			 * explain why.  The questions were never identifiably inside the group - that is what
			 * having no end means - so they are where they always were, with nothing above them.
			 */
			text.append("\n\nThat group had no end, so only the heading went. The questions that "
					+ "appeared to be inside it are untouched and now sit at the top level of the "
					+ "form. Nothing could be done about them here: with no end there is no way to "
					+ "tell which ones they were.");
			MCPToolResult broken = new MCPToolResult(text.toString());
			broken.setStructuredContent(data);
			return broken;
		}

		if(!contents.isEmpty()) {
			text.append("\n\n").append(contents.size())
					.append(contents.size() == 1 ? " question went with it: "
							: " questions went with it: ")
					.append(String.join(", ", contents)).append(".");
			if(!withAnswers.isEmpty()) {
				text.append(" ").append(String.join(", ", withAnswers))
						.append(withAnswers.size() == 1 ? " had answers and is" : " had answers and are")
						.append(" marked deleted rather than removed, so the data is still there.");
			}
		}

		if(isContainer) {
			MCPToolResult container = new MCPToolResult(text.toString());
			container.setStructuredContent(data);
			return container;
		}

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
