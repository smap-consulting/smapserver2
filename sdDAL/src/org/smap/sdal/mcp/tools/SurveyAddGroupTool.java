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
import org.smap.sdal.mcp.McpSurveyLayout;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.ChangeSet;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Add a group or a repeat to a survey.
 *
 * The two look alike on a screen and are not alike underneath, which is the whole reason this needs
 * saying rather than being left to the caller:
 *
 *   A **group** keeps questions together on one page and can be shown or hidden as a unit.  It is a
 *   begin group question, an end group question, and whatever lies between them in sequence.  Its
 *   answers are stored in the same table as the rest of the form.
 *
 *   A **repeat** is asked over and over - one set of answers per accused, per visit, per item.  It is
 *   a form of its own with its own table, so its answers are separate rows joined back to the parent.
 *
 * Choosing wrongly is expensive later: turning a group into a repeat after data has arrived means
 * moving answers between tables, which is not something to discover halfway through.
 *
 * The end group is created here rather than left to the caller.  A group with no end is a real state
 * that forms get into, and everything that reads one - the editor, this server, the delete rules -
 * then has to guess where the group stops.  Making both halves in one act is the only way it cannot
 * happen.
 */
public class SurveyAddGroupTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_add_group";
	}

	@Override
	public String getTitle() {
		return "Add a group or repeat";
	}

	@Override
	public String getDescription() {
		return "Adds a group, or a repeat, to a survey. A group keeps questions together and can be "
				+ "shown or hidden as one; a repeat is asked once per accused, per visit, per item, "
				+ "and its answers go in a table of their own. Put questions in it with "
				+ "survey_add_question naming the group, or move existing ones with "
				+ "survey_move_question.";
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
		return "survey_delete_question on the group, while it is still empty";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ANALYST);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to change, from survey_list"),
				"name", property("string",
						"The name, as used in expressions. Letters, digits and underscores."),
				"label", property("string", "The heading shown above it"),
				"repeating", property("boolean",
						"true for a repeat - asked once per accused, per visit, per item, with its "
								+ "answers in their own table. false, the default, for a group."),
				"form", property("string",
						"Optional. The form to add it to, for nesting inside an existing repeat. "
								+ "Defaults to the main form."),
				"after", property("string",
						"Optional. Put it immediately after this question. Defaults to the end."),
				"relevant", property("string",
						"Optional. An expression deciding when the whole group is asked."),
				"appearance", property("string", "Optional. How it is presented."));
		schema.put("required", new String[] { "survey_id", "name", "label" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}

		String name = stringArg(arguments, "name");
		if(name == null || !name.trim().matches("[A-Za-z_][A-Za-z0-9_]*")) {
			return new MCPToolResult("A name is required: letters, digits and underscores, not "
					+ "starting with a digit.", true);
		}
		name = name.trim();

		String label = stringArg(arguments, "label");
		if(label == null || label.trim().isEmpty()) {
			return new MCPToolResult("A label is required - the heading shown above it.", true);
		}
		label = label.trim();

		boolean repeating = boolArg(arguments, "repeating", false);

		Survey s = McpData.outline(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No survey with id " + surveyId + " that you can reach.", true);
		}

		Form form = null;
		String formName = stringArg(arguments, "form");
		for(Form f : s.surveyData.forms) {
			if(formName == null ? f.parentform == 0 : formName.equalsIgnoreCase(f.name)) {
				form = f;
				break;
			}
		}
		if(form == null) {
			return new MCPToolResult("This survey has no form called \"" + formName + "\". "
					+ "survey_get lists them.", true);
		}

		McpSurveyLayout layout = new McpSurveyLayout(ctx.sd, surveyId);
		if(layout.find(name) != null) {
			return new MCPToolResult("This survey already has a question called \"" + name + "\". "
					+ "Names have to be unique across the whole survey.", true);
		}
		String endName = name + "_groupEnd";
		if(!repeating && layout.find(endName) != null) {
			return new MCPToolResult("This survey already has a question called \"" + endName
					+ "\", which is the name the end of this group would take.", true);
		}

		/*
		 * Where it goes.  After a named question, or at the end - and "after" is resolved against the
		 * form the caller asked for, so putting something after a question in a different form is
		 * refused rather than quietly landing somewhere else.
		 */
		int seq;
		String afterName = stringArg(arguments, "after");
		if(afterName != null && !afterName.trim().isEmpty()) {
			McpSurveyLayout.Item afterItem = layout.find(afterName.trim());
			if(afterItem == null) {
				return new MCPToolResult("This survey has no question called \"" + afterName
						+ "\".", true);
			}
			if(afterItem.fId != form.id) {
				return new MCPToolResult("\"" + afterName + "\" is in " + afterItem.formName
						+ ", not " + form.name + ". A group has to sit in one form.", true);
			}
			/*
			 * After a group means after the whole group, not between its begin and its end - which
			 * would nest this one inside it without the caller asking for that.
			 */
			McpSurveyLayout.Item end = layout.endOf(afterItem);
			seq = (end == null ? afterItem.seq : end.seq) + 1;
		} else {
			seq = layout.lastSeq(form.id) + 1;
		}

		int languages = s.surveyData.languages == null || s.surveyData.languages.isEmpty()
				? 1 : s.surveyData.languages.size();

		ArrayList<ChangeSet> changes = new ArrayList<>();
		changes.add(one(question(form.id, name, repeating ? "begin repeat" : "begin group", seq,
				label, languages, stringArg(arguments, "relevant"),
				stringArg(arguments, "appearance"))));

		/*
		 * The end, immediately after, so the group starts life empty and closed.  A repeat has none:
		 * its contents live in the form it created, not between two markers.
		 */
		if(!repeating) {
			changes.add(one(question(form.id, endName, "end group", seq + 1,
					label, languages, null, null)));
		}

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.applyChangeSetArray(ctx.sd, ctx.cResults, surveyId, ctx.user, changes, true);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("added", Boolean.TRUE);
		data.put("survey", s.surveyData.displayName);
		data.put("name", name);
		data.put("type", repeating ? "repeat" : "group");
		data.put("form", form.name);
		data.put("position", seq);
		if(!repeating) {
			data.put("endsWith", endName);
		}

		StringBuilder text = new StringBuilder();
		text.append("Added the ").append(repeating ? "repeat" : "group").append(" \"").append(name)
				.append("\" to ").append(form.name).append(" in \"")
				.append(s.surveyData.displayName).append("\".");
		text.append("\n\nIt is empty. Put questions in it with survey_add_question naming ")
				.append(repeating ? "\"" + name + "\" as the form" : "it in after")
				.append(", or move existing ones with survey_move_question.");
		if(repeating) {
			text.append("\n\nBeing a repeat, its answers go in a table of their own, one row each "
					+ "time it is filled in. data_query reads them by naming it as the form.");
		} else {
			text.append("\n\nIt is closed by ").append(endName)
					.append(", which was created with it - a group with no end leaves everything "
							+ "that reads the form guessing where it stops.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	private ChangeSet one(Question q) {
		ChangeItem ci = new ChangeItem();
		ci.question = q;
		ci.source = "mcp";

		ChangeSet cs = new ChangeSet();
		cs.changeType = "question";
		cs.type = "question";
		cs.action = "add";
		cs.source = "mcp";
		cs.items = new ArrayList<>();
		cs.items.add(ci);
		return cs;
	}

	private Question question(int fId, String name, String type, int seq, String label,
			int languages, String relevant, String appearance) {

		Question q = new Question();
		q.fId = fId;
		q.name = name;
		q.columnName = name;
		q.type = type;
		q.seq = seq;
		q.source = null;			// Structural questions carry no source, as QuestionManager expects
		q.visible = true;
		q.published = false;
		q.soft_deleted = false;
		q.required = false;
		q.relevant = relevant;
		q.appearance = appearance;
		q.paramArray = new ArrayList<>();

		q.labels = new ArrayList<>();
		for(int i = 0; i < languages; i++) {
			Label l = new Label();
			l.text = label;
			q.labels.add(l);
		}
		return q;
	}
}
