package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.QuestionManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.ChangeSet;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Add a question to a survey.
 *
 * A thin wrapper over the change set the online editor sends: the work is done by
 * SurveyManager.applyChangeSetArray and QuestionManager.save, so a question added here is the same
 * question the editor would have added, sanitised the same way and logged the same way.
 *
 * What this class does that the editor's own endpoint does not is refuse before it starts.  The
 * editor has a person looking at the form who can see that a name is taken or a choice list does not
 * exist; a model cannot, so the checks are made here and answered in words rather than surfaced as a
 * constraint violation from the database.
 */
public class SurveyAddQuestionTool extends AbstractMcpTool {

	/*
	 * The types a question can be given here.  Deliberately not every type Smap supports: groups and
	 * repeats change the shape of the form and the tables under it, and adding one from a tool that
	 * cannot also place its end is a way to leave a form that does not open.
	 */
	private static final List<String> TYPES = Arrays.asList(
			"string", "int", "decimal", "date", "dateTime", "time",
			"select1", "select", "note", "calculate",
			"geopoint", "geotrace", "geoshape",
			"image", "audio", "video", "barcode", "acknowledge");

	@Override
	public String getName() {
		return "survey_add_question";
	}

	@Override
	public String getTitle() {
		return "Add a question";
	}

	@Override
	public String getDescription() {
		return "Adds a question to a survey, at the end of a form unless a position is given. The "
				+ "question appears in the design at once; if the survey already has data, the "
				+ "column that will hold the answers is added separately a moment later, which is "
				+ "what resultsStatus reports. Groups and repeats cannot be added here. Undo it "
				+ "with survey_delete_question.";
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
		return "survey_delete_question removes it again";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to add the question to"),
				"name", property("string",
						"The question name, as used in expressions such as ${name}. Letters, "
						+ "digits and underscores, not starting with a digit."),
				"type", property("string", "One of " + String.join(", ", TYPES)),
				"label", property("string", "The question as it is put to the person answering it"),
				"form", property("string",
						"Optional. The form to add it to. Defaults to the main form. survey_get "
						+ "lists the names."),
				"hint", property("string", "Optional. Guidance shown under the label."),
				"required", property("boolean", "Optional. Whether an answer must be given."),
				"relevant", property("string",
						"Optional. An expression deciding when the question is asked, such as "
						+ "${age} > 18"),
				"constraint", property("string",
						"Optional. An expression the answer must satisfy, such as . < 100"),
				"appearance", property("string", "Optional. How the question is presented."),
				"calculation", property("string",
						"Optional. For a calculate question, the expression to evaluate."),
				"default_answer", property("string", "Optional. The answer to start with."),
				"option_list", property("string",
						"Required for select and select1: the name of an existing choice list. "
						+ "survey_options lists them."),
				"position", property("integer",
						"Optional. Where in the form to put it, counting from 0. Defaults to the "
						+ "end, which is the only position that does not renumber other questions."));
		schema.put("required", new String[] { "survey_id", "name", "type", "label" });
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
		String type = stringArg(arguments, "type");
		String label = stringArg(arguments, "label");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required.", true);
		}
		name = name.trim();
		/*
		 * The name reaches XPath expressions as ${name}, so it has to be one XPath will accept.
		 * Refused here rather than left to fail later, when it would fail on a device rather than
		 * in this conversation.
		 */
		if(!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			return new MCPToolResult("\"" + name + "\" cannot be a question name. Names start with "
					+ "a letter or underscore and hold only letters, digits and underscores.", true);
		}
		if(type == null || !TYPES.contains(type)) {
			return new MCPToolResult("\"" + type + "\" is not a question type this tool can add. "
					+ "Use one of: " + String.join(", ", TYPES) + ". Groups and repeats have to be "
					+ "added in the console, because they change the shape of the form.", true);
		}
		if(label == null || label.trim().isEmpty()) {
			return new MCPToolResult("A label is required. It is what the question asks.", true);
		}

		/* The design, which answers the form, the name clash and the choice list in one read */
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

		/*
		 * A name already in use in this form violates the unique index and would surface as a
		 * database error.  A name belonging to a question that was deleted is allowed, and is worth
		 * saying out loud: save() reuses the old column, so the answers given before it was deleted
		 * come back with it.
		 */
		if(form.questions != null) {
			for(Question q : form.questions) {
				if(name.equalsIgnoreCase(q.name)) {
					return new MCPToolResult("\"" + form.name + "\" already has a question called "
							+ name + ". Question names have to be unique within a form.", true);
				}
			}
		}

		String optionList = stringArg(arguments, "option_list");
		boolean isSelect = type.equals("select") || type.equals("select1");
		if(isSelect) {
			if(optionList == null || optionList.trim().isEmpty()) {
				return new MCPToolResult("A " + type + " question needs option_list, the name of "
						+ "the choice list it offers. survey_options lists them.", true);
			}
			Survey withLists = McpData.optionLists(ctx, surveyId);
			if(withLists.surveyData.optionLists == null
					|| !withLists.surveyData.optionLists.containsKey(optionList)) {
				return new MCPToolResult("This survey has no choice list called \"" + optionList
						+ "\". survey_options lists the ones it has, and a new list has to be made "
						+ "in the console.", true);
			}
		} else if(optionList != null) {
			return new MCPToolResult("Only select and select1 questions take an option_list.", true);
		}

		QuestionManager qm = new QuestionManager(ctx.localisation);
		int seq = intArg(arguments, "position", -1);
		if(seq < 0) {
			seq = qm.getNextSeq(ctx.sd, form.id);
		}

		Question q = new Question();
		q.fId = form.id;
		q.name = name;
		q.columnName = name;
		q.type = type;
		q.seq = seq;
		q.source = "user";
		q.visible = true;
		q.published = false;
		q.soft_deleted = false;
		q.required = boolArg(arguments, "required", false);
		q.relevant = stringArg(arguments, "relevant");
		q.constraint = stringArg(arguments, "constraint");
		q.appearance = stringArg(arguments, "appearance");
		q.calculation = stringArg(arguments, "calculation");
		q.defaultanswer = stringArg(arguments, "default_answer");
		q.list_name = optionList;
		q.paramArray = new ArrayList<>();

		/*
		 * One label per language, in the survey's own order, because that is how they are stored and
		 * read back.  The same text in each: a tool that put the English in every language would be
		 * guessing, and one that filled only the first would leave the others blank, which reads on
		 * a device as a question with no words.
		 */
		q.labels = new ArrayList<>();
		int languages = s.surveyData.languages == null || s.surveyData.languages.isEmpty()
				? 1 : s.surveyData.languages.size();
		String hint = stringArg(arguments, "hint");
		for(int i = 0; i < languages; i++) {
			Label l = new Label();
			l.text = label;
			l.hint = hint;
			q.labels.add(l);
		}

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
		cs.action = "add";
		cs.source = "mcp";
		cs.items = new ArrayList<>();
		cs.items.add(ci);

		ArrayList<ChangeSet> changes = new ArrayList<>();
		changes.add(cs);

		/* The agent, so the change log says which application added the question */
		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.applyChangeSetArray(ctx.sd, ctx.cResults, surveyId, ctx.user, changes, true);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("added", Boolean.TRUE);
		data.put("survey", listed.getDisplayName());
		data.put("form", form.name);
		data.put("name", name);
		data.put("type", type);
		data.put("position", seq);

		StringBuilder text = new StringBuilder();
		text.append("Added ").append(name).append(" (").append(type).append(") to ")
				.append(form.name).append(" in \"").append(listed.getDisplayName()).append("\".");
		text.append("\n\nIt is in the design now. If this survey already holds data, the column for "
				+ "the answers is added separately - survey_history shows that as pending until it "
				+ "has been. Remove it again with survey_delete_question.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
