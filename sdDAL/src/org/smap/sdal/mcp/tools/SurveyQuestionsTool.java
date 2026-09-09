package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * The questions in a survey, with the logic attached to each.
 *
 * Labels are held per language, in the same order as the survey's language list, so a label is
 * reached by the language's position rather than by name.  The default language is used unless the
 * caller names another, because a caller asking about a form generally wants it in the language it
 * was written in.
 *
 * Soft deleted questions are not returned.  A published question that has been deleted still owns
 * its results column, so it is a real thing a survey designer may need to see - but showing it
 * beside live questions without that distinction being unmistakable would be worse than leaving it
 * out, and this tool is for reading a form as it stands.
 */
public class SurveyQuestionsTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_questions";
	}

	@Override
	public String getTitle() {
		return "Survey questions";
	}

	@Override
	public String getDescription() {
		return "The questions in a survey: name, type, label, and the logic on each one - whether "
				+ "it is required, what makes it relevant, its constraint, appearance and any "
				+ "calculation. Use survey_get first if you only need the survey's shape. Questions "
				+ "that have been deleted are not returned. For the choices on a select question, "
				+ "use survey_options with the list name reported here.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to read, from survey_list"),
				"form", property("string",
						"Optional. Only the questions in this form, which is the main form or a "
						+ "repeating group. Omit for every form. survey_get lists the names."),
				"language", property("string",
						"Optional. The language to return labels in. Defaults to the survey's own "
						+ "default language."));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey s = McpData.definition(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String formWanted = stringArg(arguments, "form");
		String languageWanted = stringArg(arguments, "language");

		int langIndex = languageIndex(s, languageWanted);
		if(langIndex < 0) {
			return new MCPToolResult("This survey has no language called \"" + languageWanted
					+ "\". survey_get lists the ones it has.", true);
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();
		boolean formFound = false;

		if(s.surveyData.forms != null) {
			for(Form f : s.surveyData.forms) {
				if(formWanted != null && !formWanted.equalsIgnoreCase(f.name)) {
					continue;
				}
				formFound = true;
				text.append(text.length() == 0 ? "" : "\n").append("Form ").append(f.name)
						.append(f.parentform > 0 ? " (repeating group)" : "").append(":");

				if(f.questions != null) {
					for(Question q : f.questions) {
						if(q.soft_deleted || q.propertyType) {
							continue;
						}
						Map<String, Object> row = new LinkedHashMap<>();
						row.put("form", f.name);
						row.put("name", q.name);
						row.put("type", q.type);
						row.put("label", labelText(q, langIndex));
						String hint = hintText(q, langIndex);
						if(hint != null && hint.length() > 0) {
							row.put("hint", hint);
						}
						putIfSet(row, "relevant", q.relevant);
						putIfSet(row, "constraint", q.constraint);
						putIfSet(row, "calculation", q.calculation);
						putIfSet(row, "appearance", q.appearance);
						putIfSet(row, "defaultAnswer", q.defaultanswer);
						putIfSet(row, "choiceFilter", q.choice_filter);
						/*
						 * required has a legacy boolean and an expression that supersedes it. Both
						 * are reported rather than merged: an expression that happens to read false
						 * is not the same fact as a question nobody made required.
						 */
						row.put("required", q.required);
						putIfSet(row, "requiredExpression", q.required_expression);
						row.put("readonly", q.readonly);
						if(q.list_name != null && q.list_name.length() > 0) {
							row.put("optionList", q.list_name);
						}
						row.put("published", q.published);
						rows.add(row);

						text.append("\n- ").append(q.name).append(" (").append(q.type).append(")");
						String label = labelText(q, langIndex);
						if(label != null && label.length() > 0) {
							text.append(" ").append(label.length() > 60
									? label.substring(0, 60) + "..." : label);
						}
						if(q.list_name != null && q.list_name.length() > 0) {
							text.append(" [list: ").append(q.list_name).append("]");
						}
					}
				}
			}
		}

		if(formWanted != null && !formFound) {
			return new MCPToolResult("This survey has no form called \"" + formWanted
					+ "\". survey_get lists the ones it has.", true);
		}
		if(rows.isEmpty()) {
			text.setLength(0);
			text.append("No questions found.");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("questions", rows);
		data.put("count", rows.size());
		data.put("language", s.surveyData.languages != null && langIndex < s.surveyData.languages.size()
				? s.surveyData.languages.get(langIndex).name : s.surveyData.def_lang);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	private static void putIfSet(Map<String, Object> row, String key, String value) {
		if(value != null && value.trim().length() > 0) {
			row.put(key, value);
		}
	}

	/*
	 * Which position in the label lists to read.  Zero when the survey names no languages at all,
	 * which is how a survey with a single unnamed language is stored.
	 */
	private static int languageIndex(Survey s, String wanted) {

		List<Language> languages = s.surveyData.languages;
		if(languages == null || languages.isEmpty()) {
			return wanted == null ? 0 : -1;
		}
		String target = wanted != null ? wanted : s.surveyData.def_lang;
		if(target != null) {
			for(int i = 0; i < languages.size(); i++) {
				if(target.equalsIgnoreCase(languages.get(i).name)) {
					return i;
				}
			}
		}
		/* A named language that does not exist is an error; falling back would answer a question
		 * the caller did not ask.  A default language that does not match is not - the survey's own
		 * setting can lag its language list, and the first language is what the console shows. */
		return wanted != null ? -1 : 0;
	}

	private static String labelText(Question q, int index) {
		Label l = label(q, index);
		return l == null ? null : l.text;
	}

	private static String hintText(Question q, int index) {
		Label l = label(q, index);
		return l == null ? null : l.hint;
	}

	private static Label label(Question q, int index) {
		if(q.labels == null || q.labels.isEmpty()) {
			return null;
		}
		return index < q.labels.size() ? q.labels.get(index) : q.labels.get(0);
	}
}
