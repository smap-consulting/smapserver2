package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Survey;

/*
 * The choices behind a survey's select questions.
 *
 * Lists are shared: several questions can use one list, which is why they are addressed by list name
 * rather than by question.  survey_questions reports the list name for each select question.
 *
 * A list whose choices come from an external file reports them as external.  Those are read from a
 * csv on the server rather than held against the survey, so what is returned is what the definition
 * knows, which may not be all of what a device would see.
 */
public class SurveyOptionsTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_options";
	}

	@Override
	public String getTitle() {
		return "Survey choice lists";
	}

	@Override
	public String getDescription() {
		return "The choice lists behind a survey's select questions: the value stored for each "
				+ "choice and the label shown for it. One list can serve several questions, so ask "
				+ "for it by list name - survey_questions reports the name each question uses.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to read, from survey_list"),
				"list_name", property("string",
						"Optional. Only this choice list. Omit for every list in the survey."),
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
		/*
		 * The lists alone. This tool never reports a question, so loading the design to reach the
		 * choices attached to it would be work thrown away.
		 */
		Survey s = McpData.optionLists(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String listWanted = stringArg(arguments, "list_name");
		String languageWanted = stringArg(arguments, "language");

		int langIndex = languageIndex(s, languageWanted);
		if(langIndex < 0) {
			return new MCPToolResult("This survey has no language called \"" + languageWanted
					+ "\". survey_get lists the ones it has.", true);
		}

		Map<String, OptionList> lists = s.surveyData.optionLists;
		if(lists == null || lists.isEmpty()) {
			return new MCPToolResult("This survey has no choice lists. That is normal for a form "
					+ "with no select questions.", false);
		}
		if(listWanted != null && !lists.containsKey(listWanted)) {
			return new MCPToolResult("This survey has no choice list called \"" + listWanted
					+ "\". survey_questions reports the list name for each select question.", true);
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();
		int total = 0;

		for(Map.Entry<String, OptionList> e : lists.entrySet()) {
			if(listWanted != null && !listWanted.equals(e.getKey())) {
				continue;
			}
			List<Map<String, Object>> choices = new ArrayList<>();
			boolean external = false;

			if(e.getValue() != null && e.getValue().options != null) {
				for(Option o : e.getValue().options) {
					Map<String, Object> choice = new LinkedHashMap<>();
					choice.put("value", o.value);
					choice.put("label", labelText(o, langIndex));
					choices.add(choice);
					external = external || o.externalFile;
					total++;
				}
			}

			Map<String, Object> row = new LinkedHashMap<>();
			row.put("list", e.getKey());
			row.put("external", external);
			row.put("choices", choices);
			rows.add(row);

			text.append(text.length() == 0 ? "" : "\n").append(e.getKey())
					.append(external ? " (from an external file)" : "")
					.append(", ").append(choices.size()).append(" choice(s):");
			for(Map<String, Object> choice : choices) {
				text.append("\n- ").append(choice.get("value"));
				Object label = choice.get("label");
				if(label != null && !label.equals(choice.get("value"))) {
					text.append(": ").append(label);
				}
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("lists", rows);
		data.put("count", total);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

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
		return wanted != null ? -1 : 0;
	}

	/*
	 * An external choice keeps its label in externalLabel rather than labels, so both are tried
	 * before falling back to the stored value, which is always something rather than nothing.
	 */
	private static String labelText(Option o, int index) {

		if(o.labels != null && !o.labels.isEmpty()) {
			Label l = index < o.labels.size() ? o.labels.get(index) : o.labels.get(0);
			if(l != null && l.text != null && l.text.length() > 0) {
				return l.text;
			}
		}
		if(o.externalLabel != null && !o.externalLabel.isEmpty()) {
			int i = index < o.externalLabel.size() ? index : 0;
			if(o.externalLabel.get(i) != null && o.externalLabel.get(i).text != null) {
				return o.externalLabel.get(i).text;
			}
		}
		return o.value;
	}
}
