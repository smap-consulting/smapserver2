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
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * One survey's settings, languages and shape, without its questions.
 *
 * The questions are the large part of a survey and are usually not what is being asked for: how many
 * forms does this have, what languages, is it deployed, is it a case management survey.  Reading the
 * whole definition to answer that costs a model its context for nothing, which is why this and
 * survey_questions are separate tools rather than one that returns everything.
 */
public class SurveyGetTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_get";
	}

	@Override
	public String getTitle() {
		return "Survey settings";
	}

	@Override
	public String getDescription() {
		return "One survey's settings, languages and structure: its forms and how many questions "
				+ "each holds, its version, and the options that govern how it behaves. Does not "
				+ "return the questions themselves - use survey_questions for those.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to read, from survey_list"));
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

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("id", s.surveyData.id);
		data.put("ident", s.surveyData.ident);
		data.put("name", s.surveyData.displayName);
		data.put("project", s.surveyData.projectName);
		data.put("version", s.surveyData.version);
		data.put("created", s.surveyData.created == null ? null : s.surveyData.created.toString());
		data.put("deleted", s.surveyData.deleted);
		data.put("blocked", s.surveyData.blocked);
		data.put("defaultLanguage", s.surveyData.def_lang);
		data.put("loadedFromXLS", s.surveyData.loadedFromXLS);

		List<String> languages = new ArrayList<>();
		if(s.surveyData.languages != null) {
			for(Language l : s.surveyData.languages) {
				languages.add(l.name);
			}
		}
		data.put("languages", languages);

		/*
		 * Forms rather than questions.  parentForm zero is the main form; anything else is a
		 * repeating group, and naming it here is what tells a caller which name data_query's form
		 * argument will take.
		 */
		List<Map<String, Object>> forms = new ArrayList<>();
		StringBuilder formText = new StringBuilder();
		if(s.surveyData.forms != null) {
			for(Form f : s.surveyData.forms) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("name", f.name);
				row.put("repeatingGroup", f.parentform > 0);
				row.put("questions", f.questions == null ? 0 : f.questions.size());
				forms.add(row);

				formText.append("\n- ").append(f.name);
				if(f.parentform > 0) {
					formText.append(" (repeating group)");
				}
				formText.append(", ").append(f.questions == null ? 0 : f.questions.size())
						.append(" question(s)");
			}
		}
		data.put("forms", forms);

		Map<String, Object> settings = new LinkedHashMap<>();
		settings.put("timingData", s.surveyData.timing_data);
		settings.put("auditLocationData", s.surveyData.audit_location_data);
		settings.put("trackChanges", s.surveyData.track_changes);
		settings.put("hideOnDevice", s.surveyData.hideOnDevice);
		settings.put("searchLocalData", s.surveyData.searchLocalData);
		settings.put("dataSurvey", s.surveyData.dataSurvey);
		settings.put("oversightSurvey", s.surveyData.oversightSurvey);
		settings.put("readOnlySurvey", s.surveyData.readOnlySurvey);
		settings.put("excludeEmpty", s.surveyData.exclude_empty);
		settings.put("myReferenceData", s.surveyData.myReferenceData);
		settings.put("taskFile", s.surveyData.task_file);
		data.put("settings", settings);

		StringBuilder text = new StringBuilder();
		text.append(s.surveyData.displayName).append(" (").append(s.surveyData.ident)
				.append("), version ").append(s.surveyData.version);
		if(s.surveyData.projectName != null) {
			text.append(", in project ").append(s.surveyData.projectName);
		}
		if(s.surveyData.deleted) {
			text.append(". Deleted");
		}
		if(s.surveyData.blocked) {
			text.append(". Blocked");
		}
		text.append(".\nLanguages: ")
				.append(languages.isEmpty() ? "none recorded" : String.join(", ", languages));
		text.append("\nForms:").append(formText);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
