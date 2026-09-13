package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.SettingChange;
import org.smap.sdal.model.Survey;

/*
 * Change a survey's settings: everything about a survey that is not its questions.
 *
 * The project is one of them rather than a move of its own.  Putting a survey in another project is
 * the same kind of act as renaming it or hiding it from devices - one field on the survey - and
 * giving it its own tool would suggest otherwise.  SurveyManager.saveSettings re-points the upload
 * events afterwards so the monitor still shows the survey's history where the survey now is.
 *
 * Only the settings named are changed.  Everything else is read first and written back as it was,
 * because the underlying save writes the whole row: a tool that passed a half filled object would
 * quietly clear every setting the caller did not mention.
 *
 * The change log gets the before and after of each setting actually changed, which is also what is
 * returned, so a caller can put any of it back.
 */
public class SurveySetSettingsTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_set_settings";
	}

	@Override
	public String getTitle() {
		return "Change survey settings";
	}

	@Override
	public String getDescription() {
		return "Changes a survey's settings: its name, its project, its default language, and the "
				+ "options governing how it behaves. Moving a survey to another project is done "
				+ "here, because the project is one of these settings. Only the settings you name "
				+ "are changed; the rest are left as they are. Returns the previous value of each "
				+ "setting changed, so any of it can be put back.";
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
		return "survey_set_settings again with the previous values, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to change, from survey_list"),
				"name", property("string", "Optional. What the survey is called."),
				"project_id", property("integer",
						"Optional. Move the survey to this project, from project_list. The data "
						+ "moves with it."),
				"default_language", property("string",
						"Optional. The language the survey is presented in by default."),
				"hide_on_device", property("boolean",
						"Optional. Keep the survey off the list people see on their phones."),
				"data_survey", property("boolean",
						"Optional. Whether this survey collects data."),
				"oversight_survey", property("boolean",
						"Optional. Whether this survey can be used to review other records."),
				"read_only_survey", property("boolean",
						"Optional. Whether records can only be read, not changed."),
				"track_changes", property("boolean",
						"Optional. Record every change made to an answer."),
				"timing_data", property("boolean",
						"Optional. Record how long each question took."),
				"audit_location_data", property("boolean",
						"Optional. Record where each answer was given."),
				"exclude_empty", property("boolean",
						"Optional. Leave records with no answers out of exports."),
				"search_local_data", property("boolean",
						"Optional. Let the device search data already on it."),
				"my_reference_data", property("boolean",
						"Optional. An enumerator sees only reference data they submitted."),
				"task_file", property("boolean",
						"Optional. Data from a file can be pre-loaded into this survey."),
				"compress_pdf", property("boolean", "Optional. Compress generated PDFs."),
				"show_form_index", property("boolean",
						"Optional. Show an index of the form's sections."),
				"turnstile", property("boolean",
						"Optional. Require a human check on a public web form."),
				"max_reference_records", property("integer",
						"Optional. The most records this survey supplies as reference data to "
						+ "others. 0 means no cap."));
		schema.put("required", new String[] { "survey_id" });
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

		/*
		 * The survey as it stands.  The save writes every column, so this is what the unnamed
		 * settings are written back as.
		 */
		Survey s = McpData.outline(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		List<SettingChange> changes = new ArrayList<>();
		Map<String, Object> previous = new LinkedHashMap<>();

		String name = stringArg(arguments, "name");
		if(name != null && !name.equals(s.surveyData.displayName)) {
			record(changes, previous, "Name", s.surveyData.displayName, name);
			s.surveyData.displayName = name;
		}

		if(arguments.containsKey("project_id")) {
			int projectId = intArg(arguments, "project_id", 0);
			if(projectId > 0 && projectId != s.surveyData.p_id) {
				/*
				 * The project has to be one this user is a member of, or a survey could be moved
				 * somewhere its owner cannot follow it.
				 */
				String projectName = null;
				for(Project p : new ProjectManager(ctx.localisation)
						.getProjects(ctx.sd, ctx.user, false, false, null, false, false)) {
					if(p.id == projectId) {
						projectName = p.name;
						break;
					}
				}
				if(projectName == null) {
					return new MCPToolResult("No such project, or you are not a member of it. "
							+ "project_list shows the ones you can use.", true);
				}
				record(changes, previous, "Project", s.surveyData.projectName, projectName);
				s.surveyData.p_id = projectId;
			}
		}

		String language = stringArg(arguments, "default_language");
		if(language != null && !language.equals(s.surveyData.def_lang)) {
			record(changes, previous, "Default language", s.surveyData.def_lang, language);
			s.surveyData.def_lang = language;
		}

		s.surveyData.hideOnDevice = flag(arguments, "hide_on_device", s.surveyData.hideOnDevice,
				"Hide on device", changes, previous);
		s.surveyData.dataSurvey = flag(arguments, "data_survey", s.surveyData.dataSurvey,
				"Data survey", changes, previous);
		s.surveyData.oversightSurvey = flag(arguments, "oversight_survey", s.surveyData.oversightSurvey,
				"Oversight survey", changes, previous);
		s.surveyData.readOnlySurvey = flag(arguments, "read_only_survey", s.surveyData.readOnlySurvey,
				"Read only", changes, previous);
		s.surveyData.track_changes = flag(arguments, "track_changes", s.surveyData.track_changes,
				"Track changes", changes, previous);
		s.surveyData.timing_data = flag(arguments, "timing_data", s.surveyData.timing_data,
				"Timing data", changes, previous);
		s.surveyData.audit_location_data = flag(arguments, "audit_location_data",
				s.surveyData.audit_location_data, "Audit location", changes, previous);
		s.surveyData.exclude_empty = flag(arguments, "exclude_empty", s.surveyData.exclude_empty,
				"Exclude empty", changes, previous);
		s.surveyData.searchLocalData = flag(arguments, "search_local_data", s.surveyData.searchLocalData,
				"Search local data", changes, previous);
		s.surveyData.myReferenceData = flag(arguments, "my_reference_data", s.surveyData.myReferenceData,
				"My reference data only", changes, previous);
		s.surveyData.task_file = flag(arguments, "task_file", s.surveyData.task_file,
				"Task file", changes, previous);
		s.surveyData.compress_pdf = flag(arguments, "compress_pdf", s.surveyData.compress_pdf,
				"Compress PDF", changes, previous);
		s.surveyData.showFormIndex = flag(arguments, "show_form_index", s.surveyData.showFormIndex,
				"Show form index", changes, previous);
		s.surveyData.turnstile = flag(arguments, "turnstile", s.surveyData.turnstile,
				"Turnstile", changes, previous);

		if(arguments.containsKey("max_reference_records")) {
			int max = intArg(arguments, "max_reference_records", s.surveyData.maxReferenceRecords);
			if(max != s.surveyData.maxReferenceRecords) {
				record(changes, previous, "Max reference records",
						String.valueOf(s.surveyData.maxReferenceRecords), String.valueOf(max));
				s.surveyData.maxReferenceRecords = max;
			}
		}

		if(changes.isEmpty()) {
			return new MCPToolResult("Nothing was changed - every setting given already had that "
					+ "value.", false);
		}

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.saveSettings(ctx.sd, surveyId, s.surveyData, changes, ctx.user);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("survey_id", surveyId);
		data.put("survey", listed.getDisplayName());
		data.put("previous", previous);

		StringBuilder text = new StringBuilder();
		text.append("Changed ").append(changes.size()).append(" setting(s) on \"")
				.append(listed.getDisplayName()).append("\":");
		for(SettingChange sc : changes) {
			text.append("\n- ").append(sc.label).append(": ")
					.append(sc.oldVal == null || sc.oldVal.isEmpty() ? "(not set)" : sc.oldVal)
					.append(" to ").append(sc.newVal);
		}
		text.append("\n\nThe previous values are above and are also returned, so any of this can be "
				+ "set back. survey_history shows the change.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/*
	 * A boolean setting, changed only when the caller named it.  containsKey rather than a default,
	 * because false and absent are different things here and treating them alike would turn every
	 * unmentioned option off.
	 */
	private boolean flag(Map<String, Object> arguments, String key, boolean current, String label,
			List<SettingChange> changes, Map<String, Object> previous) {

		if(!arguments.containsKey(key)) {
			return current;
		}
		boolean wanted = boolArg(arguments, key, current);
		if(wanted != current) {
			record(changes, previous, label, String.valueOf(current), String.valueOf(wanted));
		}
		return wanted;
	}

	private void record(List<SettingChange> changes, Map<String, Object> previous,
			String label, String oldVal, String newVal) {
		changes.add(new SettingChange(label, oldVal == null ? "" : oldVal, newVal));
		previous.put(label, oldVal);
	}
}
