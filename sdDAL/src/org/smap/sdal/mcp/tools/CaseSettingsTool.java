package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.CaseManagementAlert;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * How a survey's cases work: which question says what state a case is in, which value means it is
 * finished, and what alerts are watching.
 *
 * This is the tool that makes cases workable without a tool of their own.  A case is a record - it
 * lives in the same table with `_assigned` and `_case_closed` beside it - so data_query and
 * data_get_record already read cases, and data_update_record already closes one.  What could not be
 * known from outside is *which* question closing depends on and what value closes it, because that
 * is configuration rather than data.  Once that is known, the ordinary data tools do the rest.
 *
 * Settings belong to the survey group, as roles and reference filters do.
 */
public class CaseSettingsTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "case_settings";
	}

	@Override
	public String getTitle() {
		return "Case settings";
	}

	@Override
	public String getDescription() {
		return "How cases work in a survey: the question that holds a case's status, the value that "
				+ "means it is finished, the question holding its criticality, and the alerts "
				+ "watching for cases that need attention. Read this before closing a case - "
				+ "closing one is setting its status question to the final value with "
				+ "data_update_record.";
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
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String groupIdent = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);

		CMS cms = new CaseManager(ctx.localisation).getCaseManagementSettings(ctx.sd, groupIdent);
		if(cms == null || cms.settings == null) {
			return new MCPToolResult("\"" + survey.getDisplayName() + "\" does not use case "
					+ "management, so its records are not cases.", false);
		}

		/*
		 * A settings row can exist with nothing in it, so having one is not the same as using case
		 * management.  What decides it is a status question: without one no case is ever marked
		 * finished, whatever else is configured.
		 */
		boolean inUse = cms.settings.statusQuestion != null
				&& cms.settings.statusQuestion.trim().length() > 0;

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("survey", survey.getDisplayName());
		/*
		 * Said outright rather than left to be inferred from a missing key.  Null fields are dropped
		 * on the way out, so a survey that does not use cases and one whose settings failed to load
		 * would otherwise look identical - both an object with a name and nothing else.
		 */
		data.put("usesCaseManagement", inUse);
		data.put("statusQuestion", cms.settings.statusQuestion == null
				? "(none set)" : cms.settings.statusQuestion);
		data.put("finalStatus", cms.settings.finalStatus == null
				? "(none set)" : cms.settings.finalStatus);
		data.put("criticalityQuestion", cms.settings.criticalityQuestion == null
				? "(none set)" : cms.settings.criticalityQuestion);

		List<Map<String, Object>> alerts = new ArrayList<>();
		if(cms.alerts != null) {
			for(CaseManagementAlert a : cms.alerts) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("name", a.name);
				row.put("period", a.period);
				row.put("filter", a.filter);
				alerts.add(row);
			}
		}
		data.put("alerts", alerts);

		StringBuilder text = new StringBuilder();
		if(!inUse) {
			text.append("\"").append(survey.getDisplayName()).append("\" does not use case "
					+ "management: no question is set to hold a case's status, so no record in it "
					+ "is ever marked finished as a case.");
			if(cms.alerts != null && !cms.alerts.isEmpty()) {
				text.append(" It does have ").append(cms.alerts.size())
						.append(" alert(s) configured, listed below.");
			}
			text.append("\n");
		}
		text.append("Cases in \"").append(survey.getDisplayName()).append("\":");
		if(cms.settings.statusQuestion != null) {
			text.append("\n- status is held in ").append(cms.settings.statusQuestion);
			if(cms.settings.finalStatus != null) {
				text.append(", and a case is finished when it is set to \"")
						.append(cms.settings.finalStatus).append("\"");
			}
		} else {
			text.append("\n- no status question is set, so no case is ever marked finished");
		}
		if(cms.settings.criticalityQuestion != null) {
			text.append("\n- criticality is held in ").append(cms.settings.criticalityQuestion);
		}
		if(alerts.isEmpty()) {
			text.append("\n- no alerts are watching these cases");
		} else {
			text.append("\n").append(alerts.size()).append(" alert(s):");
			for(Map<String, Object> a : alerts) {
				text.append("\n- ").append(a.get("name"));
				if(a.get("period") != null) {
					text.append(", every ").append(a.get("period"));
				}
				if(a.get("filter") != null) {
					text.append(", when ").append(a.get("filter"));
				}
			}
		}
		if(cms.settings.statusQuestion != null && cms.settings.finalStatus != null) {
			text.append("\n\nTo close a case, set ").append(cms.settings.statusQuestion)
					.append(" to \"").append(cms.settings.finalStatus)
					.append("\" with data_update_record. The closing date is recorded by the "
							+ "server when that happens.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
