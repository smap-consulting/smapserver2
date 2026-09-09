package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.ChangeLog;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * What has been done to a survey's design, and by whom.
 *
 * The counterpart of data_audit, which answers the same question about one record.  Together they
 * are what makes a change made through MCP reviewable: every one is attributed to the person it was
 * made for and, when something acted for them, to the application that made it.
 *
 * agent is null for a change a person made in the console, which is what most changes are, and the
 * absence is meaningful rather than missing data - it says a human did this by hand.
 */
public class SurveyHistoryTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_history";
	}

	@Override
	public String getTitle() {
		return "Survey change history";
	}

	@Override
	public String getDescription() {
		return "The history of changes to one survey's design: what changed, which version it "
				+ "applied to, who made it and which application made it if one did. Use this to "
				+ "review what has been done to a form, including changes made by an AI client. "
				+ "Returns the whole history, newest first. For the history of a submitted record "
				+ "rather than the form, use data_audit.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to report on, from survey_list"));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("changes", property("array", "The changes, newest first"));
		properties.put("count", property("integer", "How many changes were returned"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
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
		/*
		 * full false, because the change log is fetched outside the block that reads the questions,
		 * options and labels.  Asking for the design as well to get the history would read the whole
		 * survey to answer a question about its history.
		 */
		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		Survey s = sm.getById(
				ctx.sd,
				ctx.cResults,
				ctx.user,
				false,			// temporaryUser
				surveyId,
				false,			// full
				null,			// basePath
				null,			// instanceId
				false,			// getResults
				false,			// generateDummyValues
				false,			// getPropertyTypeQuestions
				false,			// getSoftDeleted
				false,			// getHrk
				"real",
				true,			// getChangeHistory - the whole point of this tool
				false,			// getRoles
				false,			// superUser - the caller's own rights, not an administrator's
				"geojson",
				false,			// referenceSurveys
				false,			// onlyGetLaunched
				false);			// mergeDefaultSetValue

		List<ChangeLog> changes = s.surveyData.changes;
		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(changes == null || changes.isEmpty()) {
			text.append("No changes have been recorded for \"")
					.append(survey.getDisplayName()).append("\".");
		} else {
			/*
			 * Every change, with no limit and no paging.
			 *
			 * The row cap exists for survey data, where the number of records is unbounded and a
			 * caller asking for all of them rarely means it. A survey's history is bounded by what
			 * has been done to one form, and a partial history is worse than none: the point of
			 * reading it is to see everything that happened, and a reader shown the recent half has
			 * no way to tell that is what they are looking at.
			 */
			text.append(changes.size()).append(" change(s) to \"").append(survey.getDisplayName())
					.append("\", newest first:");

			for(int i = 0; i < changes.size(); i++) {
				ChangeLog cl = changes.get(i);

				Map<String, Object> row = new LinkedHashMap<>();
				row.put("version", cl.version);
				row.put("userName", cl.userName);
				if(cl.agent != null) {
					row.put("agent", cl.agent);
					row.put("agentId", cl.agentId);
				}
				row.put("changedTime", cl.updatedTime == null ? null : cl.updatedTime.toString());
				row.put("change", cl.change);
				/*
				 * A change that has to reach the results tables is reported whether or not it got
				 * there.  Saying only that a change was made, when applying it failed, would be the
				 * half of the story that reassures.
				 */
				row.put("appliedToResults", cl.apply_results);
				row.put("succeeded", cl.success);
				if(cl.msg != null && cl.msg.length() > 0) {
					row.put("message", cl.msg);
				}
				rows.add(row);

				text.append("\n- v").append(cl.version).append(" ");
				text.append(cl.updatedTime == null ? "" : cl.updatedTime.toString());
				text.append(" by ").append(cl.userName == null ? "unknown" : cl.userName);
				if(cl.agent != null) {
					text.append(" via ").append(cl.agent);
				}
				if(!cl.success) {
					text.append(" (not applied to the data tables");
					if(cl.msg != null && cl.msg.length() > 0) {
						text.append(": ").append(cl.msg);
					}
					text.append(")");
				}
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("changes", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
