package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Delete a survey, reversibly.
 *
 * Always the soft delete, never the hard one.  Smap keeps a deleted survey and its data, and the
 * subscriber erases it for good after server.keep_erased_days - a hundred days by default - so
 * undoing this is possible until then and impossible afterwards.  The hard erase is not exposed at
 * all, and neither is deleting the data along with the survey.
 *
 * Devices are told, the same way the console tells them, so a form disappears from a phone rather
 * than staying on it until somebody notices.
 */
public class SurveyDeleteTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_delete";
	}

	@Override
	public String getTitle() {
		return "Delete a survey";
	}

	@Override
	public String getDescription() {
		return "Marks a survey deleted. The survey and everything submitted to it are kept, so this "
				+ "can be undone with survey_undelete, until the server erases deleted surveys for "
				+ "good after its retention period. The data is never deleted with the survey.";
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
		return "survey_undelete brings it back, with its data";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to delete, from survey_list"));
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

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.delete(ctx.sd, ctx.cResults, surveyId,
				false,			// hard - never from here
				false,			// delData - the data outlives the survey
				ctx.user,
				GeneralUtilityMethods.getBasePath(ctx.request),
				null,			// tables
				0);				// not a replacement for another survey

		/* Tell the devices, as the console does, so the form leaves the phones that have it */
		new MessagingManager(ctx.localisation).surveyChange(ctx.sd, surveyId, 0);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("survey_id", surveyId);
		data.put("name", survey.getDisplayName());

		MCPToolResult result = new MCPToolResult("Deleted \"" + survey.getDisplayName()
				+ "\". The survey and its data are kept, so survey_undelete can bring it back until "
				+ "the server erases deleted surveys for good.");
		result.setStructuredContent(data);
		return result;
	}
}
