package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Bring back a deleted survey.
 *
 * Looks the survey up among the deleted ones, which is the one place in this package that asks for
 * them: everywhere else a deleted survey is not in the caller's list and so does not exist.  The
 * access rule is otherwise the same - it has to be a survey this person could have listed.
 *
 * It stops working once the subscriber has erased the survey, which happens after the server's
 * retention period.  There is nothing to bring back at that point, and the answer says so rather
 * than reporting a success that changed nothing.
 */
public class SurveyUndeleteTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_undelete";
	}

	@Override
	public String getTitle() {
		return "Undelete a survey";
	}

	@Override
	public String getDescription() {
		return "Brings back a survey that was deleted, with its data. Works until the server erases "
				+ "deleted surveys for good, after which there is nothing to bring back. Use "
				+ "survey_list with include_deleted to find a deleted survey's id.";
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
		return "survey_delete deletes it again";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer",
						"The deleted survey to bring back. survey_list with include_deleted shows "
						+ "them."));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required.", true);
		}
		Survey survey = McpData.deletedSurveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it. If it was "
					+ "deleted a long time ago the server may have erased it for good, and then "
					+ "there is nothing to bring back.", true);
		}
		if(!survey.getDeleted()) {
			return new MCPToolResult("\"" + survey.getDisplayName() + "\" is not deleted, so there "
					+ "is nothing to bring back.", true);
		}

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.restore(ctx.sd, surveyId, ctx.user);

		/* Tell the devices, as the console does, so the form comes back to the phones that had it */
		new MessagingManager(ctx.localisation).surveyChange(ctx.sd, surveyId, 0);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("restored", Boolean.TRUE);
		data.put("survey_id", surveyId);
		data.put("name", survey.getDisplayName());

		/*
		 * Deleting renames a survey with the time it went, and restoring does not rename it back -
		 * that is Smap's behaviour, not something undone here.  Said plainly, because a caller who
		 * expects the old name back will otherwise think the restore only half worked.
		 */
		StringBuilder text = new StringBuilder();
		text.append("Brought back \"").append(survey.getDisplayName())
				.append("\", with the data submitted to it.");
		if(survey.getDisplayName() != null && survey.getDisplayName().matches(".*\\(\\d{4}_\\d{2}_\\d{2} .*\\)$")) {
			text.append("\n\nDeleting added the time to its name and restoring does not take it off "
					+ "again. survey_set_settings will rename it if you want the old name back.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
