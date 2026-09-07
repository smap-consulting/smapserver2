package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * The submitted records for one survey.
 *
 * Access is established the same way as everywhere else in this package: by looking for the survey
 * in the caller's own list.  A survey the caller could not list does not exist as far as this tool
 * is concerned, so there is no separate permission check to keep in step with the listing one.
 */
public class SurveyDataTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_data";
	}

	@Override
	public String getTitle() {
		return "Read survey data";
	}

	@Override
	public String getDescription() {
		return "Returns the submitted records for one survey, including any repeating groups. "
				+ "Use survey_list first to find the survey id. Give an instance id to fetch a "
				+ "single record.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to read, from survey_list"),
				"instance_id", property("string",
						"Optional. Return only the record with this instance id."),
				"include_meta", property("boolean",
						"Include metadata columns such as upload time and device. Default false."));
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
		 * Records are addressed by instance id, never by the sequential primary key.  A prikey can
		 * be guessed by counting, so accepting one would let a caller walk a table they were never
		 * shown.
		 */
		String instanceId = stringArg(arguments, "instance_id");
		boolean includeMeta = boolArg(arguments, "include_meta", false);

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		ArrayList<Survey> surveys = sm.getSurveys(ctx.sd, ctx.user, false, false, 0,
				false, false, false, false, false, null);

		Survey survey = null;
		for(Survey s : surveys) {
			if(s.getId() == surveyId) {
				survey = s;
				break;
			}
		}
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		DataManager dm = new DataManager(ctx.localisation, ctx.timezone);
		Response response = dm.getRecordHierarchy(
				ctx.sd,
				ctx.cResults,
				ctx.user,
				survey.getIdent(),
				surveyId,
				instanceId,
				"no",					// do not merge select multiples
				ctx.localisation,
				ctx.timezone,
				includeMeta,
				null,					// urlprefix
				null,					// attachmentPrefix
				false);					// do not poll

		Object entity = response.getEntity();
		String json = entity == null ? "[]" : entity.toString();

		MCPToolResult result = new MCPToolResult(json);
		/*
		 * The data manager has already produced JSON, so it is handed back as the structured half
		 * of the result too rather than being parsed and rebuilt just to change its type.
		 */
		result.setStructuredContent(new com.google.gson.Gson().fromJson(json, Object.class));
		return result;
	}
}
