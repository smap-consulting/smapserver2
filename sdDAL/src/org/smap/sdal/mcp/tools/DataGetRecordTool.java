package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * One submitted record, with its repeating groups nested inside it rather than spread across
 * separate tables.
 *
 * The hierarchy view this uses reads the survey as a super user and applies no row filter, which is
 * fine for assembling the shape of a record and not fine for deciding who may see it.  So the
 * decision is made here first, through the same check the console uses, and the view is only asked
 * for a record the caller has already been shown to be allowed.
 */
public class DataGetRecordTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_get_record";
	}

	@Override
	public String getTitle() {
		return "Read one record";
	}

	@Override
	public String getDescription() {
		return "Returns a single submitted record with any repeating groups nested inside it. "
				+ "Records are named by instance id, which data_query returns as instanceid when "
				+ "include_meta is on. Attachments appear as https URLs that need a browser login; "
				+ "to look at one, read the resource smap://attachment/{survey ident}/{file}, "
				+ "which is the part of that URL after /attachments/.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the record was submitted to"),
				"instance_id", property("string", "The record's instance id, from data_query"),
				"include_meta", property("boolean",
						"Include metadata such as upload time and device. Default false."));
		schema.put("required", new String[] { "survey_id", "instance_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("record", property("object", "The record, with repeating groups nested"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		String instanceId = stringArg(arguments, "instance_id");
		if(surveyId <= 0 || instanceId == null || instanceId.trim().isEmpty()) {
			return new MCPToolResult(
					"Both survey_id and instance_id are required. Use data_query to find them.",
					true);
		}

		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		/*
		 * The same answer whether the record is outside the caller's row filters or does not exist,
		 * so that asking cannot be used to find out which records are there.
		 */
		if(!McpData.canSeeRecord(ctx, survey, instanceId)) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
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
				boolArg(arguments, "include_meta", false),
				/*
				 * The data manager concatenates these straight into the paths it builds, so a null
				 * does not leave them out, it writes the word "null" into every attachment path.
				 */
				GeneralUtilityMethods.getUrlPrefix(ctx.request),
				GeneralUtilityMethods.getAttachmentPrefix(ctx.request, false),
				false);					// do not poll

		Object entity = response.getEntity();
		String json = entity == null ? "{}" : entity.toString();

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("record", new com.google.gson.Gson().fromJson(json, Object.class));

		MCPToolResult result = new MCPToolResult(json);
		result.setStructuredContent(structured);
		return result;
	}
}
