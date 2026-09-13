package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.TranslationManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.ManifestValue;
import org.smap.sdal.model.Survey;

/*
 * The files a survey carries with it: the images, audio and video shown as part of a question, and
 * the csv files a question reads its choices from.
 *
 * This is the survey's manifest, not the answers people have given.  Photographs taken while
 * filling a form in are attachments on records and belong to data_attachments; these are files the
 * form was published with, the same for everyone who fills it in.
 *
 * A csv here is worth noticing for a second reason: a question reading its choices from one is
 * reading data that is not in the survey, so a choice list that looks empty in survey_options may
 * simply live in a file listed here.
 */
public class SurveyMediaListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_media_list";
	}

	@Override
	public String getTitle() {
		return "Survey media files";
	}

	@Override
	public String getDescription() {
		return "The files published with a survey: images, audio and video used in questions, and "
				+ "csv files a question reads its choices from. These are part of the form itself "
				+ "and are the same for everyone filling it in. For photographs and recordings "
				+ "collected while answering, use data_attachments instead.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to look at, from survey_list"));
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

		/*
		 * forDevice false: the device's view resolves each file to something a phone can fetch,
		 * which is not what is being asked for here.
		 */
		TranslationManager tm = new TranslationManager();
		List<ManifestValue> manifest = tm.getManifestBySurvey(ctx.sd, ctx.user, surveyId,
				GeneralUtilityMethods.getBasePath(ctx.request), survey.getIdent(), false);

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(manifest == null || manifest.isEmpty()) {
			text.append("\"").append(survey.getDisplayName())
					.append("\" has no media files of its own.");
		} else {
			text.append("\"").append(survey.getDisplayName()).append("\" carries ")
					.append(manifest.size()).append(" file(s):");

			for(ManifestValue mv : manifest) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("file", mv.fileName);
				row.put("type", mv.type);
				if(mv.url != null) {
					row.put("url", mv.url);
				}
				/*
				 * A linked survey means these choices are another survey's data rather than a file
				 * somebody uploaded, and the count is how many records are reachable through it.
				 */
				if(mv.linkedSurveyIdent != null) {
					row.put("linkedSurvey", mv.linkedSurveyIdent);
					row.put("linkedRecords", mv.linkedRecords);
				}
				rows.add(row);

				text.append("\n- ").append(mv.fileName);
				if(mv.type != null) {
					text.append(" (").append(mv.type).append(")");
				}
				if(mv.linkedSurveyIdent != null) {
					text.append(", from survey ").append(mv.linkedSurveyIdent)
							.append(" - ").append(mv.linkedRecords).append(" record(s)");
				}
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("files", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
