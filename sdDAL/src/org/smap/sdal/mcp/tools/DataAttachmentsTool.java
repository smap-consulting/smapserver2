package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.constants.SmapQuestionTypes;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpRead;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumn;

/*
 * The files on a survey's records, each with the uri that will actually fetch it.
 *
 * data_query already returns attachment URLs, but they point at /app/attachments, which is behind
 * form authentication and cannot be fetched by a client holding a bearer token.  Turning one into
 * something readable means knowing to strip everything up to /attachments/ and prefix
 * smap://attachment/, which is a rule a model has to be told and can get wrong.  Here the uri is
 * built by the server, so looking at a photo is one step rather than a transformation.
 *
 * It also puts back what the attachment resource cannot know on its own.  A stored attachment path
 * carries the survey and the file name and not the record, so the resource is authorised at survey
 * level; this tool reads through the row filtered path, so the record each file belongs to is known
 * and only files on records the caller may see are listed.
 */
public class DataAttachmentsTool extends AbstractMcpTool {

	private static final int DEFAULT_LIMIT = 100;

	@Override
	public String getName() {
		return "data_attachments";
	}

	@Override
	public String getTitle() {
		return "List record attachments";
	}

	@Override
	public String getDescription() {
		return "Lists the photos, audio, video and other files attached to a survey's records, "
				+ "with the resource uri that fetches each one. Read that uri to look at a file. "
				+ "Give an instance_id for one record's files, or a filter to narrow which records "
				+ "are looked at. Prefer this over reading attachment URLs out of data_query, "
				+ "because those need a browser login and these uris do not.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to look in, from survey_list"),
				"instance_id", property("string",
						"Optional. Only the files on this one record."),
				"filter", property("string",
						"Optional. Only records matching, written as ${question} = 'value'."),
				"form", property("string",
						"Optional. The name of a repeating group, if the files are answered "
						+ "inside one."),
				"limit", property("integer",
						"Optional. Most records to look at. Default " + DEFAULT_LIMIT + "."));
		schema.put("required", new String[] { "survey_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> attachments = new LinkedHashMap<>();
		attachments.put("type", "array");
		attachments.put("description",
				"One entry per file: instanceid, question, uri to read it, and the browser url");

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("attachments", attachments);
		properties.put("count", property("integer", "How many files were found"));

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

		String instanceId = stringArg(arguments, "instance_id");
		if(instanceId != null && !instanceId.trim().isEmpty()
				&& !McpData.canSeeRecord(ctx, survey, instanceId.trim())) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
		}

		McpRead.Request r = new McpRead.Request();
		r.survey = survey;
		r.formName = stringArg(arguments, "form");
		r.filter = stringArg(arguments, "filter");
		r.limit = ctx.cap(intArg(arguments, "limit", DEFAULT_LIMIT));
		if(r.limit <= 0) {
			r.limit = DEFAULT_LIMIT;
		}
		r.includeMeta = true;			// the instance id is what names each file's record

		McpRead.Result read = McpRead.read(ctx, r);
		if(read.noData) {
			return new MCPToolResult("This survey has no data yet.", false);
		}

		List<String> mediaColumns = mediaColumns(read.columns);
		if(mediaColumns.isEmpty()) {
			return new MCPToolResult("This survey has no questions that hold files.", false);
		}

		String wanted = instanceId == null ? null : instanceId.trim();
		List<Map<String, Object>> found = new ArrayList<>();

		for(JSONObject row : read.rows) {
			String rowInstance = McpRead.instanceId(row);
			if(wanted != null && !wanted.equals(rowInstance)) {
				continue;
			}
			for(String column : mediaColumns) {
				if(!row.has(column)) {
					continue;
				}
				String url = row.getString(column);
				if(url == null || url.trim().isEmpty()) {
					continue;
				}
				Map<String, Object> a = new LinkedHashMap<>();
				a.put("instanceid", rowInstance);
				a.put("question", column);
				a.put("uri", uri(url));
				a.put("url", url);
				found.add(a);
			}
		}

		StringBuilder text = new StringBuilder();
		if(found.isEmpty()) {
			text.append("No files found.");
		} else {
			text.append(found.size()).append(" file(s). Read the uri to look at one.");
		}

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("attachments", found);
		structured.put("count", found.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(structured);
		return result;
	}

	/*
	 * The questions whose answer is a file.  Matched on the question type rather than on the shape
	 * of the value, so a text answer that happens to look like a path is not offered as a file.
	 */
	private List<String> mediaColumns(ArrayList<TableColumn> columns) {
		List<String> media = new ArrayList<>();
		if(columns == null) {
			return media;
		}
		for(TableColumn c : columns) {
			if(c.type == null) {
				continue;
			}
			if(c.type.equals(SmapQuestionTypes.IMAGE) || c.type.equals(SmapQuestionTypes.AUDIO)
					|| c.type.equals(SmapQuestionTypes.VIDEO) || c.type.equals("file")) {
				/*
				 * The key the value arrives under, which is not always the name in the column list.
				 * It makes no difference for a question someone named, and every difference for the
				 * handful of metadata columns that get renamed on the way out.
				 */
				media.add(McpRead.jsonKey(c));
			}
		}
		return media;
	}

	/*
	 * The stored URL turned into the resource that will fetch it.
	 *
	 * Attachments live at attachments/<survey ident>/<file>, and that is what appears in the URL, so
	 * the uri is the same thing from /attachments/ onwards with smap://attachment/ in front.  Built
	 * here rather than explained to the model, which is the point of the tool.
	 */
	private String uri(String url) {
		int at = url.indexOf("/attachments/");
		if(at < 0) {
			return null;
		}
		return "smap://attachment/" + url.substring(at + "/attachments/".length());
	}
}
