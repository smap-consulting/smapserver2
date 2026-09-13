package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Delete one record, the way Smap deletes records: by marking it rather than removing it.
 *
 * Does not ask before acting. The change is confined to one record, it is written to that record's
 * history with the application and the person that made it, and data_restore_record puts it back.
 * A tool that asked here would be asking about something nobody can lose, and every such question
 * makes the next one - the one about sending mail that cannot be recalled - a little easier to wave
 * through.
 */
public class DataDeleteRecordTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_delete_record";
	}

	@Override
	public String getTitle() {
		return "Delete a record";
	}

	@Override
	public String getDescription() {
		return "Marks one submitted record as deleted. Smap keeps deleted records rather than "
				+ "removing them, so this can be undone with data_restore_record, and the record "
				+ "is still readable by passing include_deleted to data_query. Records are named "
				+ "by instance id, which data_query returns as instanceid.";
	}

	/* Deleting is not something everyone who can read data may do, which is what the console says */
	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
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
		return "data_restore_record with the same instance id, until the erase job removes the "
				+ "survey after the server's retention period";
	}

	@Override
	public Map<String, Object> getAnnotations() {
		Map<String, Object> a = super.getAnnotations();
		/*
		 * Destructive in the sense a client should warn about, even though it is reversible here.
		 * The hint is advisory and the client is told not to trust it, so it costs nothing to be
		 * plain about what the tool is for.
		 */
		a.put("destructiveHint", Boolean.TRUE);
		return a;
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the record was submitted to"),
				"instance_id", property("string", "The record's instance id, from data_query"),
				"reason", property("string",
						"Why it is being deleted. Recorded against the record and shown to anyone "
						+ "who looks at it later."));
		schema.put("required", new String[] { "survey_id", "instance_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("deleted", property("boolean", "Whether the record was marked deleted"));
		properties.put("instanceid", property("string", "The record that was deleted"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {
		return mark(ctx, arguments, true);
	}

	/*
	 * Shared with the restore tool, because the two differ only in a boolean and in what they say
	 * afterwards, and the part that must not differ is how the record is found and who may touch it.
	 */
	static MCPToolResult mark(McpToolContext ctx, Map<String, Object> arguments, boolean delete)
			throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		String instanceId = stringArg(arguments, "instance_id");
		if(surveyId <= 0 || instanceId == null || instanceId.trim().isEmpty()) {
			return new MCPToolResult(
					"Both survey_id and instance_id are required. Use data_query to find them.",
					true);
		}
		instanceId = instanceId.trim();

		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		/*
		 * Seeing the survey is not permission to touch a record in it, and a record outside the
		 * caller's row filters is answered the same way as one that does not exist, so asking
		 * cannot be used to find out which records are there.
		 */
		if(!McpData.canSeeRecord(ctx, survey, instanceId)) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
		}

		Form topForm = GeneralUtilityMethods.getTopLevelForm(ctx.sd, surveyId);
		String tableName = topForm.tableName;

		/*
		 * The primary key is looked up here rather than accepted from the caller. Records are named
		 * by instance id throughout this package because a key is sequential and can be guessed by
		 * counting; the underlying update needs the key, so this is where the one becomes the other,
		 * after the access checks and not before.
		 */
		int prikey = GeneralUtilityMethods.getPrikey(ctx.cResults, tableName, instanceId);
		if(prikey <= 0) {
			return new MCPToolResult("No such record, or you do not have access to it.", true);
		}

		String reason = stringArg(arguments, "reason");
		if(reason == null || reason.trim().isEmpty()) {
			reason = delete ? "Deleted through MCP" : "Restored through MCP";
		}

		UtilityMethodsEmail.markRecord(
				ctx.cResults,
				ctx.sd,
				ctx.localisation,
				tableName,
				delete,
				reason,
				prikey,
				surveyId,
				topForm.id,
				false,				// modified
				false,				// isChild: this is the top level form
				ctx.user,			// the person, recorded as who made the change
				true,				// mark the repeating group rows too
				ctx.timezone,
				true,				// act whether or not the record has been edited
				true,				// write it to the record's history
				ctx.clientId);		// and say which application asked

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put(delete ? "deleted" : "restored", Boolean.TRUE);
		structured.put("instanceid", instanceId);

		MCPToolResult result = new MCPToolResult(delete
				? "Record deleted. It is kept rather than removed, so data_restore_record will "
					+ "bring it back."
				: "Record restored.");
		result.setStructuredContent(structured);
		return result;
	}
}
