package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;

/*
 * Put back everything one bulk change altered.
 *
 * This is the reversal data_bulk_update declares, and it exists because of that. The registry
 * insists a mutating tool names how it is undone; it cannot check the naming is true, so a tool
 * whose undo was only described would pass registration and fail the first person who needed it.
 *
 * The old values are read from the records' own history rather than remembered anywhere: a bulk
 * change writes an event per record with the value before and after, and the change set identifier
 * is what gathers them back up.
 */
public class DataBulkUndoTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_bulk_undo";
	}

	@Override
	public String getTitle() {
		return "Undo a bulk change";
	}

	@Override
	public String getDescription() {
		return "Puts every record changed by one bulk update back to the value it held. Takes the "
				+ "change set id that data_bulk_update returned. Records changed again since are "
				+ "still put back to what they were before that bulk change, so check "
				+ "data_audit first if others may have edited them.";
	}

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
		return "data_bulk_undo records its own change set, so undoing an undo is another call with "
				+ "the id this returns";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the bulk change was made in"),
				"change_set", property("string", "The id data_bulk_update returned"));
		schema.put("required", new String[] { "survey_id", "change_set" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("restored", property("integer", "How many records were put back"));
		properties.put("change_set", property("string", "The change set this undo itself recorded"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	/* One record's worth of what a bulk change did to it */
	private static class Change {
		String col;
		String displayName;
		String newVal;
		String oldVal;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		String changeSet = stringArg(arguments, "change_set");
		if(surveyId <= 0 || changeSet == null || changeSet.trim().isEmpty()) {
			return new MCPToolResult("A survey_id and a change_set are required. data_bulk_update "
					+ "returns the change set id.", true);
		}
		changeSet = changeSet.trim();

		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}

		String tableName = GeneralUtilityMethods.getMainResultsTable(ctx.sd, ctx.cResults, surveyId);

		/*
		 * Restricted to this survey's own table as well as to the change set, so an id belonging to
		 * a change in another survey cannot be replayed here.
		 */
		String sql = "select instanceid, changes from record_event "
				+ "where change_set = ? and table_name = ? and changes is not null "
				+ "order by id asc";

		Map<String, List<Change>> byInstance = new LinkedHashMap<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(sql)) {
			pstmt.setString(1, changeSet);
			pstmt.setString(2, tableName);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String instanceId = rs.getString("instanceid");
				List<Change> changes = new Gson().fromJson(rs.getString("changes"),
						new TypeToken<List<Change>>(){}.getType());
				if(instanceId != null && changes != null) {
					byInstance.put(instanceId, changes);
				}
			}
		}

		if(byInstance.isEmpty()) {
			return new MCPToolResult("No bulk change with that id was made to this survey.", true);
		}

		String groupSurvey = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);
		String urlPrefix = GeneralUtilityMethods.getUrlPrefix(ctx.request);

		/*
		 * The undo is itself a bulk change and records its own set, so it can be undone in turn.
		 * Nothing here is a special kind of write.
		 */
		String undoSet = java.util.UUID.randomUUID().toString();
		ActionManager am = new ActionManager(ctx.localisation, ctx.timezone, ctx.clientId, undoSet);

		int restored = 0;
		int skipped = 0;
		for(Map.Entry<String, List<Change>> e : byInstance.entrySet()) {
			/*
			 * A record that has since moved out of the caller's reach is left alone rather than
			 * written to. The change set says what was done; it does not grant access to redo it.
			 */
			if(!McpData.canSeeRecord(ctx, survey, e.getKey())) {
				skipped++;
				continue;
			}

			List<Map<String, Object>> updates = new ArrayList<>();
			for(Change c : e.getValue()) {
				if(c.col == null) {
					continue;
				}
				String previous = c.oldVal == null ? "" : c.oldVal;
				Map<String, Object> update = new LinkedHashMap<>();
				update.put("name", c.col);
				update.put("displayName", c.displayName);
				update.put("value", previous);
				update.put("clear", previous.isEmpty());
				updates.add(update);
			}
			if(updates.isEmpty()) {
				continue;
			}

			am.processUpdateGroupSurvey(ctx.request, ctx.sd, ctx.cResults, ctx.user, surveyId,
					e.getKey(), 0, groupSurvey, null, new Gson().toJson(updates), true, urlPrefix);
			restored++;
		}

		Map<String, Object> structured = new LinkedHashMap<>();
		structured.put("restored", restored);
		structured.put("change_set", undoSet);
		if(skipped > 0) {
			structured.put("skipped", skipped);
		}

		StringBuilder text = new StringBuilder("Put ").append(restored).append(" record(s) back.");
		if(skipped > 0) {
			text.append(" ").append(skipped)
					.append(" were left alone because you no longer have access to them.");
		}
		text.append(" This undo is itself change set ").append(undoSet).append(".");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(structured);
		return result;
	}
}
