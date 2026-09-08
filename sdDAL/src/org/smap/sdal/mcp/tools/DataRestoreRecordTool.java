package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * Put back a record that was deleted.
 *
 * This is the reversal data_delete_record declares, and it exists because of that: the rule is that
 * a tool which changes something has to say how the change is undone, and an undo that is only
 * described is not one.
 */
public class DataRestoreRecordTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "data_restore_record";
	}

	@Override
	public String getTitle() {
		return "Restore a deleted record";
	}

	@Override
	public String getDescription() {
		return "Brings back a record that was marked deleted. Use data_query with "
				+ "include_deleted set to only to find deleted records and their instance ids.";
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
		return "data_delete_record with the same instance id";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the record was submitted to"),
				"instance_id", property("string",
						"The record's instance id. Find deleted records with data_query and "
						+ "include_deleted set to only."),
				"reason", property("string", "Why it is being restored. Recorded on the record."));
		schema.put("required", new String[] { "survey_id", "instance_id" });
		return schema;
	}

	@Override
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("restored", property("boolean", "Whether the record was brought back"));
		properties.put("instanceid", property("string", "The record that was restored"));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {
		return DataDeleteRecordTool.mark(ctx, arguments, false);
	}
}
