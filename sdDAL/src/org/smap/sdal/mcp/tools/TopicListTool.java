package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.BundleManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Bundle;
import org.smap.sdal.model.MCPToolResult;

/*
 * Topics, which are called bundles in the database and the API but topics in the console.  The
 * console word is used here because that is the one the person operating the client will say.
 */
public class TopicListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "topic_list";
	}

	@Override
	public String getTitle() {
		return "List topics";
	}

	@Override
	public String getDescription() {
		return "Lists the topics, also known as bundles, that this user can access. A topic groups "
				+ "related surveys so their data can be reported on together.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		BundleManager bm = new BundleManager(ctx.localisation);
		ArrayList<Bundle> topics = bm.getBundles(ctx.sd, ctx.user);

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(topics.isEmpty()) {
			text.append("No topics found.");
		} else {
			text.append("Found ").append(topics.size()).append(" topic(s):");
			for(Bundle topic : topics) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("name", topic.name);
				row.put("description", topic.description);
				rows.add(row);
				text.append("\n- ").append(topic.name);
				if(topic.description != null && topic.description.length() > 0) {
					text.append(": ").append(topic.description);
				}
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("topics", rows);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
