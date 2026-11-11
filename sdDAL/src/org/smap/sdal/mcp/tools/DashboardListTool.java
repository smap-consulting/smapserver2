package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.DashboardManager;
import org.smap.sdal.managers.DashboardManager.Panel;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * The panels on a dashboard: the survey panels a user has in one project, as the Analysis page shows them.
 */
public class DashboardListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "dashboard_list";
	}

	@Override
	public String getTitle() {
		return "List dashboard panels";
	}

	@Override
	public String getDescription() {
		return "The panels on a dashboard in the Analysis page: for one project, the survey panels a user has, "
				+ "with what each one shows. Yours unless user is given.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.READ;
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("project_id", property("integer", "The project, from project_list"));
		properties.put("user", DashboardPanels.userProperty());
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "project_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int projectId = intArg(arguments, "project_id", 0);
		if(projectId <= 0 || !McpData.userProjects(ctx).containsKey(projectId)) {
			return new MCPToolResult("No such project, or you are not a member of it.", true);
		}
		String user;
		try {
			user = DashboardPanels.user(ctx, stringArg(arguments, "user"));
		} catch (DashboardPanels.Refusal r) {
			return new MCPToolResult(r.getMessage(), true);
		}

		ArrayList<Panel> panels = new DashboardManager().getPanels(ctx.sd, user, projectId);
		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();
		if(panels.isEmpty()) {
			text.append(user.equals(ctx.user) ? "You have" : user + " has").append(" no panels in this project.");
		} else {
			text.append(panels.size()).append(" panel(s):");
			for(Panel p : panels) {
				rows.add(DashboardPanels.describe(p));
				text.append("\n- ").append(p.id).append(": ").append(p.type).append(" of ")
					.append(p.qname == null ? "all data" : p.qname).append(" in ").append(p.sName);
				if(p.title != null && !p.title.isEmpty()) {
					text.append(" \"").append(p.title).append("\"");
				}
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("user", user);
		data.put("panels", rows);
		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
