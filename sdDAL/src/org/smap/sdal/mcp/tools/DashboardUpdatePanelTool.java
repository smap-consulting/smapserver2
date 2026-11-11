package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.DashboardManager;
import org.smap.sdal.managers.DashboardManager.Panel;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * Change what a graph or table panel shows.  Settings not named keep the values the panel has.
 */
public class DashboardUpdatePanelTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "dashboard_update_panel";
	}

	@Override
	public String getTitle() {
		return "Change a dashboard panel";
	}

	@Override
	public String getDescription() {
		return "Changes what a graph or table panel on a dashboard shows. Settings not named keep the values "
				+ "the panel has; an empty string takes an optional one off. dashboard_list gives panel_id.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.VIEW_DATA);
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
		return "dashboard_update_panel again with the previous settings, which this returns";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("panel_id", property("integer", "The panel to change, from dashboard_list"));
		properties.putAll(DashboardAddPanelTool.panelProperties());
		properties.put("user", DashboardPanels.userProperty());
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "panel_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int id = intArg(arguments, "panel_id", 0);
		DashboardManager dm = new DashboardManager();
		String user;
		Panel p;
		Map<String, Object> previous;
		try {
			user = DashboardPanels.user(ctx, stringArg(arguments, "user"));
			p = id > 0 ? dm.getPanel(ctx.sd, user, id) : null;
			if(p == null || !(DashboardManager.GRAPH.equals(p.type) || DashboardManager.TABLE.equals(p.type))) {
				return new MCPToolResult("No graph or table panel " + id + " on "
						+ (user.equals(ctx.user) ? "your dashboard" : "the dashboard of " + user)
						+ ". Nothing was changed.", true);
			}
			previous = DashboardPanels.describe(p);
			int projectId = DashboardPanels.projectOf(ctx, p.sId);
			DashboardPanels.apply(ctx, user, p, arguments, false);
			if(DashboardPanels.projectOf(ctx, p.sId) != projectId) {
				return new MCPToolResult("A panel stays in its project, and " + p.sName
						+ " is in another. Nothing was changed.", true);
			}
		} catch (DashboardPanels.Refusal r) {
			return new MCPToolResult(r.getMessage(), true);
		}

		dm.updatePanel(ctx.sd, user, p);

		Map<String, Object> data = DashboardPanels.describe(p);
		data.put("user", user);
		data.put("previous", previous);
		MCPToolResult result = new MCPToolResult("Changed panel " + p.id + ". It now shows "
				+ (p.qname == null ? "all of the data" : p.qname) + " from " + p.sName
				+ " as a " + p.type + ". The previous settings are in previous.");
		result.setStructuredContent(data);
		return result;
	}
}
