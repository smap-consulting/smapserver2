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
 * Take a panel off a dashboard.  The panel's settings are returned, so it can be added again.
 */
public class DashboardDeletePanelTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "dashboard_delete_panel";
	}

	@Override
	public String getTitle() {
		return "Remove a dashboard panel";
	}

	@Override
	public String getDescription() {
		return "Removes a panel from a dashboard. Only the panel goes - no data is touched. Its settings are "
				+ "returned so dashboard_add_panel can put it back. dashboard_list gives panel_id.";
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
		return "dashboard_add_panel with the settings this returns";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("panel_id", property("integer", "The panel to remove, from dashboard_list"));
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
		try {
			user = DashboardPanels.user(ctx, stringArg(arguments, "user"));
		} catch (DashboardPanels.Refusal r) {
			return new MCPToolResult(r.getMessage(), true);
		}
		Panel p = id > 0 ? dm.getPanel(ctx.sd, user, id) : null;
		if(p == null) {
			return new MCPToolResult("No panel " + id + " on "
					+ (user.equals(ctx.user) ? "your dashboard" : "the dashboard of " + user)
					+ ". Nothing was changed.", true);
		}

		dm.deletePanel(ctx.sd, user, id);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("user", user);
		data.put("panel", DashboardPanels.describe(p));
		MCPToolResult result = new MCPToolResult("Removed panel " + id + " (" + p.type + " of "
				+ (p.qname == null ? "all of the data" : p.qname) + " from " + p.sName + "). Its settings are in "
				+ "panel, for dashboard_add_panel if it is wanted back.");
		result.setStructuredContent(data);
		return result;
	}
}
