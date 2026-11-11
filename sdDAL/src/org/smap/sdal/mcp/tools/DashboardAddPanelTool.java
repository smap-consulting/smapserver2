package org.smap.sdal.mcp.tools;

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
 * Add a graph or table panel to a dashboard.
 *
 * Does not ask first: a panel only changes what somebody sees in the Analysis page, nothing is sent and
 * dashboard_delete_panel takes it away again.
 */
public class DashboardAddPanelTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "dashboard_add_panel";
	}

	@Override
	public String getTitle() {
		return "Add a dashboard panel";
	}

	@Override
	public String getDescription() {
		return "Adds a graph or table panel to a dashboard in the Analysis page, after the panels already there. "
				+ "A graph shows one question, summarised by function and optionally grouped by another "
				+ "question or by time. A table shows one question the same way, or all of the survey's data "
				+ "when no question is given.";
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
		return "dashboard_delete_panel with the panel_id this returns";
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> properties = panelProperties();
		properties.put("project_id", property("integer", "The project whose dashboard this is, from project_list"));
		properties.put("user", DashboardPanels.userProperty());
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "project_id", "type", "survey_id" });
		return schema;
	}

	/* The settings of a panel, shared with dashboard_update_panel */
	static Map<String, Object> panelProperties() {
		Map<String, Object> p = new LinkedHashMap<>();
		p.put("type", property("string", "graph or table"));
		p.put("survey_id", property("integer", "The survey shown, which has to be in the project"));
		p.put("question", property("string", "The question shown, by name. Needed for a graph; a table with "
				+ "no question shows all of the survey's data"));
		p.put("function", property("string", "How the question is summarised. For select questions count or "
				+ "percent (default percent); for int and decimal average or total (default average); for "
				+ "string and calculate questions count, percent, average or total; or none"));
		p.put("group_by", property("string", "Optional. A question to group the results by"));
		p.put("date_question", property("string", "Optional. A date or dateTime question, for grouping by time "
				+ "or limiting to dates"));
		p.put("from_date", property("string", "Optional. yyyy-mm-dd, the first date included"));
		p.put("to_date", property("string", "Optional. yyyy-mm-dd, the last date included"));
		p.put("time_group", property("string", "Optional. none, hour, day, week, month or year. Anything but "
				+ "none needs date_question"));
		p.put("chart_type", property("string", "Optional, for a graph. histogram (default) or pie"));
		p.put("title", property("string", "Optional. The panel's title; the survey name is shown when empty"));
		return p;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int projectId = intArg(arguments, "project_id", 0);
		if(projectId <= 0 || !McpData.userProjects(ctx).containsKey(projectId)) {
			return new MCPToolResult("No such project, or you are not a member of it.", true);
		}

		Panel p = new Panel();
		String user;
		try {
			user = DashboardPanels.user(ctx, stringArg(arguments, "user"));
			DashboardPanels.apply(ctx, user, p, arguments, true);
		} catch (DashboardPanels.Refusal r) {
			return new MCPToolResult(r.getMessage(), true);
		}
		if(DashboardPanels.projectOf(ctx, p.sId) != projectId) {
			return new MCPToolResult(p.sName + " is not in that project. Nothing was changed.", true);
		}

		p.id = new DashboardManager().addPanel(ctx.sd, user, p);

		Map<String, Object> data = DashboardPanels.describe(p);
		data.put("user", user);
		MCPToolResult result = new MCPToolResult("Added " + p.type + " panel " + p.id + " showing "
				+ (p.qname == null ? "all of the data" : p.qname) + " from " + p.sName
				+ (user.equals(ctx.user) ? " to your dashboard." : " to the dashboard of " + user + ".")
				+ " It appears in the Analysis page for this project.");
		result.setStructuredContent(data);
		return result;
	}
}
