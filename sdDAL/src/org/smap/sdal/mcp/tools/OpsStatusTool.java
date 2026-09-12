package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.OpsMonitorManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.OpsAlert;
import org.smap.sdal.model.OpsKpi;
import org.smap.sdal.model.OpsOverview;
import org.smap.sdal.model.OpsUnit;

/*
 * How the work is going: what is open, what is late, and what is shouting.
 *
 * The same overview the operations page draws, in words.  Written for the question a manager asks on
 * a Monday rather than for a screen: how much is outstanding, which teams are behind, and what needs
 * attention first.
 *
 * The cached copy is deliberately not used.  OpsMonitorManager keeps one per user for the page,
 * which is right when somebody is clicking between tabs and wrong here - an agent asked how things
 * stand is asking now, and a number from some earlier minute answers a question nobody put.
 *
 * Alerts come back ordered by priority, because an overview whose most urgent line is fourth is one
 * that has to be read in full to be used.
 */
public class OpsStatusTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "ops_status";
	}

	@Override
	public String getTitle() {
		return "How the work is going";
	}

	@Override
	public String getDescription() {
		return "An overview of outstanding work across the organisation: open cases, open and "
				+ "overdue tasks, how each team is doing, and any alerts needing attention. Use "
				+ "this to answer how things stand, or what needs looking at first.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/* Read fresh, not from the page's cache - see the note above */
		OpsOverview o = new OpsMonitorManager(ctx.localisation)
				.getOverview(ctx.sd, ctx.cResults, oId, ctx.user, ctx.timezone);

		if(o == null) {
			return new MCPToolResult("There is no operations overview for this organisation.", false);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("generatedAt", o.generatedAt);
		data.put("unassignedCases", o.unassignedCases);

		List<Map<String, Object>> kpis = new ArrayList<>();
		StringBuilder text = new StringBuilder("How things stand");
		if(o.generatedAt != null) {
			text.append(" as at ").append(o.generatedAt);
		}
		text.append(":");

		if(o.kpis != null) {
			for(OpsKpi k : o.kpis) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("name", k.label != null ? k.label : k.key);
				row.put("value", k.value);
				/*
				 * rag is the page's colour.  Reported as words because "red" means something to a
				 * reader and a colour code does not, but kept rather than dropped: it is somebody's
				 * judgement about whether the number is bad, which the number alone does not carry.
				 */
				row.put("standing", k.rag == null || k.rag.equals("none") ? null : k.rag);
				kpis.add(row);

				text.append("\n- ").append(k.label != null ? k.label : k.key).append(": ")
						.append(k.value);
				if(k.rag != null && !k.rag.equals("none")) {
					text.append(" (").append(k.rag).append(")");
				}
			}
		}
		data.put("measures", kpis);

		if(o.unassignedCases > 0) {
			text.append("\n- Cases nobody has picked up: ").append(o.unassignedCases);
		}

		List<Map<String, Object>> units = new ArrayList<>();
		if(o.units != null && !o.units.isEmpty()) {
			text.append("\n\nBy team:");
			for(OpsUnit u : o.units) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("team", u.role);
				row.put("openCases", u.openCases);
				row.put("openTasks", u.openTasks);
				row.put("overdue", u.overdue);
				units.add(row);

				text.append("\n- ").append(u.role).append(": ").append(u.openCases)
						.append(" open case(s), ").append(u.openTasks).append(" open task(s)");
				if(u.overdue > 0) {
					text.append(", ").append(u.overdue).append(" overdue");
				}
			}
		}
		data.put("teams", units);

		/*
		 * Sorted by priority, 1 being the most urgent, so the first line read is the one that
		 * matters most.
		 */
		List<Map<String, Object>> alerts = new ArrayList<>();
		if(o.alerts != null && !o.alerts.isEmpty()) {
			List<OpsAlert> sorted = new ArrayList<>(o.alerts);
			sorted.sort((a, b) -> Integer.compare(a.priority, b.priority));

			text.append("\n\n").append(sorted.size()).append(" alert(s), most urgent first:");
			for(OpsAlert a : sorted) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("message", a.message);
				row.put("priority", a.priority);
				row.put("bundle", a.bundle);
				row.put("ageHours", a.sinceSeconds / 3600);
				alerts.add(row);

				text.append("\n- ").append(a.message);
				if(a.bundle != null) {
					text.append(" (").append(a.bundle).append(")");
				}
				long hours = a.sinceSeconds / 3600;
				if(hours > 0) {
					text.append(", ").append(hours).append(" hour(s) old");
				}
			}
		} else {
			text.append("\n\nNo alerts.");
		}
		data.put("alerts", alerts);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
