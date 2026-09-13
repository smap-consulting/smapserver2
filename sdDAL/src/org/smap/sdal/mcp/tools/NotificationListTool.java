package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Notification;

/*
 * What is set up to go out, and on what.
 *
 * A notification is a rule: when this happens, send that.  It is the thing behind
 * survey_submission_effects, which counts what one submission would set off; this lists the rules
 * themselves, so a caller can see why something is being sent rather than only how much of it.
 *
 * Nothing here sends anything, and nothing in this package does.  Email, SMS and webhooks cannot be
 * recalled, so MCP prepares and a person sends - see the note on preparing rather than sending.
 *
 * A disabled notification is listed rather than hidden.  "Why did nobody get an email" is answered
 * by a rule that exists and is switched off far more often than by one that was never made.
 */
public class NotificationListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "notification_list";
	}

	@Override
	public String getTitle() {
		return "Notifications";
	}

	@Override
	public String getDescription() {
		return "The notifications set up in a project: what each one reacts to, what it sends, "
				+ "where it sends it, and whether it is switched on. Disabled ones are listed too. "
				+ "Use survey_submission_effects to count what a single submission would set off.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"project_id", property("integer",
						"Optional. Only this project's notifications, from project_list. Omit for "
						+ "every project you are a member of."),
				"enabled_only", property("boolean",
						"Optional. Leave out the ones that are switched off. Default false."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		Map<Integer, String> projects = McpData.userProjects(ctx);
		if(projects.isEmpty()) {
			return new MCPToolResult("You are not a member of any project.", false);
		}
		int projectId = intArg(arguments, "project_id", 0);
		if(projectId > 0 && !projects.containsKey(projectId)) {
			return new MCPToolResult("No such project, or you are not a member of it.", true);
		}
		boolean enabledOnly = boolArg(arguments, "enabled_only", false);

		NotificationManager nm = new NotificationManager(ctx.localisation);
		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();
		int off = 0;

		for(Map.Entry<Integer, String> project : projects.entrySet()) {
			if(projectId > 0 && project.getKey() != projectId) {
				continue;
			}
			ArrayList<Notification> notifications = nm.getProjectNotifications(ctx.sd, null,
					ctx.user, project.getKey(), ctx.timezone);

			for(Notification n : notifications) {
				if(!n.enabled) {
					off++;
					if(enabledOnly) {
						continue;
					}
				}
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("notification_id", n.id);
				row.put("name", n.name);
				row.put("enabled", n.enabled);
				row.put("on", n.trigger);
				row.put("sends", n.target);
				row.put("project", project.getValue());
				/*
				 * Which survey it watches. A notification can be attached to one survey or to a
				 * whole bundle, and the two are different questions, so both are reported rather
				 * than merged into one "survey" that would be wrong half the time.
				 */
				row.put("survey", n.s_name);
				row.put("bundle", n.bundle_name);
				if(n.filter != null && !n.filter.trim().isEmpty()) {
					row.put("onlyWhen", n.filter);
				}
				if(n.period != null) {
					row.put("period", n.period);
				}
				rows.add(row);

				text.append("\n- ").append(n.name);
				if(!n.enabled) {
					text.append(" [switched off]");
				}
				text.append(": on ").append(n.trigger).append(", sends ").append(n.target);
				if(n.s_name != null) {
					text.append(", watching ").append(n.s_name);
				} else if(n.bundle_name != null) {
					text.append(", watching everything in ").append(n.bundle_name);
				}
				if(n.filter != null && !n.filter.trim().isEmpty()) {
					text.append(", only when ").append(n.filter);
				}
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append(enabledOnly && off > 0
					? "Nothing is switched on. " + off + " notification(s) exist but are all off."
					: "No notifications are set up.");
		} else {
			text.insert(0, "Found " + rows.size() + " notification(s):");
			if(!enabledOnly && off > 0) {
				text.append("\n\n").append(off).append(" of them are switched off and send nothing.");
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("notifications", rows);
		data.put("count", rows.size());
		data.put("switchedOff", off);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
