package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Notification;

/*
 * Switch a notification on or off.
 *
 * The one write in this area that is squarely within "prepare, never send".  Switching one off stops
 * things going out and is the thing somebody reaches for in a hurry; switching one on does not send
 * anything either, though it does mean the next matching submission will.  Both are one flag and
 * both are undone by the other, which is why neither stops to ask.
 *
 * The notification is found in the caller's own list rather than by id alone.  getNotification will
 * fetch any notification on the server given its number, so a tool taking an id and passing it
 * straight in would let somebody switch off a rule in a project they have nothing to do with.
 *
 * updateNotification writes the whole row, so the existing notification is read first and written
 * back with one field changed.  Sending it a half filled object would quietly empty everything the
 * caller did not know to supply.
 */
public class NotificationEnableTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "notification_enable";
	}

	@Override
	public String getTitle() {
		return "Switch a notification on or off";
	}

	@Override
	public String getDescription() {
		return "Switches a notification on or off. Switching one off stops it sending anything, and "
				+ "is the quickest way to stop email or SMS going out. Switching one on means the "
				+ "next submission that matches it will send. Nothing is sent by this tool itself, "
				+ "and the notification is otherwise unchanged.";
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
		return "notification_enable again with the opposite value";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"notification_id", property("integer",
						"The notification to switch, from notification_list"),
				"enabled", property("boolean",
						"true to switch it on, false to switch it off"));
		schema.put("required", new String[] { "notification_id", "enabled" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int notificationId = intArg(arguments, "notification_id", 0);
		if(notificationId <= 0) {
			return new MCPToolResult("A notification_id is required. Use notification_list to find "
					+ "one.", true);
		}
		if(!arguments.containsKey("enabled")) {
			return new MCPToolResult("enabled is required: true to switch it on, false to switch "
					+ "it off.", true);
		}
		boolean wanted = boolArg(arguments, "enabled", true);

		NotificationManager nm = new NotificationManager(ctx.localisation);

		/*
		 * Found in the caller's own projects.  An id on its own would reach any notification on the
		 * server.
		 */
		Notification found = null;
		String projectName = null;
		Map<Integer, String> projects = McpData.userProjects(ctx);
		for(Map.Entry<Integer, String> project : projects.entrySet()) {
			ArrayList<Notification> notifications = nm.getProjectNotifications(ctx.sd, null,
					ctx.user, project.getKey(), ctx.timezone);
			for(Notification n : notifications) {
				if(n.id == notificationId) {
					found = n;
					projectName = project.getValue();
					break;
				}
			}
			if(found != null) {
				break;
			}
		}
		if(found == null) {
			return new MCPToolResult("No such notification, or it is in a project you are not a "
					+ "member of. notification_list shows the ones you can see.", true);
		}
		if(found.enabled == wanted) {
			return new MCPToolResult("\"" + found.name + "\" is already switched "
					+ (wanted ? "on" : "off") + ". Nothing changed.", false);
		}

		/*
		 * The whole notification as it stands, with one flag changed.  update_password stays false
		 * so the stored password is left alone - the other branch of the update expects a new one
		 * and would overwrite it with nothing.
		 */
		Notification full = nm.getNotification(ctx.sd, notificationId, ctx.timezone);
		if(full == null) {
			return new MCPToolResult("That notification could not be read.", true);
		}
		full.enabled = wanted;
		full.update_password = false;

		nm.updateNotification(ctx.sd, null, ctx.user, full, ctx.timezone);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("notification_id", notificationId);
		data.put("name", found.name);
		data.put("enabled", wanted);
		data.put("project", projectName);

		StringBuilder text = new StringBuilder();
		text.append("\"").append(found.name).append("\" is now switched ")
				.append(wanted ? "on" : "off").append(".");
		if(wanted) {
			text.append(" The next submission that matches it will send ").append(found.target)
					.append(". Nothing has been sent by this change.");
		} else {
			text.append(" It will not send anything until it is switched back on. Anything already "
					+ "queued to go out is unaffected - this stops new sends, it does not recall "
					+ "old ones.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
