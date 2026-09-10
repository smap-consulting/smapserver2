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
 * Remove a notification.
 *
 * Deleting is a real delete - the row goes, unlike a survey or a record, which are kept and marked.
 * So the answer says what was there in enough detail to build it again, because that is the only
 * form the undo can take.
 *
 * It stops nothing that has already gone: mail already sent stays sent, and anything queued still
 * leaves.  Switching a notification off with notification_enable is the way to stop it while keeping
 * it, and is what somebody usually wants.
 */
public class NotificationDeleteTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "notification_delete";
	}

	@Override
	public String getTitle() {
		return "Delete a notification";
	}

	@Override
	public String getDescription() {
		return "Removes a notification for good - unlike a survey or a record, it is not kept. To "
				+ "stop one sending while keeping it, switch it off with notification_enable "
				+ "instead, which is usually what is wanted. Returns what the notification was, "
				+ "since that is all an undo can work from.";
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
		return "notification_create builds it again from the details this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"notification_id", property("integer",
						"The notification to remove, from notification_list"));
		schema.put("required", new String[] { "notification_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int notificationId = intArg(arguments, "notification_id", 0);
		if(notificationId <= 0) {
			return new MCPToolResult("A notification_id is required. Use notification_list to find "
					+ "one.", true);
		}

		NotificationManager nm = new NotificationManager(ctx.localisation);

		/* In the caller's own projects, for the same reason notification_enable looks there */
		Notification found = null;
		Map<Integer, String> projects = McpData.userProjects(ctx);
		for(Map.Entry<Integer, String> project : projects.entrySet()) {
			ArrayList<Notification> notifications = nm.getProjectNotifications(ctx.sd, null,
					ctx.user, project.getKey(), ctx.timezone);
			for(Notification n : notifications) {
				if(n.id == notificationId) {
					found = n;
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

		/* Everything about it, read before it goes, because afterwards there is nowhere to read it */
		Notification full = nm.getNotification(ctx.sd, notificationId, ctx.timezone);

		Map<String, Object> was = new LinkedHashMap<>();
		was.put("name", found.name);
		was.put("on", found.trigger);
		was.put("sends", found.target);
		was.put("survey", found.s_name);
		was.put("enabled", found.enabled);
		if(found.filter != null && !found.filter.trim().isEmpty()) {
			was.put("onlyWhen", found.filter);
		}
		if(full != null && full.notifyDetails != null) {
			was.put("sendTo", full.notifyDetails.emails);
			was.put("subject", full.notifyDetails.subject);
			was.put("content", full.notifyDetails.content);
		}

		nm.deleteNotification(ctx.sd, ctx.user, notificationId, found.s_id);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("notification_id", notificationId);
		data.put("was", was);

		StringBuilder text = new StringBuilder();
		text.append("Deleted \"").append(found.name).append("\".");
		if(found.enabled) {
			text.append(" It was switched on, so it will not send again.");
		} else {
			text.append(" It was already switched off.");
		}
		text.append(" Anything it has already sent stays sent, and anything queued still goes.\n\n"
				+ "What it was is above, and notification_create can build it again from that.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
