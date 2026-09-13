package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.WorkflowManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.WorkflowData;
import org.smap.sdal.model.WorkflowItem;
import org.smap.sdal.model.WorkflowLink;

/*
 * What happens after a form is submitted, as a graph.
 *
 * A workflow is not a thing in its own right in Smap: it is what you get when you read the
 * notifications, task groups and case rules of an organisation together and draw the arrows between
 * them.  WorkflowManager does exactly that for the console's workflow page, and this reports the
 * same graph in words.
 *
 * Read only, and deliberately.  Setting a workflow means creating notifications, task groups and
 * case rules, which are their own tools in their own increments; a tool that claimed to "set a
 * workflow" would be a tool that quietly did several other things.
 *
 * The positions of the boxes are not reported.  They are where somebody dragged them on a screen and
 * say nothing about what the server does.
 */
public class WorkflowListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "workflow_list";
	}

	@Override
	public String getTitle() {
		return "Workflow";
	}

	@Override
	public String getDescription() {
		return "What happens after a form is submitted: the notifications, task groups and case "
				+ "steps in this organisation and how they lead into each other. Use this to see "
				+ "what a submission sets off, or where a step comes from. Read only - a workflow "
				+ "is made by creating the notifications and task groups it is drawn from.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE, Authorise.MANAGE_TASKS);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		WorkflowData wf = new WorkflowManager().getWorkflowItems(ctx.sd, ctx.user);

		List<Map<String, Object>> steps = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(wf == null || wf.items == null || wf.items.isEmpty()) {
			text.append("Nothing is set up to happen after a submission.");
		} else {
			text.append(wf.items.size()).append(" step(s):");
			for(WorkflowItem item : wf.items) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("id", item.id);
				row.put("type", item.type);
				row.put("name", item.label != null ? item.label : item.name);
				row.put("enabled", item.enabled);
				row.put("project", item.project);
				row.put("bundle", item.bundle);
				row.put("assignee", item.assignee);
				steps.add(row);

				text.append("\n- ").append(item.label != null ? item.label : item.name)
						.append(" (").append(item.type).append(")");
				if(!item.enabled) {
					text.append(" [turned off]");
				}
				if(item.assignee != null && !item.assignee.isEmpty()) {
					text.append(", to ").append(item.assignee);
				}
				if(item.project != null) {
					text.append(", in ").append(item.project);
				}
			}
		}

		/* The arrows, which are the half that says what leads to what */
		List<Map<String, Object>> links = new ArrayList<>();
		if(wf != null && wf.links != null) {
			for(WorkflowLink l : wf.links) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("from", l.from);
				row.put("to", l.to);
				links.add(row);
			}
			if(!links.isEmpty()) {
				text.append("\n\n").append(links.size()).append(" connection(s) between them.");
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("steps", steps);
		data.put("links", links);
		data.put("count", steps.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
