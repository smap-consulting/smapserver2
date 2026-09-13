package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Project;

/*
 * The projects this user belongs to.
 *
 * Projects are how survey access is granted in Smap, so this is usually the first thing worth
 * knowing: a survey the caller cannot see is in a project they are not a member of.  "all" is
 * deliberately false - the wider list is an administrator's view of the organisation, and an agent
 * should hold what the person holds.
 */
public class ProjectListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "project_list";
	}

	@Override
	public String getTitle() {
		return "List projects";
	}

	@Override
	public String getDescription() {
		return "Lists the projects this user is a member of. Survey access follows project "
				+ "membership, so a survey that does not appear in survey_list is usually in a "
				+ "project not listed here.";
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
	public Map<String, Object> getOutputSchema() {
		Map<String, Object> props = new LinkedHashMap<>();
		props.put("id", property("integer", "Project id"));
		props.put("name", property("string", "Project name"));
		props.put("description", property("string", "Project description"));

		Map<String, Object> item = new LinkedHashMap<>();
		item.put("type", "object");
		item.put("properties", props);

		Map<String, Object> list = new LinkedHashMap<>();
		list.put("type", "array");
		list.put("items", item);

		Map<String, Object> top = new LinkedHashMap<>();
		top.put("projects", list);

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", top);
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		ProjectManager pm = new ProjectManager(ctx.localisation);
		ArrayList<Project> projects = pm.getProjects(ctx.sd, ctx.user, false, false, null, false, false);

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(projects.isEmpty()) {
			text.append("You are not a member of any project.");
		} else {
			text.append("Member of ").append(projects.size()).append(" project(s):");
			for(Project p : projects) {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("id", p.id);
				row.put("name", p.name);
				row.put("description", p.desc);
				rows.add(row);

				text.append("\n- ").append(p.name).append(" (id ").append(p.id).append(")");
				if(p.desc != null && p.desc.trim().length() > 0) {
					text.append(": ").append(p.desc);
				}
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("projects", rows);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
