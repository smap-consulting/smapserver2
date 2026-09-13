package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Project;

/*
 * Rename a project, or change what it says it is for.
 *
 * Only those two things.  The console's own project save updates the name and then removes every
 * member of the project so it can re-add whoever was listed on the form - which is right for a screen
 * where the membership was on display and is being submitted back, and would be silent destruction
 * here, where nobody mentioned members at all.  So this uses a narrower manager method that leaves
 * membership alone, and says so.
 */
public class ProjectUpdateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "project_update";
	}

	@Override
	public String getTitle() {
		return "Rename a project";
	}

	@Override
	public String getDescription() {
		return "Changes a project's name or description. Who belongs to the project is untouched, "
				+ "and so is everything in it. Returns the previous name and description so the "
				+ "change can be put back.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ADMIN;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "project_update again with the previous name, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"project_id", property("integer", "The project to change, from project_list"),
				"name", property("string", "Optional. What it should be called."),
				"description", property("string", "Optional. What it is for."));
		schema.put("required", new String[] { "project_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int projectId = intArg(arguments, "project_id", 0);
		if(projectId <= 0) {
			return new MCPToolResult("A project_id is required. Use project_list to find one.", true);
		}

		ProjectManager pm = new ProjectManager(ctx.localisation);
		Project found = null;
		for(Project p : pm.getProjects(ctx.sd, ctx.user, true, false, null, false, false)) {
			if(p.id == projectId) {
				found = p;
				break;
			}
		}
		if(found == null) {
			return new MCPToolResult("No such project in this organisation.", true);
		}

		/*
		 * Anything not named keeps what it had.  The update writes both columns, so the current
		 * values are read first - passing a half filled object would blank the description of a
		 * project somebody only meant to rename.
		 */
		String name = stringArg(arguments, "name");
		String description = stringArg(arguments, "description");
		if(name == null && description == null) {
			return new MCPToolResult("Give a name or a description to change.", true);
		}
		if(name == null) {
			name = found.name;
		}
		name = name.trim();
		if(name.isEmpty()) {
			return new MCPToolResult("A project cannot have an empty name.", true);
		}
		if(description == null) {
			description = found.desc;
		}

		if(!name.equalsIgnoreCase(found.name)) {
			for(Project p : pm.getProjects(ctx.sd, ctx.user, true, false, null, false, false)) {
				if(p.id != projectId && name.equalsIgnoreCase(p.name)) {
					return new MCPToolResult("There is already a project called \"" + p.name
							+ "\" in this organisation.", true);
				}
			}
		}

		Map<String, Object> previous = new LinkedHashMap<>();
		previous.put("name", found.name);
		previous.put("description", found.desc);

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		pm.updateProject(ctx.sd, projectId, name, description, ctx.user, oId);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("project_id", projectId);
		data.put("name", name);
		data.put("description", description);
		data.put("previous", previous);

		StringBuilder text = new StringBuilder();
		if(!name.equals(found.name)) {
			text.append("Renamed \"").append(found.name).append("\" to \"").append(name).append("\".");
		} else {
			text.append("Updated \"").append(name).append("\".");
		}
		text.append(" Its members and everything in it are unchanged.");
		text.append("\n\nIt was: ").append(found.name);
		if(found.desc != null && !found.desc.isEmpty()) {
			text.append(" - ").append(found.desc);
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
