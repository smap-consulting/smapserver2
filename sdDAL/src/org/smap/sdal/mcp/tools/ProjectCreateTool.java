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
 * Make a project.
 *
 * A project is the unit access is granted in: surveys live in one, tasks belong to one, and being a
 * member of it is what lets somebody reach any of that.
 *
 * The person creating it is made a member, by createProject itself - the same manager method the
 * console calls, so a project made here and one made in the user interface end up the same.  Adding
 * anybody *else* is not offered: widening who can reach what is the change that lets every other
 * change happen unnoticed, and it belongs behind its own permission rather than arriving with the
 * ability to make a folder.
 */
public class ProjectCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "project_create";
	}

	@Override
	public String getTitle() {
		return "Create a project";
	}

	@Override
	public String getDescription() {
		return "Creates a project: the folder surveys and tasks live in, and the unit people are "
				+ "given access by. You are added to it, as you would be creating one in the "
				+ "console; adding anyone else is done there. Move surveys into it with "
				+ "survey_set_settings.";
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
		return "project_delete removes it while it is still empty";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"name", property("string", "What the project is called"),
				"description", property("string", "Optional. What it is for."));
		schema.put("required", new String[] { "name" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required.", true);
		}
		name = name.trim();

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		int uId = GeneralUtilityMethods.getUserId(ctx.sd, ctx.user);

		/*
		 * A name already in use is refused here rather than left to the database, which would answer
		 * with a constraint violation naming a column.
		 */
		ProjectManager pm = new ProjectManager(ctx.localisation);
		for(Project existing : pm.getProjects(ctx.sd, ctx.user, true, false, null, false, false)) {
			if(name.equalsIgnoreCase(existing.name)) {
				return new MCPToolResult("There is already a project called \"" + existing.name
						+ "\" in this organisation.", true);
			}
		}

		Project p = new Project();
		p.name = name;
		p.desc = stringArg(arguments, "description");

		int pId = pm.createProject(ctx.sd, ctx.user, p, oId, uId, ctx.user);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("project_id", pId);
		data.put("name", name);
		data.put("members", 1);
		data.put("memberIsCaller", Boolean.TRUE);

		MCPToolResult result = new MCPToolResult("Created project \"" + name + "\" (" + pId + ").\n\n"
				+ "You are a member of it, as you would be if you had made it in the console. "
				+ "Nobody else is - adding other people to a project is done there. Surveys are "
				+ "moved into it with survey_set_settings.");
		result.setStructuredContent(data);
		return result;
	}
}
