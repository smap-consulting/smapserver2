package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
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
 * Remove a project.
 *
 * Smap refuses to delete a project that still holds surveys, which is what makes this safe enough to
 * offer at all: the failure mode is not "a project quietly took its contents with it" but a refusal
 * naming what is still in there.  That check is Smap's, not this tool's, and it is the reason the
 * reversal for project_create can honestly be "delete it while it is still empty".
 *
 * The refusal is reported as the answer it is.  A project with surveys in it is not an error in the
 * call; it is the server saying move those first, and survey_set_settings is how.
 */
public class ProjectDeleteTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "project_delete";
	}

	@Override
	public String getTitle() {
		return "Delete a project";
	}

	@Override
	public String getDescription() {
		return "Removes a project. A project that still holds surveys will not be deleted - move "
				+ "them elsewhere first with survey_set_settings, or delete them. Use this to tidy "
				+ "up a project that was made by mistake.";
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
		return "project_create makes it again, though its surveys have to be moved back";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"project_id", property("integer", "The project to remove, from project_list"));
		schema.put("required", new String[] { "project_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int projectId = intArg(arguments, "project_id", 0);
		if(projectId <= 0) {
			return new MCPToolResult("A project_id is required. Use project_list to find one.", true);
		}

		/*
		 * Found among the organisation's own projects.  project_list shows only those the caller is
		 * a member of, and an administrator may well be deleting one they never joined, so this
		 * looks wider - but still only within their organisation.
		 */
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

		ArrayList<Project> toDelete = new ArrayList<>();
		toDelete.add(found);

		try {
			pm.deleteProjects(ctx.sd, ctx.cResults,
					new Authorise(null, Authorise.ADMIN),
					toDelete,
					ctx.user,
					GeneralUtilityMethods.getBasePath(ctx.request));
		} catch (Exception e) {
			/*
			 * Smap refuses a project that still has surveys, and that refusal is the answer rather
			 * than a failure of the call.
			 */
			return new MCPToolResult("\"" + found.name + "\" was not deleted: "
					+ (e.getMessage() == null ? "it could not be removed" : e.getMessage())
					+ "\n\nMove its surveys to another project with survey_set_settings, or delete "
					+ "them, and then try again.", true);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("project_id", projectId);
		data.put("name", found.name);

		MCPToolResult result = new MCPToolResult("Deleted project \"" + found.name + "\".");
		result.setStructuredContent(data);
		return result;
	}
}
