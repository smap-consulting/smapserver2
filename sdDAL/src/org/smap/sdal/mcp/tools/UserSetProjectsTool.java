package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.User;

/*
 * Which projects somebody belongs to.
 *
 * Project membership is what makes a survey reachable at all - not a group, not a role.  Somebody
 * who cannot see a survey is almost always not in its project, and this is the tool that fixes it.
 *
 * **It replaces the list rather than adding to it.**  That is Smap's own shape and it is stated
 * everywhere it could be misread, because the failure is silent: sending the one project somebody
 * should join removes every other project they were in, and nothing about the answer would look
 * wrong.  So the current list is read first, the answer names what was added and what was removed,
 * and the previous list comes back for putting straight.
 *
 * It cannot be pointed at the caller.  The dispatcher refuses that before this runs.
 */
public class UserSetProjectsTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "user_set_projects";
	}

	@Override
	public String getTitle() {
		return "Set a user's projects";
	}

	@Override
	public String getDescription() {
		return "Sets which projects somebody belongs to, which is what decides the surveys they can "
				+ "reach. THIS REPLACES THE WHOLE LIST - projects left out are removed, so include "
				+ "everything they should keep. user_list shows what they have now. Returns the "
				+ "previous list, and what changed. You cannot change your own.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ACCESS;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "user_set_projects again with the previous list, which this returns";
	}

	@Override
	public String getSelfProtectedArgument() {
		return "username";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> projects = property("array",
				"The complete list of project names they should belong to. An empty list removes "
						+ "them from every project.");
		projects.put("items", property("string", "A project name, from project_list"));

		Map<String, Object> schema = schema(
				"username", property("string", "The person to change, from user_list"),
				"projects", projects);
		schema.put("required", new String[] { "username", "projects" });
		return schema;
	}

	@SuppressWarnings("unchecked")
	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String username = stringArg(arguments, "username");
		if(username == null || username.trim().isEmpty()) {
			return new MCPToolResult("A username is required. Use user_list to find one.", true);
		}
		username = username.trim();

		Object wanted = arguments.get("projects");
		if(!(wanted instanceof List)) {
			return new MCPToolResult("projects has to be a list of project names - the complete "
					+ "list they should belong to, not just the ones to add.", true);
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		UserManager um = new UserManager(ctx.localisation);

		User found = null;
		for(User u : um.getUserList(ctx.sd, oId, false, false, true, ctx.user)) {
			if(username.equalsIgnoreCase(u.ident)) {
				found = u;
				break;
			}
		}
		if(found == null) {
			return new MCPToolResult("There is no user called " + username + " in this "
					+ "organisation. user_list shows who there is.", true);
		}

		/*
		 * Named projects resolved against this organisation's own list.  A project id would have been
		 * shorter to type and is not offered: an id from another organisation is a number that looks
		 * right, and a name that does not exist here is refused by name.
		 */
		List<Project> orgProjects = new ProjectManager(ctx.localisation)
				.getProjects(ctx.sd, ctx.user, true, false, null, false, false);

		ArrayList<Project> toSet = new ArrayList<>();
		List<String> unknown = new ArrayList<>();
		List<String> names = new ArrayList<>();
		for(Object o : (List<Object>) wanted) {
			if(o == null) {
				continue;
			}
			String name = o.toString().trim();
			if(name.isEmpty()) {
				continue;
			}
			Project match = null;
			for(Project p : orgProjects) {
				if(name.equalsIgnoreCase(p.name)) {
					match = p;
					break;
				}
			}
			if(match == null) {
				unknown.add(name);
			} else if(!names.contains(match.name)) {
				toSet.add(match);
				names.add(match.name);
			}
		}
		if(!unknown.isEmpty()) {
			return new MCPToolResult("This organisation has no project called "
					+ String.join(", ", unknown) + ". project_list shows what there is. Nothing was "
					+ "changed.", true);
		}

		List<String> before = new ArrayList<>();
		if(found.projects != null) {
			for(Project p : found.projects) {
				before.add(p.name);
			}
		}

		List<String> added = new ArrayList<>();
		List<String> removed = new ArrayList<>();
		for(String n : names) {
			if(!before.contains(n)) {
				added.add(n);
			}
		}
		for(String n : before) {
			if(!names.contains(n)) {
				removed.add(n);
			}
		}

		if(added.isEmpty() && removed.isEmpty()) {
			return new MCPToolResult(found.ident + " is already in exactly those projects, so "
					+ "nothing was changed.", false);
		}

		um.setUserProjects(ctx.sd, found.id, toSet,
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ORG_ID),
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.SECURITY_ID),
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ENTERPRISE_ID),
				GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.OWNER_ID));

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("username", found.ident);
		data.put("projects", names);
		data.put("added", added);
		data.put("removed", removed);
		data.put("previous", before);

		StringBuilder text = new StringBuilder();
		text.append(found.ident).append(" is now in ")
				.append(names.isEmpty() ? "no projects" : String.join(", ", names)).append(".");
		if(!added.isEmpty()) {
			text.append("\n- added: ").append(String.join(", ", added));
		}
		if(!removed.isEmpty()) {
			/*
			 * Said as a consequence rather than as a list.  Somebody removed from a project loses
			 * the surveys in it, and that is the half of this change nobody asked for out loud.
			 */
			text.append("\n- removed: ").append(String.join(", ", removed))
					.append(" - they can no longer reach the surveys in ")
					.append(removed.size() == 1 ? "it" : "those");
		}
		text.append("\n\nThey were in: ")
				.append(before.isEmpty() ? "no projects" : String.join(", ", before));
		text.append("\nTheir groups and roles are unchanged.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
