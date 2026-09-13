package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Role;

/*
 * Make a role.
 *
 * A role on its own does nothing at all - it filters no records and nobody holds it.  It becomes real
 * when somebody is given it (user_set_roles) and when it is attached to a survey with a row filter,
 * which is done in the console.  Saying that plainly matters: a caller that makes a role and stops
 * has not restricted anything, and might reasonably believe it has.
 */
public class RoleCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "role_create";
	}

	@Override
	public String getTitle() {
		return "Create a role";
	}

	@Override
	public String getDescription() {
		return "Makes a new role. A role does nothing until somebody holds it and it is attached to "
				+ "a survey with a row filter - the filter is set in the console. Use role_list to "
				+ "see what exists.";
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
		return "role_delete, while nobody holds it and it filters nothing";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ORG, Authorise.SECURITY, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"name", property("string", "What the role is called"),
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

		Role r = new Role();
		r.name = name;
		r.desc = stringArg(arguments, "description");
		r.users = null;

		RoleManager rm = new RoleManager(ctx.localisation);
		int rId = rm.createRole(ctx.sd, r, oId, ctx.user, false);

		/*
		 * createRole does nothing at all when the name is taken, and returns zero.  Reported as the
		 * refusal it is rather than as a success with no role behind it.
		 */
		if(rId <= 0) {
			return new MCPToolResult("There is already a role called \"" + name + "\" in this "
					+ "organisation. role_list shows what there is.", true);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("role_id", rId);
		data.put("name", name);

		MCPToolResult result = new MCPToolResult("Created the role \"" + name + "\".\n\n"
				+ "It does nothing yet. Nobody holds it, and it is not attached to any survey, so it "
				+ "filters no records. Give it to somebody with user_set_roles, and set its row "
				+ "filter against a survey in the console.");
		result.setStructuredContent(data);
		return result;
	}
}
