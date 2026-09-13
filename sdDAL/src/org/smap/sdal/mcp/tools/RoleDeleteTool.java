package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
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
 * Remove a role.
 *
 * This changes what people can see, and not always in the direction somebody expects.  A role carries
 * a row filter, so deleting it can widen what its holders see - the filter that was narrowing them is
 * gone.  But on a survey where roles are what grant access at all, deleting the only role its people
 * held takes their access away entirely.  Which of those happens depends on the survey, so the answer
 * reports what the role was attached to rather than guessing.
 *
 * It asks first, and it says who holds it, because that number is the size of the change and nobody
 * asking to tidy up an unused role expects it to be nine.
 */
public class RoleDeleteTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "role_delete";
	}

	@Override
	public String getTitle() {
		return "Delete a role";
	}

	@Override
	public String getDescription() {
		return "Removes a role, its row filters, and it from everybody who holds it. This changes "
				+ "what those people can see - on some surveys they will see more, on surveys where "
				+ "a role is what grants access they will see nothing. Check role_list first. "
				+ "Somebody has to agree to it.";
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
		return "none - role_create makes a role of the same name, but its filters and its holders "
				+ "are gone and have to be set again";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ORG, Authorise.SECURITY, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"role", property("string", "The role to remove, from role_list"),
				"acknowledge", property("object",
						"Only when your client cannot show an approval prompt. After the person has "
								+ "agreed, call again with {\"role\": \"<the role name>\"}."));
		schema.put("required", new String[] { "role" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String role = stringArg(arguments, "role");
		if(role == null || role.trim().isEmpty()) {
			return new MCPToolResult("A role is required. Use role_list to find one.", true);
		}
		role = role.trim();

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		int rId = 0;
		String name = null;
		int holders = 0;
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select r.id, r.name, "
				+ "(select count(*) from user_role ur where ur.r_id = r.id) as holders "
				+ "from role r where r.o_id = ? and r.name = ?")) {
			pstmt.setInt(1, oId);
			pstmt.setString(2, role);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				rId = rs.getInt(1);
				name = rs.getString(2);
				holders = rs.getInt(3);
			}
		}
		if(rId <= 0) {
			return new MCPToolResult("There is no role called \"" + role + "\" in this "
					+ "organisation. role_list shows what there is.", true);
		}

		List<String> surveys = new ArrayList<>();
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select coalesce(s.display_name, sr.survey_ident) "
				+ "from survey_role sr left join survey s on s.ident = sr.survey_ident "
				+ "where sr.r_id = ?")) {
			pstmt.setInt(1, rId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				surveys.add(rs.getString(1));
			}
		}

		if(refused(ctx)) {
			return new MCPToolResult("Left alone. \"" + name + "\" still exists.", false);
		}
		if(!approved(ctx)) {
			StringBuilder what = new StringBuilder("Delete the role \"").append(name).append("\"?");
			what.append("\n\nIt is held by ").append(holders)
					.append(holders == 1 ? " person" : " people").append(".");
			if(surveys.isEmpty()) {
				what.append(" It is not attached to any survey, so it filters nothing and deleting "
						+ "it changes what nobody sees.");
			} else {
				what.append(" It filters records on ").append(String.join(", ", surveys))
						.append(". Deleting it changes what those ")
						.append(holders == 1 ? "person sees" : "people see")
						.append(" there - more, or nothing at all, depending on whether the role is "
								+ "what grants them access.");
			}
			what.append("\n\nThe filters and the holders cannot be put back by making the role "
					+ "again.");

			if(ctx.canElicit()) {
				return ask(ctx, what.toString());
			}
			Object ackArg = arguments.get("acknowledge");
			String claimed = null;
			if(ackArg instanceof Map) {
				Object r = ((Map<?, ?>) ackArg).get("role");
				claimed = r == null ? null : r.toString().trim();
			}
			if(claimed == null || !claimed.equalsIgnoreCase(name)) {
				return new MCPToolResult(what
						+ "\n\nThis client cannot ask you to approve that mid-request. Put it to the "
						+ "person responsible, and if they agree call again with acknowledge set to "
						+ "{\"role\": \"" + name + "\"}.", true);
			}
		}

		ArrayList<Role> toDelete = new ArrayList<>();
		Role r = new Role();
		r.id = rId;
		r.name = name;
		toDelete.add(r);
		new RoleManager(ctx.localisation).deleteRoles(ctx.sd, toDelete, oId, ctx.user);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("name", name);
		data.put("holdersAffected", holders);
		data.put("surveys", surveys);

		StringBuilder text = new StringBuilder();
		text.append("Deleted the role \"").append(name).append("\".");
		if(holders > 0) {
			text.append(" ").append(holders).append(holders == 1 ? " person has" : " people have")
					.append(" lost it.");
		}
		if(!surveys.isEmpty()) {
			text.append("\n\nWhat they see on ").append(String.join(", ", surveys))
					.append(" has changed. Worth checking with somebody who uses ")
					.append(surveys.size() == 1 ? "it" : "them").append(".");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
