package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.User;

/*
 * Remove somebody from this server.
 *
 * Most of this tool is refusing to pretend the change is reversible, because for most people it is
 * not.  Smap only softens the delete for somebody who belongs to several organisations, and then it
 * simply takes them out of this one.  For anybody in a single organisation - which is nearly
 * everybody - it is:
 *
 *     delete from users where id = ? and o_id = ?
 *
 * followed by deleting their media directory off disk.  The row goes, what cascades from it goes, and
 * the files are gone.  There is no undo, and the answer says so before the fact rather than after.
 *
 * Which of the two happens is not a choice made here, it is decided inside deleteUser by counting the
 * organisations somebody belongs to.  So this counts them first and **tells the caller which delete
 * they are about to get**, because "removed from this organisation" and "erased" are different
 * enough that nobody should find out afterwards.
 *
 * It never deletes across organisations.  deleteUser takes a flag for that and it is always false
 * here: reaching into organisations this caller is not administering is not something a tool should
 * offer at any level of confirmation.
 *
 * What is usually wanted instead is in the answer for the case where it is refused, and worth saying
 * here too: user_set_groups to nothing and user_set_projects to nothing leaves an account that can do
 * nothing and can be put straight back.
 */
public class UserDeleteTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "user_delete";
	}

	@Override
	public String getTitle() {
		return "Remove a user";
	}

	@Override
	public String getDescription() {
		return "Removes somebody. For a user who belongs only to this organisation this is "
				+ "PERMANENT - their account, what belongs to it and their uploaded files are "
				+ "deleted and cannot be recovered. Somebody who is in several organisations is only "
				+ "removed from this one. Consider user_set_groups with an empty list instead, which "
				+ "leaves an account that can do nothing and can be undone. Somebody has to agree to "
				+ "this.";
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
		return "none - for a user in one organisation nothing about this can be undone";
	}

	@Override
	public String getSelfProtectedArgument() {
		return "username";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getAnnotations() {
		Map<String, Object> a = super.getAnnotations();
		a.put("destructiveHint", Boolean.TRUE);
		return a;
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"username", property("string", "The person to remove, from user_list"),
				"acknowledge", property("object",
						"Only when your client cannot show an approval prompt. After the person has "
								+ "agreed, call again with {\"user\": \"<their username>\"}."));
		schema.put("required", new String[] { "username" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String username = stringArg(arguments, "username");
		if(username == null || username.trim().isEmpty()) {
			return new MCPToolResult("A username is required. Use user_list to find one.", true);
		}
		username = username.trim();

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
		 * A server owner is not removed from here.  Nothing grants that group through user
		 * administration either, so deleting the account would be a way round a rule the rest of the
		 * system takes care to enforce.
		 */
		if(GeneralUtilityMethods.hasSecurityGroup(ctx.sd, found.ident, Authorise.OWNER_ID)) {
			return new MCPToolResult(found.ident + " is a server owner. Removing one is not offered "
					+ "here - nothing grants that group through user administration either, and "
					+ "deleting the account would be a way around that.", true);
		}

		/*
		 * Which of the two deletes is about to happen.  Counted the same way deleteUser counts it,
		 * because the caller is entitled to know which one they are agreeing to.
		 */
		int organisations = 0;
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select count(*) from user_organisation where u_id = ?")) {
			pstmt.setInt(1, found.id);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				organisations = rs.getInt(1);
			}
		}
		boolean permanent = organisations <= 1;

		int tasks = 0;
		try {
			tasks = um.getTasksCount(ctx.sd, ctx.cResults, ctx.localisation, found.ident);
		} catch (Exception e) {
			tasks = -1;			// Not knowing is reported as not knowing
		}

		List<String> projects = new ArrayList<>();
		if(found.projects != null) {
			for(org.smap.sdal.model.Project p : found.projects) {
				projects.add(p.name);
			}
		}

		if(refused(ctx)) {
			return new MCPToolResult("Left alone. " + found.ident + " still has their account.",
					false);
		}
		if(!approved(ctx)) {
			StringBuilder what = new StringBuilder();
			if(permanent) {
				what.append("**Permanently delete ").append(found.ident).append(" (")
						.append(found.name).append(")?**\n\n")
						.append("They belong only to this organisation, so this deletes the account "
								+ "itself, everything attached to it, and their uploaded files from "
								+ "disk. None of it can be recovered.");
			} else {
				what.append("Remove ").append(found.ident).append(" (").append(found.name)
						.append(") from this organisation?\n\n")
						.append("They belong to ").append(organisations)
						.append(" organisations, so the account survives and they keep the others. ")
						.append("They lose everything here.");
			}
			if(tasks > 0) {
				what.append("\n\nThey have ").append(tasks)
						.append(tasks == 1 ? " task or case" : " tasks or cases")
						.append(" assigned. Reassign ")
						.append(tasks == 1 ? "it" : "them").append(" first if it matters.");
			} else if(tasks < 0) {
				what.append("\n\nTheir assigned tasks could not be counted.");
			}
			if(!projects.isEmpty()) {
				what.append("\nThey are in ").append(String.join(", ", projects)).append(".");
			}
			what.append("\n\nIf what you want is that they can no longer do anything, "
					+ "user_set_groups with an empty list does that and can be undone.");

			if(ctx.canElicit()) {
				return ask(ctx, what.toString());
			}
			Object ackArg = arguments.get("acknowledge");
			String claimed = null;
			if(ackArg instanceof Map) {
				Object u = ((Map<?, ?>) ackArg).get("user");
				claimed = u == null ? null : u.toString().trim();
			}
			if(claimed == null || !claimed.equalsIgnoreCase(found.ident)) {
				return new MCPToolResult(what
						+ "\n\nThis client cannot ask you to approve that mid-request. Put it to the "
						+ "person responsible, and if they agree call again with acknowledge set to "
						+ "{\"user\": \"" + found.ident + "\"}.", true);
			}
		}

		String ident = found.ident;
		String name = found.name;

		/*
		 * deleteAll false, always.  The flag reaches into every organisation the person belongs to,
		 * including ones this caller does not administer.
		 */
		um.deleteUser(ctx.sd, ctx.user, GeneralUtilityMethods.getBasePath(ctx.request),
				found.id, oId, false);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("deleted", Boolean.TRUE);
		data.put("username", ident);
		data.put("permanent", permanent);
		data.put("organisations", organisations);
		data.put("tasksAssigned", tasks);

		StringBuilder text = new StringBuilder();
		if(permanent) {
			text.append("Deleted ").append(ident).append(" (").append(name)
					.append(") permanently. The account, what belonged to it and their uploaded "
							+ "files are gone, and cannot be brought back.");
		} else {
			text.append("Removed ").append(ident).append(" (").append(name)
					.append(") from this organisation. Their account still exists in the ")
					.append(organisations - 1)
					.append(organisations - 1 == 1 ? " other organisation" : " other organisations")
					.append(" they belong to.");
		}
		if(tasks > 0) {
			text.append("\n\nThey had ").append(tasks)
					.append(tasks == 1 ? " task or case" : " tasks or cases")
					.append(" assigned. Worth checking what became of ")
					.append(tasks == 1 ? "it" : "them").append(".");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
