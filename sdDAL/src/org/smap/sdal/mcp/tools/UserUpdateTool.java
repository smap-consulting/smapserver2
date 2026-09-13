package org.smap.sdal.mcp.tools;

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
 * Correct somebody's name or email address.
 *
 * Details only, and the word is doing real work.  What a person is allowed to do, which projects
 * they belong to and which roles they hold are all left exactly as they were - not because this tool
 * politely declines to touch them, but because it uses a manager method that cannot.
 *
 * The ordinary user update cannot be used here.  It finishes by rewriting the user's groups, projects
 * and roles from the object it was given, so handing it one that carries no lists does not leave them
 * alone: it deletes them.  Correcting a misspelt surname would have removed somebody's access to
 * everything and reported success.
 *
 * The username is not changeable either.  It is what the person signs in with and what every log
 * entry is written against, so changing it from here would quietly detach a person from their own
 * history.
 */
public class UserUpdateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "user_update";
	}

	@Override
	public String getTitle() {
		return "Correct a user's details";
	}

	@Override
	public String getDescription() {
		return "Changes a person's display name or email address. What they are allowed to do, the "
				+ "projects they belong to and the roles they hold are untouched, and their username "
				+ "cannot be changed here because it is what they sign in with and what their "
				+ "history is recorded against. Returns the previous values.";
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
		return "user_update again with the previous details, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"username", property("string", "The person to correct, from user_list"),
				"name", property("string", "Optional. Their display name."),
				"email", property("string", "Optional. Their email address."));
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

		/*
		 * Found in this organisation's own list rather than by ident alone, which would reach a user
		 * anywhere on the server.
		 */
		UserManager um = new UserManager(ctx.localisation);
		User found = null;
		ArrayList<User> users = um.getUserList(ctx.sd, oId, false, false, true, ctx.user);
		for(User u : users) {
			if(username.equalsIgnoreCase(u.ident)) {
				found = u;
				break;
			}
		}
		if(found == null) {
			return new MCPToolResult("There is no user called " + username + " in this "
					+ "organisation. user_list shows who there is.", true);
		}

		String name = stringArg(arguments, "name");
		String email = stringArg(arguments, "email");
		if(name == null && email == null) {
			return new MCPToolResult("Give a name or an email address to change.", true);
		}
		/*
		 * Whatever was not named keeps what it had.  The update writes both columns, so the current
		 * values are read first - the same reason projects and survey settings do it.
		 */
		if(name == null) {
			name = found.name;
		}
		if(email == null) {
			email = found.email;
		}
		name = name == null ? null : name.trim();
		email = email == null ? null : email.trim();

		if(name == null || name.isEmpty()) {
			return new MCPToolResult("A name cannot be empty.", true);
		}
		if(email != null && !email.isEmpty() && !email.matches("[^@\\s]+@[^@\\s]+\\.[^@\\s]+")) {
			return new MCPToolResult("\"" + email + "\" is not an email address.", true);
		}

		Map<String, Object> previous = new LinkedHashMap<>();
		previous.put("name", found.name);
		previous.put("email", found.email);

		um.updateUserDetails(ctx.sd, found.id, name, email, ctx.user);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("username", found.ident);
		data.put("name", name);
		data.put("email", email);
		data.put("previous", previous);

		StringBuilder text = new StringBuilder();
		text.append("Updated ").append(found.ident).append(".");
		if(!name.equals(found.name)) {
			text.append("\n- name: ").append(found.name).append(" to ").append(name);
		}
		if(email != null && !email.equals(found.email)) {
			text.append("\n- email: ")
					.append(found.email == null || found.email.isEmpty() ? "(none)" : found.email)
					.append(" to ").append(email);
		}
		text.append("\n\nTheir groups, projects and roles are unchanged, and so is the username they "
				+ "sign in with.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
