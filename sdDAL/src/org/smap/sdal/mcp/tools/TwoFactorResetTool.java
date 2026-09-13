package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.TwoFactorManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.TwoFactorStatus;

/*
 * Clear somebody's two factor authentication so they can sign in and set it up again.
 *
 * This is the lost phone tool.  Without it a user whose authenticator is gone has no way back in at
 * all, which is why it exists in the console; offering it here is offering the same help through the
 * same rules, and the manager logs it against both people either way.
 *
 * Two things are deliberately narrower here than in the console.
 *
 * It will not reset the caller's own.  In the console that is harmless, because reaching the console
 * already meant passing the second factor.  Here it would not be: a token is a bearer credential,
 * and an MCP client that can clear the second factor on the account it is acting as has removed the
 * protection that the token being stolen was supposed to run into.  Somebody who has genuinely lost
 * their own phone is reset by another administrator, or in the console.
 *
 * And it asks first.  Everything else that needs approval here is about data; this is about
 * authentication, where the person who should be agreeing is a person, not a client with a token.
 * Where the client cannot put a question to anybody - which is most of them, including the one this
 * was first tested from - the caller names the person back instead.  That is a weaker thing and the
 * answer says so: it shows the call was meant, not that somebody agreed.  Refusing outright was the
 * alternative, and it left a tool that could never be used from the client people actually have.
 */
public class TwoFactorResetTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "two_factor_reset";
	}

	@Override
	public String getTitle() {
		return "Reset two factor authentication";
	}

	@Override
	public String getDescription() {
		return "Clears another user's two factor authentication, for somebody who has lost the "
				+ "phone or authenticator app holding it. They sign in with their password alone "
				+ "until they enrol again, so tell them to set it up afresh. You cannot reset your "
				+ "own from here. Somebody has to agree to it first, so ask before calling.";
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
		return "none - the person enrols again from their own security settings";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"username", property("string", "The person to reset, from user_list"),
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

		if(username.equalsIgnoreCase(ctx.user)) {
			return new MCPToolResult("This will not reset your own two factor authentication. Ask "
					+ "another administrator, or do it in the console, where you have already "
					+ "passed the second factor to get there.", true);
		}

		int callerOrg = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		int uId = GeneralUtilityMethods.getUserId(ctx.sd, username);
		if(uId <= 0 || GeneralUtilityMethods.getOrganisationId(ctx.sd, username) != callerOrg) {
			return new MCPToolResult("There is no user called " + username + " in this "
					+ "organisation. user_list shows who there is.", true);
		}

		TwoFactorManager tfm = new TwoFactorManager(ctx.localisation);
		TwoFactorStatus status = tfm.getStatus(ctx.sd, username);

		if(!status.enabled && !status.pending) {
			return new MCPToolResult(username + " has no two factor authentication set up, so there "
					+ "is nothing to reset. They sign in with their password already.", false);
		}

		if(refused(ctx)) {
			return new MCPToolResult("Left alone. " + username + " still has two factor "
					+ "authentication.", false);
		}
		if(!approved(ctx)) {
			String what = "Clear two factor authentication for " + username + "?\n\n"
					+ "They will sign in with their password alone until they enrol again, so this "
					+ "is worth doing only when you know it is really them asking. It is logged "
					+ "against both of you.";
			if(ctx.canElicit()) {
				return ask(ctx, what);
			}
			/*
			 * A client that cannot put the question to anybody would otherwise leave this tool
			 * permanently out of reach, which is what happened: the confirmation was built and then
			 * nothing could get past it.  So the caller names the person back instead.
			 *
			 * This is weaker and worth saying so.  It is evidence that the call was meant rather than
			 * evidence that somebody agreed - the same trade data_bulk_update makes, for the same
			 * reason.  What stands behind it is not the acknowledgement: it is that this cannot touch
			 * the caller's own account, cannot reach outside their organisation, and is written to
			 * the log against both people whatever happens.
			 */
			Object ackArg = arguments.get("acknowledge");
			String claimed = null;
			if(ackArg instanceof Map) {
				Object u = ((Map<?, ?>) ackArg).get("user");
				claimed = u == null ? null : u.toString().trim();
			}
			if(claimed == null || !claimed.equalsIgnoreCase(username)) {
				return new MCPToolResult(what
						+ "\n\nThis client cannot ask you to approve that mid-request. Put it to "
						+ "the person responsible, and if they agree call again with acknowledge set "
						+ "to {\"user\": \"" + username + "\"}.", true);
			}
		}

		tfm.reset(ctx.sd, ctx.user, username, callerOrg);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("reset", Boolean.TRUE);
		data.put("username", username);
		data.put("wasEnrolled", status.enabled);

		StringBuilder text = new StringBuilder();
		text.append("Cleared two factor authentication for ").append(username).append(".");
		if(status.pending && !status.enabled) {
			text.append(" They had only started enrolling, so nothing was in use.");
		}
		text.append("\n\nThey sign in with their password alone now. Tell them to set it up again "
				+ "from their own security settings. This was logged against both of you.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
