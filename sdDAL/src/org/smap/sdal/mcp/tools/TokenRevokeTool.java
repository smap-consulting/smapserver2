package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.OAuthManager;
import org.smap.sdal.managers.OAuthTokenManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * Withdraw an application's access.
 *
 * Revokes every live token it holds for that person and forgets the remembered consent, so it has to
 * ask from the beginning rather than quietly picking up where it left off.  Both halves matter: the
 * tokens alone would let a remembered consent hand out a new one without anybody being asked.
 *
 * It takes effect at once.  A token is checked on every request, so an application loses access
 * mid-conversation rather than at the end of one - which is the point, since this is the tool
 * somebody reaches for when an application is doing something they did not intend.
 *
 * **This can be used on the connection making the call.** Revoking the client you are speaking
 * through ends the conversation, and the answer says so before it is too late to matter.  It is
 * allowed because forbidding it would be worse: the one application somebody most urgently needs to
 * stop is the one currently doing something wrong.
 */
public class TokenRevokeTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "token_revoke";
	}

	@Override
	public String getTitle() {
		return "Withdraw AI access";
	}

	@Override
	public String getDescription() {
		return "Withdraws an application's access for one person: every token it holds stops "
				+ "working immediately and the remembered permission is forgotten, so it has to ask "
				+ "again from the beginning. Use token_list to find the application. If you revoke "
				+ "the application you are speaking through, this conversation ends.";
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
		return "none directly - the person authorises the application again, which is the point";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.SECURITY, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"client_id", property("string",
						"The application to withdraw, from token_list"),
				"username", property("string",
						"Optional. Whose access to withdraw. Defaults to your own."));
		schema.put("required", new String[] { "client_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String clientId = stringArg(arguments, "client_id");
		if(clientId == null || clientId.trim().isEmpty()) {
			return new MCPToolResult("A client_id is required. Use token_list to find one.", true);
		}
		clientId = clientId.trim();

		String username = stringArg(arguments, "username");
		if(username == null || username.trim().isEmpty()) {
			username = ctx.user;
		}
		username = username.trim();

		/*
		 * Withdrawing somebody else's access is a security manager's act, not an ordinary
		 * administrator's - the same rule that decides whose grants token_list shows.
		 */
		if(!username.equalsIgnoreCase(ctx.user)) {
			boolean canActForOthers =
					GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.SECURITY_ID)
					|| GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ORG_ID)
					|| GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.OWNER_ID);
			if(!canActForOthers) {
				return new MCPToolResult("You can only withdraw applications acting as yourself. "
						+ "Withdrawing somebody else's needs a security manager, an organisation "
						+ "administrator or a server owner.", true);
			}
			/* And only within this organisation */
			int callerOrg = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
			int targetOrg = GeneralUtilityMethods.getOrganisationId(ctx.sd, username);
			if(targetOrg <= 0 || targetOrg != callerOrg) {
				return new MCPToolResult("There is no user called " + username + " in this "
						+ "organisation.", true);
			}
		}

		int uId = GeneralUtilityMethods.getUserId(ctx.sd, username);
		if(uId <= 0) {
			return new MCPToolResult("There is no user called " + username + ".", true);
		}

		boolean itsMe = clientId.equals(ctx.clientId) && username.equalsIgnoreCase(ctx.user);

		OAuthTokenManager tm = new OAuthTokenManager();
		int revoked = tm.revokeForUserAndClient(ctx.sd, uId, clientId, ctx.user);
		new OAuthManager().forgetConsent(ctx.sd, uId, clientId);

		if(revoked == 0) {
			return new MCPToolResult("That application had no live access as " + username
					+ ", so nothing was withdrawn. Any remembered permission has been forgotten "
					+ "anyway, so it will ask again if it comes back.", false);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("revoked", Boolean.TRUE);
		data.put("client_id", clientId);
		data.put("user", username);
		data.put("tokensStopped", revoked);
		data.put("wasThisConnection", itsMe);

		StringBuilder text = new StringBuilder();
		text.append("Withdrew that application's access as ").append(username).append(". ")
				.append(revoked).append(" token(s) stopped working, and the remembered permission "
						+ "is forgotten, so it has to ask again from the beginning.");
		if(itsMe) {
			text.append("\n\n**That was the application you are speaking through.** This "
					+ "conversation has just lost its access and will need authorising again.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
