package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.OAuthTokenManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * Which AI applications can currently act as somebody here.
 *
 * Grouped by application and person rather than listed token by token: a client holding four
 * refreshed tokens is not four grants, it is one application somebody allowed once, and the question
 * is always about the application.
 *
 * Whether the caller sees the whole organisation or only their own grants follows the same rule as
 * the console's AI access page - a security manager, an organisation administrator or a server owner
 * sees everyone, and anybody else sees themselves.  That is decided here, where the caller is known,
 * rather than inside the query.
 *
 * Worth saying what this list is: every row is an application that can act as that person, with
 * their permissions, until somebody withdraws it.  Self registered is flagged because anyone can
 * register a client - the name beside it was chosen by whoever did.
 */
public class TokenListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "token_list";
	}

	@Override
	public String getTitle() {
		return "AI access grants";
	}

	@Override
	public String getDescription() {
		return "The AI applications that can currently act as somebody on this server: which "
				+ "application, for which person, what it is allowed to do, when it was first "
				+ "allowed and when it was last used. Withdraw one with token_revoke.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ADMIN;
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.SECURITY, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	/*
	 * The scopes an application holds, each named once.
	 *
	 * The query aggregates them with a distinct over each token's whole scope STRING, not over the
	 * scopes inside it, so a client with one token allowing read and write and another allowing read,
	 * write and admin comes back as "smap:read smap:write smap:read smap:write smap:admin".  Read
	 * aloud that says the application is allowed to read twice, which is not a thing.
	 */
	private String tidyScopes(String scopes) {

		if(scopes == null || scopes.trim().isEmpty()) {
			return null;
		}
		List<String> seen = new ArrayList<>();
		for(String scope : scopes.trim().split("\\s+")) {
			if(!scope.isEmpty() && !seen.contains(scope)) {
				seen.add(scope);
			}
		}
		return String.join(" ", seen);
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		int uId = GeneralUtilityMethods.getUserId(ctx.sd, ctx.user);

		/*
		 * The same rule the console's AI access page applies.  Asked here rather than assumed,
		 * because an ordinary administrator seeing everybody's grants would be a widening nobody
		 * asked for.
		 */
		boolean orgWide = GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.SECURITY_ID)
				|| GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ORG_ID)
				|| GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.OWNER_ID);

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		for(OAuthTokenManager.Grant g : new OAuthTokenManager().getGrants(ctx.sd, oId, uId, orgWide)) {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("client_id", g.clientId);
			row.put("application", g.clientName == null ? g.clientId : g.clientName);
			row.put("user", g.user);
			row.put("allowedTo", tidyScopes(g.scopes));
			row.put("firstAllowed", g.firstIssued);
			row.put("lastUsed", g.lastUsed);
			row.put("selfRegistered", g.selfRegistered);
			rows.add(row);

			text.append("\n- ").append(g.clientName == null ? g.clientId : g.clientName)
					.append(" acting as ").append(g.user);
			String scopes = tidyScopes(g.scopes);
			if(scopes != null) {
				text.append(", allowed to ").append(scopes);
			}
			text.append("\n    last used ")
					.append(g.lastUsed == null ? "never" : g.lastUsed);
			if(g.selfRegistered) {
				text.append(", registered itself");
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append(orgWide
					? "No AI application has access to this organisation."
					: "No AI application has access as you.");
		} else {
			text.insert(0, rows.size() + (orgWide
					? " application(s) can act as somebody in this organisation:"
					: " application(s) can act as you:"));
			text.append("\n\nEach of these can do what that person can do, within what it is allowed "
					+ "to, until it is withdrawn with token_revoke. An application that registered "
					+ "itself chose its own name.");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("grants", rows);
		data.put("count", rows.size());
		data.put("wholeOrganisation", orgWide);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
