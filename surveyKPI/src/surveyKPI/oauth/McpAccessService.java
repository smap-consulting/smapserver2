package surveyKPI.oauth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.OAuthManager;
import org.smap.sdal.managers.OAuthTokenManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.MCPScope;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Managing your own MCP access from the console: which AI clients you have authorised, taking that
 * back, and minting a token for something that has no browser to authorise with.
 *
 * Deliberately NOT under /oauth.  That path is unauthenticated in the vhost, because the endpoints
 * beneath it authenticate their callers themselves; anything mounted there would inherit that.
 * These are ordinary console services and belong behind the console's own form authentication.
 */
@Path("/mcpaccess")
public class McpAccessService extends Application {

	private static Logger log = Logger.getLogger(McpAccessService.class.getName());

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	Authorise a = new Authorise(null, Authorise.MCP_ACCESS);

	/*
	 * The clients this user has given access to, and when each was last used.  A security manager
	 * sees everyone in their organisation, because taking back a departing colleague's client
	 * access is their job, not something to wait for that person to do.
	 */
	@GET
	@Path("/grants")
	@Produces(MediaType.APPLICATION_JSON)
	public Response grants(@Context HttpServletRequest request) {

		String connectionString = "surveyKPI-McpGrants";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			if(!ServerManager.isMcpEnabled(sd)) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}
			String user = request.getRemoteUser();
			a.isAuthorised(sd, request, user);

			boolean orgWide = GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.SECURITY_ID)
					|| GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.ORG_ID);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			int uId = GeneralUtilityMethods.getUserId(sd, user);

			String sql = "select t.client_id, c.client_name, c.source, u.ident, "
					+ "min(t.issued) as first_issued, max(t.last_used) as last_used, "
					+ "string_agg(distinct t.scope, ' ') as scopes, count(*) as tokens "
					+ "from oauth_token t "
					+ "inner join users u on u.id = t.u_id "
					+ "left outer join oauth_client c on c.client_id = t.client_id "
					+ "where t.revoked is null and t.client_id is not null "
					+ (orgWide ? "and t.o_id = ? " : "and t.u_id = ? ")
					+ "group by t.client_id, c.client_name, c.source, u.ident "
					+ "order by max(t.last_used) desc nulls last";

			ArrayList<Map<String, Object>> out = new ArrayList<>();
			try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
				pstmt.setInt(1, orgWide ? oId : uId);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					Map<String, Object> row = new HashMap<>();
					row.put("client_id", rs.getString("client_id"));
					row.put("client_name", rs.getString("client_name"));
					row.put("self_registered", !"preregistered".equals(rs.getString("source")));
					row.put("user", rs.getString("ident"));
					row.put("first_issued", rs.getString("first_issued"));
					row.put("last_used", rs.getString("last_used"));
					row.put("scopes", rs.getString("scopes"));
					row.put("tokens", rs.getInt("tokens"));
					out.add(row);
				}
			}
			return Response.ok(gson.toJson(out)).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "Listing mcp grants", e);
			return Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * Withdraw a client's access.  Revokes every live token and forgets the remembered consent, so
	 * the client has to ask again from the beginning rather than silently picking up where it was.
	 */
	@DELETE
	@Path("/grants/{clientId}")
	public Response revokeGrant(@Context HttpServletRequest request,
			@PathParam("clientId") String clientId,
			@jakarta.ws.rs.QueryParam("user") String targetUser) {

		String connectionString = "surveyKPI-McpRevokeGrant";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			if(!ServerManager.isMcpEnabled(sd)) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}
			String user = request.getRemoteUser();
			a.isAuthorised(sd, request, user);

			String owner = user;
			if(targetUser != null && !targetUser.equals(user)) {
				/*
				 * Revoking somebody else's access is a security manager's job, and only within
				 * their own organisation
				 */
				if(!GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.SECURITY_ID)
						&& !GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.ORG_ID)) {
					return Response.status(Response.Status.FORBIDDEN).build();
				}
				if(GeneralUtilityMethods.getOrganisationId(sd, targetUser)
						!= GeneralUtilityMethods.getOrganisationId(sd, user)) {
					return Response.status(Response.Status.FORBIDDEN).build();
				}
				owner = targetUser;
			}

			int uId = GeneralUtilityMethods.getUserId(sd, owner);
			OAuthTokenManager tm = new OAuthTokenManager();
			int revoked = tm.revokeForUserAndClient(sd, uId, clientId, user);
			new OAuthManager().forgetConsent(sd, uId, clientId);

			log.info("MCP access for " + owner + " to " + clientId + " withdrawn by " + user
					+ ", " + revoked + " tokens revoked");
			return Response.ok().build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "Revoking mcp grant", e);
			return Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * Mint a token for a caller that has no browser to authorise with - a script, a scheduled job.
	 *
	 * The value is returned once and never again.  It is an ordinary oauth_token with no client
	 * attached, so it goes through exactly the same validation as one obtained through a browser:
	 * same audience check, same scopes, same revocation, and it stops working the moment MCP is
	 * switched off or the group is taken away.
	 */
	@POST
	@Path("/tokens")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response mint(@Context HttpServletRequest request,
			@FormParam("name") String name,
			@FormParam("scope") String scope,
			@FormParam("days") Integer days) {

		String connectionString = "surveyKPI-McpMintToken";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			if(!ServerManager.isMcpEnabled(sd)) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}
			String user = request.getRemoteUser();
			a.isAuthorised(sd, request, user);

			/*
			 * A token can only carry scopes this user could have consented to anyway.  Unknown ones
			 * are dropped rather than refused, and an empty request gets the minimal scope.
			 */
			String granted = MCPScope.join(MCPScope.parse(scope));
			if(granted.length() == 0) {
				granted = MCPScope.join(MCPScope.SUPPORTED);
			}

			int uId = GeneralUtilityMethods.getUserId(sd, user);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			String resource = OAuthUrls.canonicalResource(request);

			String value = new OAuthTokenManager().mint(sd, uId, oId, granted, resource,
					name == null ? "console" : name, days);

			log.info("MCP token minted in the console by " + user + " for " + granted);

			Map<String, Object> out = new HashMap<>();
			out.put("token", value);
			out.put("scope", granted);
			out.put("resource", resource);
			return Response.ok(gson.toJson(out)).header("Cache-Control", "no-store").build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "Minting mcp token", e);
			return Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}
}
