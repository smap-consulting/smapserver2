package surveyKPI.oauth;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.TokenThrottle;
import org.smap.sdal.managers.ApiTokenManager;
import org.smap.sdal.managers.OAuthManager;
import org.smap.sdal.managers.OAuthTokenManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.model.OAuthClient;
import org.smap.sdal.model.ServerData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * The token endpoint.
 *
 * Two grants and no more: authorization_code, and refresh_token to rotate.  No implicit grant, no
 * resource owner password grant.  OAuth 2.1 drops both and there is no case for them here - a
 * password grant would hand an MCP client the user's Smap password, which is exactly what putting
 * OAuth in front of MCP was meant to stop.
 */
@Path("/oauth/token")
public class OAuthTokenService extends Application {

	private static Logger log = Logger.getLogger(OAuthTokenService.class.getName());

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response token(@Context HttpServletRequest request,
			@FormParam("grant_type") String grantType,
			@FormParam("code") String code,
			@FormParam("redirect_uri") String redirectUri,
			@FormParam("client_id") String clientId,
			@FormParam("client_secret") String clientSecret,
			@FormParam("code_verifier") String codeVerifier,
			@FormParam("refresh_token") String refreshToken,
			@FormParam("resource") String resource) {

		String connectionString = "surveyKPI-OAuthToken";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			ServerManager sm = new ServerManager();
			ServerData server = sm.getServer(sd, null);
			if(!server.mcp_enabled) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			if(!TokenThrottle.isPermitted(request)) {
				return error(Response.Status.TOO_MANY_REQUESTS, "temporarily_unavailable",
						"Too many attempts");
			}

			// A client id may also arrive by http basic, which is client_secret_basic
			String[] basic = basicCredentials(request);
			if(basic != null) {
				clientId = basic[0];
				clientSecret = basic[1];
			}
			if(clientId == null) {
				return badRequest(request, "invalid_client", "A client id is required");
			}

			OAuthManager om = new OAuthManager();
			OAuthClient client = om.read(sd, clientId);
			if(client == null) {
				return badRequest(request, "invalid_client", "Unknown client");
			}
			if("disabled".equals(client.status)) {
				return badRequest(request, "invalid_client", "This client has been disabled");
			}
			/*
			 * A confidential client proves who it is.  A public one cannot keep a secret, so PKCE
			 * is what stands between an intercepted code and a token.
			 */
			if(!client.isPublic()) {
				if(clientSecret == null || client.secretHash == null
						|| !client.secretHash.equals(ApiTokenManager.hash(clientSecret))) {
					return badRequest(request, "invalid_client", "Client authentication failed");
				}
			}

			if("authorization_code".equals(grantType)) {
				return authorizationCode(sd, request, om, client, code, redirectUri, codeVerifier,
						resource, server);
			}
			if("refresh_token".equals(grantType)) {
				return refresh(sd, request, client, refreshToken, resource, server);
			}
			return badRequest(request, "unsupported_grant_type", "Unsupported grant type");

		} catch (Exception e) {
			log.log(Level.SEVERE, "Issuing oauth token", e);
			return error(Response.Status.INTERNAL_SERVER_ERROR, "server_error",
					"The token could not be issued");
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	private Response authorizationCode(Connection sd, HttpServletRequest request, OAuthManager om,
			OAuthClient client, String code, String redirectUri, String codeVerifier,
			String resource, ServerData server) throws Exception {

		if(code == null || codeVerifier == null) {
			return badRequest(request, "invalid_request", "A code and a code verifier are required");
		}

		OAuthManager.Grant grant = om.redeemCode(sd, code, client.client_id, redirectUri, codeVerifier);
		if(grant == null) {
			return badRequest(request, "invalid_grant", "The authorization code is not valid");
		}

		/*
		 * RFC 8707.  A token is bound to the resource it was asked for, and the resource server
		 * refuses anything issued for somebody else.  Without this a token for one MCP server could
		 * be replayed against another that trusts the same authorization server.
		 */
		if(resource != null && !resource.equals(grant.resource)) {
			return badRequest(request, "invalid_target", "The resource does not match the grant");
		}

		String refused = stillEligible(sd, grant.uId, grant.oId);
		if(refused != null) {
			return badRequest(request, "invalid_grant", refused);
		}

		OAuthTokenManager tm = new OAuthTokenManager();
		OAuthTokenManager.Issued issued = tm.issue(sd, client.client_id, grant.uId, grant.oId,
				grant.scope, grant.resource, server.mcp_token_ttl, null);

		return ok(issued);
	}

	private Response refresh(Connection sd, HttpServletRequest request, OAuthClient client,
			String refreshToken, String resource, ServerData server) throws Exception {

		if(refreshToken == null) {
			return badRequest(request, "invalid_request", "A refresh token is required");
		}

		OAuthTokenManager tm = new OAuthTokenManager();
		OAuthTokenManager.Resolved resolved = tm.redeemRefresh(sd, refreshToken, client.client_id);
		if(resolved == null) {
			return badRequest(request, "invalid_grant", "The refresh token is not valid");
		}
		if(resource != null && !resource.equals(resolved.resource)) {
			return badRequest(request, "invalid_target", "The resource does not match the grant");
		}

		/*
		 * Re-checked on every refresh, not only at first issue.  A user who has lost the mcp access
		 * group, or moved organisation, stops being able to refresh straight away rather than when
		 * their last access token happens to run out.
		 */
		String refused = stillEligible(sd, resolved.uId, resolved.oId);
		if(refused != null) {
			return badRequest(request, "invalid_grant", refused);
		}

		OAuthTokenManager.Issued issued = tm.issue(sd, client.client_id, resolved.uId, resolved.oId,
				resolved.scope, resolved.resource, server.mcp_token_ttl, resolved.tokenId);

		return ok(issued);
	}

	/*
	 * Everything that could have changed between consent and now.  Returns null when the grant is
	 * still good, or the reason it is not.
	 */
	static String stillEligible(Connection sd, int uId, int oId) throws Exception {

		String ident = GeneralUtilityMethods.getUserIdent(sd, uId);
		if(ident == null) {
			return "The user no longer exists";
		}
		if(!GeneralUtilityMethods.hasSecurityGroup(sd, ident, Authorise.MCP_ACCESS_ID)) {
			return "The user no longer has MCP access";
		}
		/*
		 * A grant belongs to the organisation it was given in.  Switching organisation reloads the
		 * user's security groups from that organisation's saved settings, so a token issued as an
		 * analyst in one could come back as an administrator in another.  The grant does not travel.
		 */
		if(GeneralUtilityMethods.getOrganisationId(sd, ident) != oId) {
			return "The user has changed organisation, please authorise again";
		}
		return null;
	}

	private Response ok(OAuthTokenManager.Issued issued) {
		Map<String, Object> out = new HashMap<>();
		out.put("access_token", issued.accessToken);
		out.put("token_type", "Bearer");
		out.put("expires_in", issued.expiresIn);
		out.put("refresh_token", issued.refreshToken);
		out.put("scope", issued.scope);
		return Response.ok(gson.toJson(out))
				.header("Cache-Control", "no-store")
				.build();
	}

	private Response badRequest(HttpServletRequest request, String code, String description) {
		TokenThrottle.failed(request);
		return error(Response.Status.BAD_REQUEST, code, description);
	}

	private Response error(Response.Status status, String code, String description) {
		Map<String, String> body = new HashMap<>();
		body.put("error", code);
		body.put("error_description", description);
		return Response.status(status).entity(gson.toJson(body))
				.header("Cache-Control", "no-store")
				.build();
	}

	private String[] basicCredentials(HttpServletRequest request) {
		String header = request.getHeader("Authorization");
		if(header == null || !header.toLowerCase().startsWith("basic ")) {
			return null;
		}
		try {
			String decoded = new String(java.util.Base64.getDecoder()
					.decode(header.substring(6).trim()), java.nio.charset.StandardCharsets.UTF_8);
			int colon = decoded.indexOf(':');
			if(colon < 0) {
				return null;
			}
			return new String[] {
					java.net.URLDecoder.decode(decoded.substring(0, colon), "UTF-8"),
					java.net.URLDecoder.decode(decoded.substring(colon + 1), "UTF-8") };
		} catch (Exception e) {
			return null;
		}
	}
}
