package surveyKPI.oauth;

/*
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.
*/

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.MCPScope;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * The two discovery documents an MCP client reads before it can authorize.
 *
 * Both are unauthenticated by design: a client has to be able to find out how to authenticate
 * before it has anything to authenticate with.  Neither says anything a caller could not work out
 * by trying the endpoints.
 *
 * Apache maps the well known paths onto these:
 *    /.well-known/oauth-protected-resource      -> /oauth/metadata/protected-resource
 *    /.well-known/oauth-authorization-server    -> /oauth/metadata/authorization-server
 */
@Path("/oauth/metadata")
public class OAuthDiscovery extends Application {

	private static Logger log = Logger.getLogger(OAuthDiscovery.class.getName());

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

	/*
	 * RFC 9728.  Names this server as a protected resource and points at the authorization server
	 * that issues tokens for it, which for Smap is itself.
	 */
	@GET
	@Path("/protected-resource")
	@Produces(MediaType.APPLICATION_JSON)
	public Response protectedResource(@Context HttpServletRequest request) {

		if(!enabled(request)) {
			return Response.status(Response.Status.NOT_FOUND).build();
		}

		String base = OAuthUrls.base(request);

		Map<String, Object> doc = new HashMap<>();
		doc.put("resource", OAuthUrls.canonicalResource(request));
		doc.put("authorization_servers", new String[] { base });
		doc.put("bearer_methods_supported", new String[] { "header" });
		/*
		 * Only the minimal scope is advertised.  Everything else is reached by step up: a tool the
		 * token cannot run answers 403 naming the scope it needs, and the client re-authorises for
		 * that as well as what it already had.  A client therefore never holds a permission it has
		 * not yet had a use for.
		 */
		doc.put("scopes_supported", MCPScope.SUPPORTED.toArray(new String[0]));
		doc.put("resource_documentation", "https://www.smap.com.au/docs/");

		return Response.ok(gson.toJson(doc)).build();
	}

	/*
	 * RFC 8414
	 */
	@GET
	@Path("/authorization-server")
	@Produces(MediaType.APPLICATION_JSON)
	public Response authorizationServer(@Context HttpServletRequest request) {

		if(!enabled(request)) {
			return Response.status(Response.Status.NOT_FOUND).build();
		}

		String base = OAuthUrls.base(request);

		Map<String, Object> doc = new HashMap<>();
		doc.put("issuer", base);
		doc.put("authorization_endpoint", base + "/oauth/authorize");
		doc.put("token_endpoint", base + "/oauth/token");
		doc.put("revocation_endpoint", base + "/oauth/revoke");
		doc.put("registration_endpoint", base + "/oauth/register");
		doc.put("scopes_supported", MCPScope.all().toArray(new String[0]));
		doc.put("response_types_supported", new String[] { "code" });
		doc.put("grant_types_supported", new String[] { "authorization_code", "refresh_token" });
		/*
		 * S256 only.  The plain method gives no protection against an intercepted authorization
		 * request, which is the whole reason PKCE exists.
		 */
		doc.put("code_challenge_methods_supported", new String[] { "S256" });
		doc.put("token_endpoint_auth_methods_supported",
				new String[] { "none", "client_secret_basic", "client_secret_post" });
		/*
		 * RFC 9207.  Says that authorization responses carry iss, so a client knows to check it and
		 * cannot be walked into redeeming a code at the wrong authorization server.
		 */
		doc.put("authorization_response_iss_parameter_supported", Boolean.TRUE);

		return Response.ok(gson.toJson(doc)).build();
	}

	private boolean enabled(HttpServletRequest request) {
		String connectionString = "surveyKPI-OAuthDiscovery";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			return ServerManager.isMcpEnabled(sd);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}
}
