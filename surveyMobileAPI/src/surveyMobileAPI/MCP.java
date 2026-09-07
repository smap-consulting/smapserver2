/*****************************************************************************

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

******************************************************************************/

package surveyMobileAPI;

import java.io.IOException;
import java.sql.Connection;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.RequestIdentity;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.TokenThrottle;
import org.smap.sdal.managers.OAuthTokenManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpDispatcher;
import org.smap.sdal.mcp.McpProtocol;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.mcp.McpToolRegistry;
import org.smap.sdal.mcp.tools.SurveyDataTool;
import org.smap.sdal.mcp.tools.SurveyListTool;
import org.smap.sdal.mcp.tools.SurveySubmissionCountTool;
import org.smap.sdal.mcp.tools.TopicListTool;
import org.smap.sdal.mcp.tools.WhoAmITool;
import org.smap.sdal.model.MCPError;
import org.smap.sdal.model.MCPRequest;
import org.smap.sdal.model.MCPResponse;
import org.smap.sdal.model.ServerData;

import com.google.gson.Gson;

/*
 * The Model Context Protocol endpoint, revision 2026-07-28.
 *
 * That revision is stateless: no handshake, no session, and every request carries its own protocol
 * version and capabilities.  So this resource holds nothing between requests except the registry of
 * tools, which is fixed at class load.  The prototype it replaces kept a mutable "initialized" flag
 * on a static manager shared by every user of the JVM.
 */
@Path("/mcp")
public class MCP extends Application {

	private static Logger log = Logger.getLogger(MCP.class.getName());

	private Gson gson = new Gson();
	private Authorise a = new Authorise(null, Authorise.MCP_ACCESS);

	/*
	 * Registered once.  The registry itself is immutable after this runs; which tools a caller can
	 * see is decided per request from their groups and their token's scopes.
	 */
	private static final McpToolRegistry registry = new McpToolRegistry();
	static {
		registry.register(new WhoAmITool());
		registry.register(new SurveyListTool());
		registry.register(new SurveySubmissionCountTool());
		registry.register(new SurveyDataTool());
		registry.register(new TopicListTool());
	}

	@POST
	@Consumes({MediaType.APPLICATION_JSON})
	@Produces({MediaType.APPLICATION_JSON})
	public Response mcpHandler(@Context HttpServletRequest request, String jsonQuery)
			throws IOException, ApplicationException {

		Connection sd = null;
		Connection cResults = null;
		String connectionString = "surveyMobileAPI-MCP";
		String method = null;
		String toolName = null;

		try {
			sd = SDDataSource.getConnection(connectionString);

			/*
			 * MCP is off unless a server owner has switched it on.  Answered as though the endpoint
			 * does not exist rather than advertising that it is there but disabled, and checked on
			 * every request so that switching it off stops sessions that are already running.
			 */
			ServerData server = new ServerManager().getServer(sd, null);
			if(!server.mcp_enabled) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			/*
			 * Authenticate with an OAuth 2.1 bearer token, and nothing else.  The x-api-key header
			 * is deliberately not accepted: two ways in means the weaker one becomes the way in,
			 * and an api token carries no MCP scopes, so honouring one would hand a client
			 * everything its holder can do and discard the containment the scopes exist for.
			 */
			String bearer = bearerToken(request);
			if(bearer == null) {
				return unauthorized(request, "A bearer token is required");
			}
			if(!TokenThrottle.isPermitted(request)) {
				return unauthorized(request, "Too many attempts");
			}

			OAuthTokenManager tm = new OAuthTokenManager();
			OAuthTokenManager.Resolved token = tm.resolve(sd, bearer);
			if(token == null) {
				TokenThrottle.failed(request);
				return unauthorized(request, "The token is not valid");
			}
			if(!isThisResource(request, token.resource)) {
				log.warning("MCP token presented with audience " + token.resource);
				return unauthorized(request, "The token was not issued for this server");
			}

			String user = token.ident;

			/*
			 * Re-checked every request rather than trusted from when the token was issued, so that
			 * removing the group, or the user moving organisation, takes effect at once.
			 */
			a.isAuthorised(sd, request, user);
			if(GeneralUtilityMethods.getOrganisationId(sd, user) != token.oId) {
				return unauthorized(request, "Your organisation has changed, please authorise again");
			}

			RequestIdentity.fromOauth(request, user, token.scope);
			tm.touch(sd, token.tokenId, request.getRemoteAddr());

			MCPRequest mcpRequest;
			try {
				mcpRequest = gson.fromJson(jsonQuery, MCPRequest.class);
			} catch (Exception e) {
				return Response.ok(gson.toJson(new MCPResponse(null,
						new MCPError(McpProtocol.PARSE_ERROR, "The request could not be parsed"))))
						.build();
			}
			if(mcpRequest != null) {
				method = mcpRequest.getMethod();
				if(mcpRequest.getParams() != null) {
					Object name = mcpRequest.getParams().get("name");
					toolName = name == null ? null : name.toString();
				}
			}

			cResults = ResultsDataSource.getConnection(connectionString);

			McpToolContext ctx = context(sd, cResults, request, user, token, server);
			MCPResponse mcpResponse = new McpDispatcher(registry).process(ctx, mcpRequest);

			// A notification expects no reply
			if(mcpResponse == null) {
				return Response.status(Response.Status.NO_CONTENT).build();
			}

			return withRoutingHeaders(Response.ok(gson.toJson(mcpResponse)), method, toolName).build();

		} catch (McpDispatcher.ScopeRequired e) {
			/*
			 * The one refusal a client can do something about.  A 403 naming the missing scope is
			 * what tells it to step up and come back, rather than to report a failure to the user.
			 */
			return withRoutingHeaders(insufficientScope(request, e.scope), method, toolName).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "MCP request failed", e);
			return Response.ok(gson.toJson(new MCPResponse(null,
					new MCPError(McpProtocol.INTERNAL_ERROR, "The request could not be completed"))))
					.build();
		} finally {
			if(cResults != null) {
				try {
					ResultsDataSource.closeConnection(connectionString, cResults);
				} catch (Exception e) {
					log.log(Level.WARNING, "Closing results connection", e);
				}
			}
			if(sd != null) {
				try {
					SDDataSource.closeConnection(connectionString, sd);
				} catch (Exception e) {
					log.log(Level.WARNING, "Closing connection", e);
				}
			}
		}
	}

	private McpToolContext context(Connection sd, Connection cResults, HttpServletRequest request,
			String user, OAuthTokenManager.Resolved token, ServerData server) throws Exception {

		Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, user));
		ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

		String tz = GeneralUtilityMethods.getOrganisationTZ(sd, token.oId);
		if(tz == null) {
			tz = "UTC";
		}

		/*
		 * The MCP cap if one is set, otherwise the API cap, otherwise unbounded.  A tool asked for
		 * everything should return a bounded answer rather than spending the server trying.
		 */
		int maxRows = server.mcp_max_rows > 0 ? server.mcp_max_rows : server.getMaxRecords();

		int uId = GeneralUtilityMethods.getUserId(sd, user);

		return new McpToolContext(sd, cResults, request, user, uId, token.oId, token.scope,
				false, localisation, tz, maxRows);
	}

	/*
	 * Mcp-Method and Mcp-Name let a gateway route, rate limit or meter per tool without parsing the
	 * JSON body, which is why the specification added them.
	 */
	private Response.ResponseBuilder withRoutingHeaders(Response.ResponseBuilder builder,
			String method, String toolName) {
		if(method != null) {
			builder.header(McpProtocol.HEADER_METHOD, method);
		}
		if(toolName != null) {
			builder.header(McpProtocol.HEADER_NAME, toolName);
		}
		return builder;
	}

	private String bearerToken(HttpServletRequest request) {
		String header = request.getHeader("Authorization");
		if(header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
			return null;
		}
		String value = header.substring(7).trim();
		return value.length() == 0 ? null : value;
	}

	private boolean isThisResource(HttpServletRequest request, String resource) {
		if(resource == null) {
			return false;
		}
		String given = resource.trim();
		while(given.endsWith("/")) {
			given = given.substring(0, given.length() - 1);
		}
		return given.equalsIgnoreCase("https://" + host(request) + "/mcp");
	}

	private String host(HttpServletRequest request) {
		String host = request.getHeader("X-Forwarded-Host");
		if(host == null || host.trim().length() == 0) {
			host = request.getServerName();
		}
		int comma = host.indexOf(',');
		if(comma > 0) {
			host = host.substring(0, comma);
		}
		return host.trim();
	}

	/*
	 * RFC 9728.  The challenge says where the protected resource metadata lives, so a client that
	 * has never spoken to this server can discover how to authorise without being told out of band.
	 */
	private Response unauthorized(HttpServletRequest request, String description) {
		return Response.status(Response.Status.UNAUTHORIZED)
				.header("WWW-Authenticate", challenge(request, MCPScope.SUPPORTED.get(0),
						"invalid_token", description))
				.build();
	}

	private Response.ResponseBuilder insufficientScope(HttpServletRequest request, String scope) {
		return Response.status(Response.Status.FORBIDDEN)
				.header("WWW-Authenticate", challenge(request, scope, "insufficient_scope",
						"This tool needs the " + scope + " permission"));
	}

	private String challenge(HttpServletRequest request, String scope, String error, String description) {
		return "Bearer resource_metadata=\"https://" + host(request)
				+ "/.well-known/oauth-protected-resource\""
				+ ", scope=\"" + scope + "\""
				+ ", error=\"" + error + "\""
				+ ", error_description=\"" + description.replace('"', '\'') + "\"";
	}
}
