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
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.RequestIdentity;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.TokenThrottle;
import org.smap.sdal.managers.MCPManager;
import org.smap.sdal.managers.OAuthTokenManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.tools.EchoTool;
import org.smap.sdal.mcp.tools.GetSurveyDataTool;
import org.smap.sdal.mcp.tools.GetSurveySubmissionsTool;
import org.smap.sdal.mcp.tools.ListSurveysTool;
import org.smap.sdal.mcp.tools.ListTopicsTool;
import org.smap.sdal.model.MCPError;
import org.smap.sdal.model.MCPRequest;
import org.smap.sdal.model.MCPResponse;

import com.google.gson.Gson;


/*
 * Handle Model Context Protocol (MCP) requests
 * Implements JSON-RPC 2.0 protocol for MCP
 */

@Path("/mcp")

public class MCP extends Application {

	private Gson gson = new Gson();
	private static Logger log = Logger.getLogger(MCP.class.getName());
	private Authorise a = new Authorise(null, Authorise.MCP_ACCESS);

	// Singleton MCP manager with registered tools
	private static MCPManager mcpManager;

	static {
		// Initialize MCP manager and register tools
		mcpManager = new MCPManager();
		mcpManager.getToolRegistry().register(new EchoTool());
		mcpManager.getToolRegistry().register(new ListSurveysTool());
		mcpManager.getToolRegistry().register(new GetSurveySubmissionsTool());
		mcpManager.getToolRegistry().register(new GetSurveyDataTool());
		mcpManager.getToolRegistry().register(new ListTopicsTool());
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
		String host = request.getHeader("X-Forwarded-Host");
		if(host == null || host.trim().length() == 0) {
			host = request.getServerName();
		}
		int comma = host.indexOf(',');
		if(comma > 0) {
			host = host.substring(0, comma);
		}
		String expected = "https://" + host.trim() + "/mcp";
		String given = resource.trim();
		while(given.endsWith("/")) {
			given = given.substring(0, given.length() - 1);
		}
		return given.equalsIgnoreCase(expected);
	}

	/*
	 * RFC 9728 wants the 401 to say where the protected resource metadata lives, so a client that
	 * has never spoken to this server can discover how to authorise without being told out of band.
	 */
	private Response unauthorized(HttpServletRequest request, String description) {
		String host = request.getHeader("X-Forwarded-Host");
		if(host == null || host.trim().length() == 0) {
			host = request.getServerName();
		}
		String challenge = "Bearer resource_metadata=\"https://" + host
				+ "/.well-known/oauth-protected-resource\", scope=\""
				+ String.join(" ", MCPScope.SUPPORTED) + "\", error=\"invalid_token\""
				+ ", error_description=\"" + description.replace('"', '\'') + "\"";
		return Response.status(Response.Status.UNAUTHORIZED)
				.header("WWW-Authenticate", challenge)
				.build();
	}

	@POST
	@Produces({MediaType.APPLICATION_JSON})
	public Response mcpHandler(@Context HttpServletRequest request, String jsonQuery) throws IOException, ApplicationException {

		log.info("MCP request: " + jsonQuery);

		Connection sd = null;
		MCPResponse mcpResponse = null;
		String connectionString = "surveyMobileAPI-MCP";

		try {
			// Get database connection
			sd = SDDataSource.getConnection(connectionString);

			/*
			 * MCP is off unless a server owner has switched it on.  Answer as though the endpoint
			 * does not exist rather than advertising that it is there but disabled.  Checked on
			 * every request so that switching it off stops sessions that are already running.
			 */
			if(!ServerManager.isMcpEnabled(sd)) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			/*
			 * Authenticate with an OAuth 2.1 bearer token, and nothing else.
			 *
			 * The x-api-key header is deliberately not accepted here.  Two ways in would mean the
			 * weaker one becomes the way in, and an api token carries no MCP scopes, so honouring
			 * one would hand a client everything its holder can do and throw away the containment
			 * the scopes exist to provide.
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

			/*
			 * A token is for this server or it is for nothing.  RFC 8707 audience binding is what
			 * stops a token issued for another MCP server, by an authorization server both trust,
			 * being replayed here.
			 */
			if(!isThisResource(request, token.resource)) {
				log.warning("MCP token presented with audience " + token.resource);
				return unauthorized(request, "The token was not issued for this server");
			}

			String user = token.ident;

			/*
			 * Re-checked on every request rather than trusted from when the token was issued, so
			 * that removing the group, or the user moving organisation, takes effect at once.  The
			 * server level switch was already checked above, which is what makes it a kill switch.
			 */
			a.isAuthorised(sd, request, user);
			if(GeneralUtilityMethods.getOrganisationId(sd, user) != token.oId) {
				return unauthorized(request, "Your organisation has changed, please authorise again");
			}

			/*
			 * Tell the rest of the request who this is.  Without it everything downstream would
			 * call getRemoteUser(), which is empty on a bearer authenticated request, and quietly
			 * lose the user's language and super user status.
			 */
			RequestIdentity.fromOauth(request, user, token.scope);
			tm.touch(sd, token.tokenId, request.getRemoteAddr());

			// Parse JSON-RPC request
			MCPRequest mcpRequest = null;
			try {
				mcpRequest = gson.fromJson(jsonQuery, MCPRequest.class);
			} catch (Exception e) {
				log.severe("Failed to parse MCP request: " + e.getMessage());
				mcpResponse = new MCPResponse(
					null,
					new MCPError(MCPError.PARSE_ERROR, "Parse error: " + e.getMessage())
				);
				return Response.ok(gson.toJson(mcpResponse)).build();
			}

			// Process the request
			mcpResponse = mcpManager.processRequest(sd, user, mcpRequest);

			// If response is null, this was a notification that expects no response
			if (mcpResponse == null) {
				log.info("Notification processed, no response needed");
				return Response.status(Response.Status.NO_CONTENT).build();
			}

		} catch (AuthorisationException e) {
			log.warning("Authorization failed: " + e.getMessage());
			mcpResponse = new MCPResponse(
				null,
				new MCPError(MCPError.INTERNAL_ERROR, "Authorization failed: " + e.getMessage())
			);
		} catch (Exception e) {
			log.severe("Error processing MCP request: " + e.getMessage());
			e.printStackTrace();
			mcpResponse = new MCPResponse(
				null,
				new MCPError(MCPError.INTERNAL_ERROR, "Internal error: " + e.getMessage())
			);
		} finally {
			// Clean up database connection
			if (sd != null) {
				try {
					SDDataSource.closeConnection(connectionString, sd);
				} catch (Exception e) {
					log.severe("Error closing connection: " + e.getMessage());
				}
			}
		}

		return Response.ok(gson.toJson(mcpResponse)).build();
	}
}

