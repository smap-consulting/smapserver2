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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.MCPManager;
import org.smap.sdal.mcp.tools.EchoTool;
import org.smap.sdal.mcp.tools.GetSurveySubmissionsTool;
import org.smap.sdal.mcp.tools.ListSurveysTool;
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
	private Authorise a = new Authorise(null, Authorise.ENUM);

	// Singleton MCP manager with registered tools
	private static MCPManager mcpManager;

	static {
		// Initialize MCP manager and register tools
		mcpManager = new MCPManager();
		mcpManager.getToolRegistry().register(new EchoTool());
		mcpManager.getToolRegistry().register(new ListSurveysTool());
		mcpManager.getToolRegistry().register(new GetSurveySubmissionsTool());
	}

	@POST
	@Produces({MediaType.APPLICATION_JSON})
	public Response mcpHandler(@Context HttpServletRequest request, String jsonQuery) throws IOException, ApplicationException {

		log.info("MCP request: " + jsonQuery);

		Connection sd = null;
		MCPResponse mcpResponse = null;

		try {
			// Get database connection
			sd = SDDataSource.getConnection("surveyMobileAPI-MCP");

			// Get authenticated user
			String user = request.getRemoteUser();
			if (user == null) {
				user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
			}
			if (user == null) {
				throw new AuthorisationException("Unknown User");
			}

			// Authorize user
			a.isAuthorised(sd, user);

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
					sd.close();
				} catch (Exception e) {
					log.severe("Error closing connection: " + e.getMessage());
				}
			}
		}

		return Response.ok(gson.toJson(mcpResponse)).build();
	}
}

