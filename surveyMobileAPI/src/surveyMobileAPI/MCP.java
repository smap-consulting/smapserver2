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
import org.smap.sdal.model.MCPQuery;
import org.smap.sdal.model.MCPResponse;

import com.google.gson.Gson;


/*
 * Handle MCP queries
 */

@Path("/mcp")

public class MCP extends Application {
	
	private Gson gson = new Gson();
	private static Logger log = Logger.getLogger(MCP.class.getName());
	
	@POST
	@Produces({MediaType.APPLICATION_JSON})   
	public Response mcpHandler(@Context HttpServletRequest request, String jsonQuery) throws IOException, ApplicationException {

		log.info("MCP request: " + jsonQuery);
		
		MCPResponse response;
		MCPQuery query;
		try {
			query = gson.fromJson(jsonQuery, MCPQuery.class);
			response = new MCPResponse("success", "plane");
		} catch (Exception e) {
			response = new MCPResponse("error", e.getMessage());
			System.out.println("Invalid JSON");
		}

            
		return Response.ok(gson.toJson(response)).build();
           
	}
	
}

