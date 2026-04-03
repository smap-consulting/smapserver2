package surveyKPI;

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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.WorkflowManager;
import org.smap.sdal.model.WorkflowData;
import org.smap.sdal.model.WorkflowItem;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

@Path("/workflow")
public class Workflow extends Application {

	Authorise a = null;

	private static Logger log = Logger.getLogger(Workflow.class.getName());

	public Workflow() {
		ArrayList<String> authorisations = new ArrayList<String>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}

	/*
	 * Get all workflow items accessible to the logged-in user across all their projects
	 */
	@Path("/items")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getWorkflowItems(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-items";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			WorkflowManager wm = new WorkflowManager();
			WorkflowData data = wm.getWorkflowItems(sd, request.getRemoteUser());

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(data)).build();
			System.out.println(gson.toJson(data));

		} catch (SQLException e) {
			log.log(Level.SEVERE, "No data available", e);
			response = Response.serverError().entity("No data available").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error getting workflow items", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Save the full node positions map for the current user+org.
	 * Body: JSON object mapping node_id -> {x, y}
	 */
	@Path("/positions")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response savePositions(@Context HttpServletRequest request, String body) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-savePositions";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			Map<String, WorkflowItem> positions = new Gson().fromJson(body,
					new TypeToken<Map<String, WorkflowItem>>(){}.getType());
			WorkflowManager wm = new WorkflowManager();
			wm.savePositions(sd, request.getRemoteUser(), positions);
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error saving workflow positions", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Reset node positions for the current user+org back to server-computed defaults.
	 */
	@Path("/positions")
	@DELETE
	public Response resetPositions(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-resetPositions";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			WorkflowManager wm = new WorkflowManager();
			wm.resetPositions(sd, request.getRemoteUser());
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error resetting workflow positions", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
}
