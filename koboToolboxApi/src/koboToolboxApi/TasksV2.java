package koboToolboxApi;
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

import javax.servlet.http.HttpServletRequest;
import managers.TasksManager;
import model.TasksEndPoint;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;

/*
 * Returns overview data such as the number of submissions
 * This is a Smap specific extension to the KoboToolbox API
 */
@Path("/v1/taskgroups")
@Produces("application/json")
public class TasksV2 extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(TasksV2.class.getName());
	
	public TasksV2() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 *  Get the available end points for tasks
	 */
	@GET
	@Produces("application/json")
	public Response getTaskGroups(@Context HttpServletRequest request) { 
		
		Response response = null;
		String connectionString = "API - TaskGroups";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			int orgId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser(), 0);
			
			if(orgId > 0) {
				TasksManager tm = new TasksManager(orgId);
				response = tm.getTasks(sd, orgId, userId, limit);
			}
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(endPoints);
			response = Response.ok(resp).build();
			
		} catch (Exception e) {
			e.printStackTrace();
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection("koboToolboxApi - get individual tasks", sd);
		}

		return response;
	}
	

}

