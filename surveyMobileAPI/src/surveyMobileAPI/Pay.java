package surveyMobileAPI;


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
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.EsewaManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Login functions
 */
@Path("/pay")
public class Pay extends Application {
	
	private static Logger log =
			 Logger.getLogger(Pay.class.getName());
	
	Authorise a = null;
	
	public Pay() {
		a = new Authorise(null, Authorise.ENUM);
	}
	
	/*
	 * Pay using eSewa
	 */
	@GET
	@Path("/esewa/merchant/{instanceid}")
	@Produces("application/json")
	public Response esewaPay(@Context HttpServletRequest request,
			@PathParam("instanceId") String instanceId
			) {
		
		Response response = null;
		String connectionString = "SurveyMobileApi - pay - esewa"; 		
		
		// Authorisation - TODO extend to non logged in users
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());	
		
		try {	
			
			EsewaManager mgr = new EsewaManager(true, request.getServerName());
			mgr.pay(instanceId);
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to get access key", e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection("surveyKPI-login-key", sd);
		}
		
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		String resp = gson.toJson("{}");
		response = Response.ok(resp).build();
		
		return response;

	}

	
	/*
	 * Successful payment response for esewa
	 */
	@GET
	@Path("/esewa/success")
	public Response esewaSuccess(@Context HttpServletRequest request
			) {
		
		Response response = null;
		 
		String connectionString = "SurveyMobileApi - pay - esewa - success"; 		
		
		// No Authorisation
		Connection sd = SDDataSource.getConnection(connectionString);
		
		try {
			
			response = Response.ok("").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		
		
		return response;

	}

	/*
	 * Failed payment response for esewa
	 */
	@GET
	@Path("/esewa/failed")
	public Response esewaFailed(@Context HttpServletRequest request
			) {
		
		Response response = null;
		 
		String connectionString = "SurveyMobileApi - pay - esewa - failed"; 		
		
		// No Authorisation
		Connection sd = SDDataSource.getConnection(connectionString);
		
		try {
			
			response = Response.ok("").build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to get access key", e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;

	}



}

