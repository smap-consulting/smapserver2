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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.notifications.interfaces.EmitNotifications;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.OrganisationManager;
import org.smap.sdal.managers.PeopleManager;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;

import com.google.gson.Gson;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Returns a list of all projects that are in the same organisation as the user making the request
 */
@Path("/subscriptions")
public class Subscriptions extends Application {

	private static Logger log =
			 Logger.getLogger(Subscriptions.class.getName());
	
	/*
	 * Unsubscribe
	 */
	@GET
	@Path("/unsubscribe/{token}")
	public Response unsubscribe(
			@Context HttpServletRequest request,
			@PathParam("token") String token) { 
		
		Response response = null;
		
		log.info("Unsubscribing user: " + token);
		
		Connection sd = SDDataSource.getConnection("surveyKPI-Register");
		
		PreparedStatement pstmt = null;
		try {
			
			// Localisation
			String hostname = request.getServerName();
			String loc_code = "en";
			if(hostname.contains("kontrolid")) {
				loc_code = "es";
			} 
			Locale locale = new Locale(loc_code);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			
			PeopleManager pm = new PeopleManager(localisation);
			pm.unsubscribe(sd, token);
			
			response = Response.ok().build();
		
				
		} catch(ApplicationException e) {
			response = Response.serverError().entity(e.getMessage()).build();
		} catch(Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			SDDataSource.closeConnection("surveyKPI-Register", sd);
		}
		
		return response;
	}
	


}

