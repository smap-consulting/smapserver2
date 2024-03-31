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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
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
import org.smap.sdal.managers.ContactManager;
import org.smap.sdal.model.SubItemDt;
import org.smap.sdal.model.SubsDt;

/*
 * Provides access to people identified by emails (subscribers)
 */
@Path("/v1/subscriptions")
public class Subscriptions extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(Subscriptions.class.getName());
	
	public Subscriptions() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	
	/*
	 * Get subscription entries
	 */
	@GET
	@Produces("application/json")
	public Response getSubscriptions(@Context HttpServletRequest request,
			@QueryParam("dt") boolean dt,
			@QueryParam("tz") String tz					// Timezone
			) { 
		
		String connectionString = "API - get subscriptions";
		Response response = null;
		ArrayList<SubItemDt> data = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		tz = (tz == null) ? "UTC" : tz;
		
		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			ContactManager cm = new ContactManager(localisation);
			data = cm.getSubscriptions(sd, request.getRemoteUser(), tz, dt);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			if(dt) {
				SubsDt subs = new SubsDt();
				subs.data = data;
				response = Response.ok(gson.toJson(subs)).build();
			} else {
				response = Response.ok(gson.toJson(data)).build();
			}
			
	
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}

}

