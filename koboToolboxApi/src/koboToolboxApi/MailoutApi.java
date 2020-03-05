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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.MailoutPersonDt;
import org.smap.sdal.model.MailoutPersonTotals;

/*
 * Provides access to collected data
 */
@Path("/v1/mailout")
public class MailoutApi extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(MailoutApi.class.getName());
	
	public MailoutApi() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ANALYST);
		a = new Authorise(authorisations, null);
	}	
	
	/*
	 * Get subscription entries
	 */
	@GET
	@Produces("application/json")
	@Path("/{mailoutId}")
	public Response getSubscriptions(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId,
			@QueryParam("dt") boolean dt
			) { 
		
		String connectionString = "API - get emails in mailout";
		Response response = null;
		ArrayList<MailoutPerson> data = new ArrayList<> ();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		// End authorisation
		
		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		
			
			MailoutManager mm = new MailoutManager(localisation);
			data = mm.getMailoutPeople(sd, mailoutId);				
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			if(dt) {
				MailoutPersonDt mpDt = new MailoutPersonDt();
				mpDt.data = data;
				response = Response.ok(gson.toJson(mpDt)).build();
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
	
	/*
	 * Get subscription totals
	 */
	@GET
	@Produces("application/json")
	@Path("/{mailoutId}/totals")
	public Response getSubscriptionTotals(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId
			) { 
		
		String connectionString = "API - get emails in mailout";
		Response response = null;
		MailoutPersonTotals totals = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		// End authorisation
		
		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		
			
			MailoutManager mm = new MailoutManager(localisation);
			totals = mm.getMailoutPeopleTotals(sd,mailoutId);		
			
			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			response = Response.ok(gson.toJson(totals)).build();
	
			
	
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}

}

