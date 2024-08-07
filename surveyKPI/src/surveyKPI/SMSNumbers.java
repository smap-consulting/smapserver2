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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.model.Notification;
import org.smap.sdal.model.SMSNumber;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Services for managing SMS numbers
 */

@Path("/smsnumbers")
public class SMSNumbers extends Application {
	
	Authorise a = null;
	Authorise aOwner = null;

	private static Logger log =
			 Logger.getLogger(SMSNumbers.class.getName());
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm").create();
	
	LogManager lm = new LogManager(); // Application log
	
	public SMSNumbers() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		
		// Only allow server owners and organisational administrators to view or update number data
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
		aOwner = new Authorise(null, Authorise.ADMIN);
	
	}
	
	/*
	 * Get the sms numbers for the organisation or whole server depending on user permissions
	 */
	@GET
	@Produces("application/json")
	public Response getSMSNumbers(
			@Context HttpServletRequest request
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getSMSNumbers";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());		
		// End Authorisation
			
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SMSManager mgr = new SMSManager(localisation, "UTC");
			ArrayList<SMSNumber> numbers = mgr.getOurNumbers(sd, request.getRemoteUser());
			
			response = Response.ok(gson.toJson(numbers)).build();
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	

	/*
	 * Add a notification
	 */
	@Path("/number")
	@POST
	public Response addNotification(@Context HttpServletRequest request,
			@FormParam("ourNumber") String ourNumber,
			@QueryParam("tz") String tz) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-add sms number";
		
		log.info("Add Notification:========== " + ourNumber);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		// End Authorisation
		
		if(tz == null) {
			tz = "UTC";
		}
		
		String sql = "insert into sms_number (element_identifier, time_modified, our_number) "
				+ "values (gen_random_uuid(), now(), ?)";
		PreparedStatement pstmt = null;
		
		try {	
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ourNumber);
			pstmt.executeUpdate();
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
}

