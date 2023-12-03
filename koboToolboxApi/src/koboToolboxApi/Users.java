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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.UserLocationManager;
import org.smap.sdal.managers.UserManager;

/*
 * Provides access to audit views on the surveys
 */
@Path("/v1/users")
public class Users extends Application {
	
	Authorise a = null;
	private static Logger log =
			Logger.getLogger(UserLocationManager.class.getName());

	LogManager lm = new LogManager();		// Application log

	public Users() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
	}

	/*
	 * Returns a list of user locations in geojson
	 */
	@GET
	@Path("/locations")
	@Produces("application/json")
	public Response getUserLocation(@Context HttpServletRequest request,
			@QueryParam("project") int pId,				// Project Id
			@QueryParam("tz") String tz) { 

		Response response = null;
		String connectionString = "API - getUserLocations";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		tz = (tz == null) ? "UTC" : tz;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			UserLocationManager ulm = new UserLocationManager(localisation, tz);
			response = Response.ok(ulm.getUserLocations(sd, 
					pId,
					0,
					0,
					request.getRemoteUser(),
					false
					)).build();
			
		} catch (SQLException e) {
			e.printStackTrace();
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Update a users location
	 */
	@POST
	@Path("/location")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces("application/json")
	public Response updateUserLocation(@Context HttpServletRequest request,
			@FormParam("lat") String latString,
			@FormParam("lon") String lonString) { 

		Response response = null;
		String connectionString = "API - updateUserLocation";

		// Authorisation not required as a user only updates their own location
		Connection sd = SDDataSource.getConnection(connectionString);
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			UserLocationManager ulm = new UserLocationManager(localisation, "UTC");
			Double lat = 0.0;
			Double lon = 0.0;
			try {
				lat = Double.valueOf(latString);
				lon = Double.valueOf(lonString);
			} catch (Exception e) {
				log.log(Level.SEVERE, "invalid lat or lon value: " + latString + " : " + lonString, e);
			}
			if(!(lat == 0.0 && lon == 0.0)) {
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				ulm.recordRefresh(sd, oId, request.getRemoteUser(), lat, lon, 
						0L, null, null, null, false);
				log.info("User Location update: " + latString + " : " + lonString);
			} else {
				log.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ User Location 0.0 0.0: ");
			}
			
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Changes a user current organisation
	 */
	@GET
	@Path("/organisation/{org}")
	@Produces("application/json")
	public Response setUserOrganisation(@Context HttpServletRequest request,
			@PathParam("org") String orgName) { 

		Response response = null;
		String connectionString = "API - setUserOrganisation";

		// Authorisation - Not required the current organisation for the authenticated user will be changed

		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			log.info("New organisation name: " + orgName);
			UserManager um = new UserManager(localisation);
			int newOrgId = GeneralUtilityMethods.getOrganisationIdfromName(sd, orgName);
			um.switchUsersOrganisation(sd, newOrgId, request.getRemoteUser(), true);
			response = Response.ok("{}").build();
		} catch (Exception e) {
			e.printStackTrace();
			response = Response.serverError().entity("{msg: \"" + e.getMessage() + "\"}").build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

}

