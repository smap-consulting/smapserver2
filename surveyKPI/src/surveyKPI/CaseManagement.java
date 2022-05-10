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
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.CMS;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Services for managing roles
 */

@Path("/cases")
public class CaseManagement extends Application {
	
	Authorise a = null;

	private static Logger log =
			 Logger.getLogger(CaseManagement.class.getName());
	
	LogManager lm = new LogManager(); // Application log
	
	public CaseManagement() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		
		// Only allow security administrators and organisational administrators to view or update the roles
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.SECURITY);
		authorisations.add(Authorise.ORG);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ANALYST);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get the case management settings for the organisation
	 */
	@GET
	@Path("/settings")
	@Produces("application/json")
	public Response getCaseManagementSettings(
			@Context HttpServletRequest request
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getCaseManagementSettings";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());		
		// End Authorisation
		
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			CaseManager cm = new CaseManager(localisation);
			
			int o_id  = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			ArrayList<CMS> settings = cm.getCases(sd, o_id);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(settings);
			response = Response.ok(resp).build();
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Update the case management settings
	 */
	@Path("/settings")
	@POST
	@Consumes("application/json")
	public Response updateRoles(@Context HttpServletRequest request, @FormParam("settings") String settings) { 
		
		Response response = null;
		String connectionString = "surveyKPI-updateRoles";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
			
		CMS cms = new Gson().fromJson(settings, CMS.class);
		
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			CaseManager cm = new CaseManager(localisation);

			String msg = null;
			if(cms.id == -1) {
					
				// New settings
				cm.createCMS(sd, cms, o_id, request.getRemoteUser());
				msg = localisation.getString("cm_s_created");
					
			} else {
				// Existing setting
				cm.updateCMS(sd, cms, o_id, request.getRemoteUser());
				msg = localisation.getString("r_modified");	
			}
			msg = msg.replace("%s1",  cms.name);
			lm.writeLogOrganisation(sd, o_id, request.getRemoteUser(), LogManager.CASE_MANAGEMENT, msg, 0);
				
			response = Response.ok().build();
				
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	

}

