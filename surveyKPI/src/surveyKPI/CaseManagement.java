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
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.KeyManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.CaseManagementAlert;
import org.smap.sdal.model.CaseManagementSettings;
import org.smap.sdal.model.UniqueKey;

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
	 * Get the case management settings for passed in survey
	 */
	@GET
	@Path("/settings/{survey_id}")
	@Produces("application/json")
	public Response getCaseManagementSettings(
			@Context HttpServletRequest request,
			@PathParam("survey_id") int sId
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getCaseManagementSettings";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());	
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			CaseManager cm = new CaseManager(localisation);
			
			String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
			CMS cms = cm.getCaseManagementSettings(sd, groupSurveyIdent);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(cms);
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
	 * Get the key settings for the passed in survey
	 */
	@GET
	@Path("/keys/{survey_id}")
	@Produces("application/json")
	public Response getKeySettings(
			@Context HttpServletRequest request,
			@PathParam("survey_id") int sId
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getCaseManagementSettings";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());	
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			KeyManager km = new KeyManager(localisation);
			
			String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
			UniqueKey uk = km.get(sd, groupSurveyIdent);
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(uk);
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
	 * Update the case management keys
	 */
	@Path("/keys/{group_survey_ident}")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateCaseManagementKeys(@Context HttpServletRequest request, 
			@PathParam("group_survey_ident") String groupSurveyIdent,
			@FormParam("keys") String keyString) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-updateCaseManagementKeys";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurveyIdent(sd, request.getRemoteUser(), groupSurveyIdent, false, false);
		// End Authorisation
			
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			// Validate setting structure
			UniqueKey keys = new Gson().fromJson(keyString, UniqueKey.class);
			
			KeyManager km = new KeyManager(localisation);

			String msg = null;
			km.update(sd, groupSurveyIdent, keys.key, keys.key_policy, request.getRemoteUser(), oId, true);
			msg = localisation.getString("cm_k_updated");	
			msg = msg.replace("%s1", groupSurveyIdent);
			msg = msg.replace("%s2", keys.key);
			msg = msg.replace("%s3", keys.key_policy);
			lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), LogManager.CASE_MANAGEMENT, msg, 0);
				
			response = Response.ok().build();
				
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
	@Path("/settings/{group_survey_ident}")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateCaseManagementSettings(@Context HttpServletRequest request, 
			@PathParam("group_survey_ident") String groupSurveyIdent,
			@FormParam("settings") String settingsString) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-updateCaseManagementSettings";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurveyIdent(sd, request.getRemoteUser(), groupSurveyIdent, false, superUser);
		// End Authorisation
			
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// Validate setting structure
			CaseManagementSettings settings = new Gson().fromJson(settingsString, CaseManagementSettings.class);
			
			CaseManager cm = new CaseManager(localisation);

			int sId = GeneralUtilityMethods.getSurveyId(sd, groupSurveyIdent);
			String msg = null;
			cm.updateSettings(sd, request.getRemoteUser(), groupSurveyIdent, settings);
			msg = localisation.getString("cm_s_updated");	
			msg = msg.replace("%s1", groupSurveyIdent);
			msg = msg.replace("%s2", new Gson().toJson(settings));
			lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.CASE_MANAGEMENT, msg, 0, request.getServerName());
				
			response = Response.ok().build();
				
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Update the case management alerts
	 */
	@Path("/settings/alert")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateCaseManagementAlert(@Context HttpServletRequest request, @FormParam("alert") String alertString) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-updateCaseManagementAlert";
		CaseManagementAlert alert = new Gson().fromJson(alertString, CaseManagementAlert.class);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(alert.id > 0) {
			a.isValidCaseManagementAlert(sd, request.getRemoteUser(), alert.id);
		}
		// End Authorisation
			
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			CaseManager cm = new CaseManager(localisation);

			String msg = null;
			if(alert.id == -1) {
					
				// New settings
				cm.createAlert(sd, request.getRemoteUser(), alert);
				msg = localisation.getString("cm_a_created");
					
			} else {
				// Existing setting
				cm.updateAlert(sd, request.getRemoteUser(), alert);
				msg = localisation.getString("cm_a_modified");	
			}
			msg = msg.replace("%s1", alert.name);
			msg = msg.replace("%s2",  alert.group_survey_ident);
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
	
	/*
	 * Delete case management alert
	 */
	@Path("/settings/alert")
	@DELETE
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response delCaseManagementSetting(@Context HttpServletRequest request, @FormParam("alert") String alertString) { 
		
		Response response = null;
		String requestName = "surveyKPI- delete case management alert";
		CaseManagementAlert alert = new Gson().fromJson(alertString, CaseManagementAlert.class);

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidCaseManagementAlert(sd, request.getRemoteUser(), alert.id);
		// End Authorisation			
		
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			CaseManager cm = new CaseManager(localisation);
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			cm.deleteAlert(sd, alert.id);
			String msg = localisation.getString("cm_a_deleted");
			msg = msg.replaceAll("%s1", String.valueOf(alert.id));
			lm.writeLogOrganisation(sd, o_id, request.getRemoteUser(), LogManager.CASE_MANAGEMENT, msg, 0);
			
			response = Response.ok().build();			
		}  catch (Exception ex) {
			log.log(Level.SEVERE, ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
			
		} finally {			
			SDDataSource.closeConnection(requestName, sd);
		}
		
		return response;
	}
	
}

