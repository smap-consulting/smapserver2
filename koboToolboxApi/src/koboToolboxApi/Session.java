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

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.http.auth.AuthenticationException;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.CreateTaskResp;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.SurveyIdent;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskProperties;
import org.smap.sdal.model.TaskServerDefn;

/*
 * Provides API access managed using a session key
 * Does not seem to be used
 *
@Path("/v1/session")
public class Session extends Application {
	
	Authorise a = null;
	Authorise aOrg = null;
	
	private static Logger log =
			 Logger.getLogger(Session.class.getName());
	
	private class SessionResponse {
		@SuppressWarnings("unused")   // Used to generate json
		public String sessionKey;
		public SessionResponse(String key) {
			sessionKey = key;
		}
	}
	
	public Session() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);	 
	}
	
	/*
	 * Get a token
	 *
	@POST
	@Produces("application/json")
	@Path("/login")
	public Response login(@Context HttpServletRequest request,
			@FormParam("username") String username,
			@FormParam("password") String password) {
		
		String connectionString = "Session - login";
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		Response response;
		
		log.info("xxxxxxx: " + username + " : " + password);
		String sessionKey = null;
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			if(GeneralUtilityMethods.isPasswordValid(sd, username, password)) {
				sessionKey = GeneralUtilityMethods.getNewAccessKey(sd, username, false);
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			 SDDataSource.closeConnection(connectionString, sd);
		}
		
		if(sessionKey == null) {
			response = Response.status(Status.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, "Smap").entity("Authorisation Error").build();
		} else {
			 SessionResponse sr = new SessionResponse(sessionKey);
			 response = Response.ok(gson.toJson(sr)).build();
		}

		return response; 
	}

	/*
	 * Get available surveys
	 *
	@GET
	@Produces("application/json")
	@Path("/surveys")
	public Response getSurveys(@Context HttpServletRequest request,
			@QueryParam("sessionKey") String sessionKey) throws AuthenticationException {
		
		String connectionString = "session - Get Survey Idents";
		Response response = null;
		Connection sd = null;
	
		try {
			sd = SDDataSource.getConnection(connectionString);
			String user = null;
			try {
				user = authoriseSessionKey(sd, sessionKey);
			} catch (Exception e) {
				
			}
			if(user == null) {
				response = Response.status(Status.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, "Smap").entity("Authorisation Error").build();
			} else {
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, user));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				ArrayList<SurveyIdent> surveyIdents = sm.getSurveyIdentList(sd, user, false);
				Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				
				response = Response.ok(gson.toJson(surveyIdents)).build();	
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {			
			SDDataSource.closeConnection(connectionString , sd);				
		}

		return response;
	}
	
	@GET
	@Produces("application/json")
	@Path("/survey_data/poll")
	public Response getSubmissions(@Context HttpServletRequest request,
			@QueryParam("sessionKey") String sessionKey,
			@QueryParam("survey") String sIdent,	
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("filter") String filter
			) throws ApplicationException, Exception { 
		
		Response response;
	
		if(tz == null) {
			tz = "UTC";
		}
		
		String connectionString = "session - poll for survey records";
		
		Connection cResults = null;
		Connection sd = null;		
	
		try {
			sd = SDDataSource.getConnection(connectionString);
			cResults = ResultsDataSource.getConnection(connectionString);
			
			String user = null;
			try {
				user = authoriseSessionKey(sd, sessionKey);
			} catch (Exception e) {
				
			}
			if(user == null) {
				response = Response.status(Status.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, "Smap").entity("Authorisation Error").build();
			} else {
				
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, user));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
				if(!GeneralUtilityMethods.isApiEnabled(sd, user)) {
					throw new ApplicationException(localisation.getString("susp_api"));
				}
		
				int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);	
			
				DataManager dm = new DataManager(localisation, tz);
	
				response = dm.getRecordHierarchy(sd, cResults, request,
						sIdent,
						sId,
						null,
						"yes", 			// If set to yes then do not put choices from select multiple questions in separate objects
						localisation,
						tz,				// Timezone
						true
						);	
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			String resp = "{error: " + e.getMessage() + "}";
			response = Response.serverError().entity(resp).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}
		return response;
	}
	
	/*
	 * Creates a new task
	 *
	@POST
	@Produces("application/json")
	public Response createTask(@Context HttpServletRequest request,
			@QueryParam("sessionKey") String sessionKey,
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("preserveInitialData") boolean preserveInitialData,		// Set true when the initial data for a tasks should not be updated
			@FormParam("task") String task
			) throws ApplicationException, Exception { 
		
		Response response = null;
		String connectionString = "session - Tasks - add new task";
		
		if(task == null) {
			response = Response.serverError().entity("No task has been provided").build();
			return response;
		}
		
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		TaskProperties tp = gson.fromJson(task, TaskProperties.class);	
		
		if(tp == null) {
			response = Response.serverError().entity("Error reading task form data: " + task).build();
			return response;
		}
		
		Connection cResults = null;
		Connection sd = null;		
	
		try {
			sd = SDDataSource.getConnection(connectionString);
			cResults = ResultsDataSource.getConnection(connectionString);
			
			String user = null;
			try {
				user = authoriseSessionKey(sd, sessionKey);
			} catch (Exception e) {
				
			}
			if(user == null) {
				response = Response.status(Status.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, "Smap").entity("Authorisation Error").build();
			} else {
				
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, user));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
				if(!GeneralUtilityMethods.isApiEnabled(sd, user)) {
					throw new ApplicationException(localisation.getString("susp_api"));
				}
		
				if(tp.form_id > 0) {
					a.isValidSurvey(sd, user, tp.form_id, false, false);
				} else {
					a.isValidSurveyIdent(sd, user, tp.survey_ident, false, false);
					tp.form_id = GeneralUtilityMethods.getSurveyId(sd, tp.survey_ident);
				}
				
				if(tp.assignee_ident != null) {
					tp.assignee = GeneralUtilityMethods.getUserId(sd, tp.assignee_ident);
					a.isValidUser(sd, user, tp.assignee);
				}
				
				if(tp.tg_id > 0) {
					a.isValidTaskGroup(sd, user, tp.tg_id);
				}
				// End Authorisation
	
			
				TaskManager tm = new TaskManager(localisation, tz);
				SurveyManager sm = new SurveyManager(localisation, tz);
				if(tp.tg_id <= 0) {
				
					Survey s = sm.getById(sd, cResults, user, false, tp.form_id, 
						false, null, null, false, false, 
						false, false, false, null, false, false, false, null, false, false,
						false		// Don't merge set value into default values
					);
				
					// Create a task group based on the survey
					tp.tg_id = tm.createTaskGroup(sd, s.displayName, 
						s.p_id,
						null,	// address columns
						null,	// setting
						0,	// source survey id
						0,	// Target survey id
						0,	// Download distance
						tp.complete_all,
						tp.assign_auto,
						true		// Use an existing task group of the same name
						);	
				}
			
				tp.survey_ident = GeneralUtilityMethods.getSurveyIdent(sd, tp.form_id);
			
				if(tz == null) {
					tz = "UTC";	// Set default for timezone
				}

				TaskFeature tf = new TaskFeature();
				tf.properties = (TaskProperties) tp;
			
				TaskServerDefn tsd = tm.convertTaskFeature(tf);
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				String urlprefix = request.getScheme() + "://" + request.getServerName();
				CreateTaskResp resp = tm.writeTask(sd, cResults, tp.tg_id, tsd, request.getServerName(), 
					false, 
					oId, 
					true, 
					user,
					false,
					urlprefix,
					preserveInitialData);
			
				response = Response.ok(gson.toJson(resp)).build();
			}
		
		} catch (Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
	
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}
		
		return response;
	}
	
	
	/*
	 * Private functions
	 *
	
	private String authoriseSessionKey(Connection sd, String sessionKey) throws AuthenticationException {
		
		String user = null;
		try {
			user = GeneralUtilityMethods.getDynamicUser(sd, sessionKey);
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
		if(user == null) {
			throw new AuthenticationException();
		}
		a.isAuthorised(sd, user);
		return user;
	}

}
*/

