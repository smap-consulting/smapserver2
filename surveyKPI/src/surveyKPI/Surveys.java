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
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ExternalFileManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.SurveyTableManager;
import org.smap.sdal.model.ChangeElement;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.ChangeResponse;
import org.smap.sdal.model.ChangeSet;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Pulldata;
import org.smap.sdal.model.SurveyIdent;
import org.smap.sdal.model.SurveySummary;
import org.smap.sdal.model.Template;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.lang.reflect.Type;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/surveys")
public class Surveys extends Application {

	Authorise aGet = null;
	Authorise aUpdate = null;
	
	private static Logger log =
			 Logger.getLogger(Surveys.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public Surveys() {
		
		ArrayList<String> authorisations1 = new ArrayList<String> ();
		ArrayList<String> authorisations2 = new ArrayList<String> ();
		
		authorisations1.add(Authorise.ANALYST);
		authorisations1.add(Authorise.VIEW_DATA);
		authorisations1.add(Authorise.ADMIN);
		authorisations1.add(Authorise.ENUM);
		authorisations1.add(Authorise.MANAGE_TASKS);
		
		authorisations2.add(Authorise.ANALYST);
		authorisations2.add(Authorise.ADMIN);
		
		aGet = new Authorise(authorisations1, null);
		aUpdate = new Authorise(authorisations2, null);
		
	}

	@GET
	@Produces("application/json")
	public Response getSurveys(@Context HttpServletRequest request,
			@QueryParam("deleted") boolean getDeleted,
			@QueryParam("blocked")  boolean getBlocked,
			@QueryParam("projectId") int projectId,
			@QueryParam("groups") boolean groups
			) { 
		
		String connectionString = "surveyKPI-Surveys";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		if(getDeleted) {
			aUpdate.isAuthorised(sd, request.getRemoteUser());
		} else {
			aGet.isAuthorised(sd, request.getRemoteUser());
		}
		if(projectId > 0) {
			aGet.isValidProject(sd, request.getRemoteUser(), projectId);
		}
		// End Authorisation
		
		ArrayList<org.smap.sdal.model.Survey> surveys = null;
		
		Response response = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			
			surveys = sm.getSurveys(sd,
					request.getRemoteUser(), 
					getDeleted, 
					getBlocked, 
					projectId,
					superUser,
					groups,
					true,
					false,		// Get oversight and data surveys
					false,		// Get links
					null
					);
			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(surveys);
			response = Response.ok(resp).build();
			
			
		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
			response = Response.serverError().build();
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
	
	/*
	 * Get details on a survey
	 * Used to get the data in order to edit the survey
	 */
	@GET
	@Path("/{sId}")
	@Produces("application/json")
	public Response getSurveyDetails(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("get_changes") boolean getChangeHistory,
			@QueryParam("get_soft_deleted") String stringGetSoftDeleted,
			@QueryParam("tz") String tz
			) { 
		
		String connectionString = "surveyKPI - Get Survey Details";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString );	
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		org.smap.sdal.model.Survey survey = null;
		
		// Get the base path
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		boolean getSoftDeleted = true;		// Boolean
		if(stringGetSoftDeleted != null) {
			if(stringGetSoftDeleted.equals("no") || stringGetSoftDeleted.equals("false")) {
				getSoftDeleted = false;
			}
		}
		
		Response response = null;
		Connection cResults = ResultsDataSource.getConnection(connectionString );
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, tz);
			survey = sm.getById(sd, cResults,  request.getRemoteUser(), false, sId, 
					true, 		// Get full details
					basePath, 
					null, 		// instance id
					false, 		// get results
					false, 		// Generate dummy values
					true, 		// Get property type questions
					getSoftDeleted,	
					false,
					"internal",
					getChangeHistory,
					false,
					superUser,
					null,
					false,		// Do not include child surveys
					false,		// launched only
					true		// merge setValues into default value
					);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(survey);
			response = Response.ok(resp).build();
			
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			SDDataSource.closeConnection(connectionString , sd);	
			ResultsDataSource.closeConnection(connectionString , cResults);
			
		}

		return response;
	}
	
	/*
	 * Get high level details on a survey given its ident
	 */
	@GET
	@Path("/summary/{sIdent}")
	@Produces("application/json")
	public Response getSurveySummary(@Context HttpServletRequest request,
			@PathParam("sIdent") String sIdent
			) { 
		
		String connectionString = "surveyKPI - Get Survey Summary";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString );	
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.surveyInUsersOrganisation(sd, request.getRemoteUser(), sIdent);
		// End Authorisation
		
		Response response = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			SurveySummary summary = sm.getSummary(sd, sIdent);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(summary);
			response = Response.ok(resp).build();			
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {			
			SDDataSource.closeConnection(connectionString , sd);				
		}

		return response;
	}
	
	/*
	 * Get the survey templates
	 */
	@Path("/templates/{sId}")
	@GET
	@Produces("application/json")
	public Response getTemplates(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("get_not_available") boolean getNotAvailable) { 
 
		Response response = null;
		String connectionString = "surveyKPI - Get Survey Templates";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aGet.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String basePath = GeneralUtilityMethods.getBasePath(request);
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			ArrayList<Template> templates = sm.getTemplates(sd, sIdent, basePath, getNotAvailable);
			
			response = Response.ok(gson.toJson(templates)).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Get high level details on a survey given its id
	 */
	@GET
	@Path("/summary/id/{id}")
	@Produces("application/json")
	public Response getSurveySummaryForId(@Context HttpServletRequest request,
			@PathParam("id") int id
			) { 
		
		String connectionString = "surveyKPI - Get Survey Summary for Id";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString );	
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		Response response = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, id);
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			SurveySummary summary = sm.getSummary(sd, sIdent);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(summary);
			response = Response.ok(resp).build();			
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {			
			SDDataSource.closeConnection(connectionString , sd);				
		}

		return response;
	}
	
	/*
	 * Get a list of surveys with their idents that are accessible to a user
	 */
	@GET
	@Path("/idents")
	@Produces("application/json")
	public Response getSurveyIdents(@Context HttpServletRequest request) { 
		
		String connectionString = "surveyKPI - Get Survey Idents";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString );	
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		
		Response response = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			ArrayList<SurveyIdent> surveyIdents = sm.getSurveyIdentList(sd, request.getRemoteUser(), superUser);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(surveyIdents);
			response = Response.ok(resp).build();			
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {			
			SDDataSource.closeConnection(connectionString , sd);				
		}

		return response;
	}
	
	/*
	 * Create a new survey
	 */
	@POST
	@Path("/new/{project}/{name}")
	@Produces("application/json")
	public Response createNewSurvey(@Context HttpServletRequest request,
			@PathParam("project") int projectId,
			@PathParam("name") String name,
			@FormParam("existing") boolean existing,
			@FormParam("existing_survey") int existingSurveyId,
			@FormParam("existing_form") int existingFormId,
			@FormParam("shared_results") boolean sharedResults
			) { 
		
		log.info("userevent: " + request.getRemoteUser() + " create new survey " + name + " (" + existing + "," + 
				existingSurveyId + "," + existingFormId + ")");
		
		// Authorisation - Access
		boolean superUser = false;
		Connection sd = SDDataSource.getConnection("surveyKPI-Surveys");
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidProject(sd, request.getRemoteUser(), projectId);
		if(existing) {
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			aUpdate.isValidSurvey(sd, request.getRemoteUser(), existingSurveyId, false, superUser);	// Validate that the user can access the existing survey
		}
		// End Authorisation
		
		org.smap.sdal.model.Survey survey = null;
		
		// Get the base path
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		log.info("userevent: " + request.getRemoteUser() + " create new survey " + name + " (" + existing + "," + 
				existingSurveyId + "," + existingFormId + ")");
		
		Response response = null;
		Connection cResults = ResultsDataSource.getConnection("surveyKPI-Surveys");

		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			if(sm.surveyExists(sd, name, projectId)) {
				String msg = localisation.getString("tu_ae");
				msg = msg.replaceAll("%s1", name);
				throw new ApplicationException(msg);
			}
			int sId = sm.createNewSurvey(sd, name, projectId, existing, existingSurveyId, sharedResults, request.getRemoteUser());
			// Get the survey details.  superUser set to true as this user just created the survey so they are effectively a super user for this survey and we can save a database call
			survey = sm.getById(sd, 
					cResults,  request.getRemoteUser(), false, sId, true, 
					basePath, null, false, false, true, true, false,
					"internal", false, false, true, null,
					false,		// Do not include child surveys
					false,		// launched only
					true		// Merge set values into default value
					);
			log.info("userevent: " + request.getRemoteUser() + " : create empty survey : " + name + " in project " + projectId);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(survey);
			response = Response.ok(resp).build();
			
			
		} catch(ApplicationException e) {		
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		
		} catch (SQLException e) {
			
			if(e.getMessage().contains("duplicate key")) {
				String msg = "There is already a form called " + name + " in this project";
				response = Response.status(Status.NO_CONTENT).entity(msg).build();
				log.info(msg + e.getMessage());
			} else {
				log.log(Level.SEVERE, "SQL Error", e);
				response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		} finally {
			
			SDDataSource.closeConnection("surveyKPI-Surveys", sd);	
			ResultsDataSource.closeConnection("surveyKPI-Surveys", cResults);
			
		}

		return response;
	}
	

	/*
	 * Update the survey languages
	 */
	@Path("/save_languages/{sId}")
	@POST
	public Response saveLanguages(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@FormParam("languages") String languages) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Surveys");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		PreparedStatement pstmt = null;
		String sqlChangeLog = "insert into survey_change " +
				"(s_id, version, changes, user_id, apply_results, updated_time) " +
				"values(?, ?, ?, ?, 'true', ?)";
		PreparedStatement pstmtChangeLog = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			
			// Start transaction
			sd.setAutoCommit(false);
			
			/*
			 * Lock the survey
			 * update version number of survey and get the new version
			 */
			String sqlUpdateVersion = "update survey set version = version + 1 where s_id = ?";
			pstmt = sd.prepareStatement(sqlUpdateVersion);
			pstmt.setInt(1, sId);
			pstmt.execute();
			pstmt.close();

			int version = GeneralUtilityMethods.getSurveyVersion(sd, sId);
			
			/*
			 * Parse the request
			 */
			Type type = new TypeToken<ArrayList<Language>>(){}.getType();
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			ArrayList<Language> languageList = gson.fromJson(languages, type);
			
			// Update the languages
			GeneralUtilityMethods.setLanguages(sd, sId, languageList);
			GeneralUtilityMethods.setMediaForLanguages(sd, sId, languageList);	// Cope with media being duplicated across all languages
			// Get the survey details.  superUser set to true as this user just edited the survey so they are effectively a super user for this survey and we can save a databse call
			org.smap.sdal.model.Survey  survey = sm.getById(sd, 
					null,  request.getRemoteUser(), false, sId, true, 
					basePath, null, false, false, true, true, false,
					"internal", false, false, true, null,
					false,		// Do not include child surveys
					false,		// launched only
					true		// Merge default values into set value
					);
			
			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);
					
			// Write to the change log
			int userId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
			ChangeElement change = new ChangeElement();
			change.action = "language_update";
			change.origSId = sId;
			StringBuilder msg = new StringBuilder("");
			for(Language l : languageList) {
				if(msg.length() > 0) {
					msg.append(", ");
				}
				msg.append(l.name);
				if(l.code != null) {
					msg.append(" (").append(l.code).append(")");
				}
				if(l.deleted) {
					msg.append(" [").append(localisation.getString("c_del")).append("]");
				} 
			}
			change.msg = msg.toString();
			
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
			pstmtChangeLog.setInt(1, sId);
			pstmtChangeLog.setInt(2, version);
			pstmtChangeLog.setString(3, gson.toJson(change));
			pstmtChangeLog.setInt(4, userId);
			pstmtChangeLog.setTimestamp(5, GeneralUtilityMethods.getTimeStamp());
			pstmtChangeLog.execute();
			
			sd.commit();
			sd.setAutoCommit(true);
			
			String resp = gson.toJson(survey);
			response = Response.ok(resp).build();
			
		} catch (SQLException e) {
			try{sd.rollback();} catch(Exception ex) {};
			log.log(Level.SEVERE,"sql error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} catch (Exception e) {
			try{sd.rollback();} catch(Exception ex) {};
			log.log(Level.SEVERE,"Exception loading settings", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			if (pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (SQLException e) {}
			if (pstmt != null) try {pstmt.close();} catch (SQLException e) {}
			SDDataSource.closeConnection("surveyKPI-Surveys", sd);
			
			try {sd.setAutoCommit(true);} catch(Exception e) {}
		}

		return response;
	}
	
	/*
	 * Save the pulldata key used to get repeating records from dependent forms
	 */
	@Path("/save_pulldata/{sId}")
	@POST
	public Response savePulldata(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@FormParam("pulldata") String pulldata) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Surveys");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
		String sql = "update survey set pulldata = ? where s_id = ?;"; 
		PreparedStatement pstmt = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			/*
			 * Parse the request
			 */
			Type type = new TypeToken<ArrayList<Pulldata>>(){}.getType();
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			ArrayList<Pulldata> pulldataList = gson.fromJson(pulldata, type);
			
			String parsedPd = gson.toJson(pulldataList);
			
			// Update the pulldata settings
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, parsedPd);
			pstmt.setInt(2, sId);
			pstmt.executeUpdate();	

			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);
			
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception updating pulldata settings", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection("surveyKPI-Surveys", sd);
			
		}

		return response;
	}
	
	/*
	 * Apply updates to the survey
	 */
	@PUT
	@Path("/save/{sId}")
	@Produces("application/json")
	public Response saveSurveyDetails(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@FormParam("changes") String changesString
			) { 
		
		log.info("Save survey:" + sId + " : " + changesString);
		
		Type type = new TypeToken<ArrayList<ChangeSet>>(){}.getType();
		Gson gson =  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		ArrayList<ChangeSet> changes = gson.fromJson(changesString, type);	
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Surveys");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());	
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		
		Connection cResults = ResultsDataSource.getConnection("surveyKPI-Surveys");
		
		// Authorise the changes
		for(ChangeSet cs : changes) {
			for (ChangeItem ci : cs.items) {
				// Check that property changes are being applied to questions in the specified survey
				if(ci.property != null) {
					if(!ci.property.type.equals("option") 
							&& !ci.property.type.equals("optionlist")) {
						log.info("Validating question for type: " + ci.property.type);
						aUpdate.isValidQuestion(sd, request.getRemoteUser(), sId, ci.property.qId);
					}
				} 
			}
		}
		// End Authorisation

		Response response = null;

		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			ChangeResponse resp = sm.applyChangeSetArray(sd, cResults, sId, request.getRemoteUser(), changes, true);
			
			// Force regeneration of any dynamic CSV files that this survey links to
			ExternalFileManager efm = new ExternalFileManager(localisation);			
			efm.linkerChanged(sd, sId);	// deprecated
			GeneralUtilityMethods.updateFormDependencies(sd, sId);
			
			SurveyTableManager stm = new SurveyTableManager(sd, localisation);
			stm.delete(sId);			// Delete references to this survey in the csv table so that they get regenerated
			
			String respString = gson.toJson(resp);	// Create the response	
			response = Response.ok(respString).build();
			
			
		}  catch (Exception e) {
			try {sd.rollback();} catch (Exception ex) {};
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			SDDataSource.closeConnection("surveyKPI-Surveys", sd);		
			ResultsDataSource.closeConnection("surveyKPI-Surveys", cResults);
			
		}

		return response;
	}

	/*
	 * Update the survey settings (does not change question / forms etc)
	 */
	@Path("/save_settings/{sId}")
	@POST
	public Response saveSettings(@Context HttpServletRequest request,
			@PathParam("sId") int sId) { 
		
		Response response = null;
		String connectionString = "surveyKPI-Save Settings";
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();	
		fileItemFactory.setSizeThreshold(20*1024*1024);
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
		FileItem pdfItem = null;
		String fileName = null;
		String newSurveyName = null;
		String settings = null;
		String pdfSet = "no";
		int version = 0;
				
		PreparedStatement pstmt = null;
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmtChangeLog = null;
		PreparedStatement pstmtAddHrk = null;
		
		Connection cResults = null;
		try {
				
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();

			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();
				
				if(item.isFormField()) {
					log.info("Form field:" + item.getFieldName() + " - " + item.getString());
				
					
					if(item.getFieldName().equals("settings")) {
						try {
							settings = item.getString("UTF-8");  // Set encoding type to UTF-8 as per http://stackoverflow.com/questions/22025999/sending-files-and-text-with-ajax-multipart-form-data-utf-8-encoding
						} catch (Exception e) {
							
						}
					} else if(item.getFieldName().equals("pdfSet")) {
						try {
							pdfSet = item.getString("UTF-8");  // Set encoding type to UTF-8 as per http://stackoverflow.com/questions/22025999/sending-files-and-text-with-ajax-multipart-form-data-utf-8-encoding
						} catch (Exception e) {
							
						}
					}
					
					
				} else if(!item.isFormField()) {
					// Handle Uploaded files.
					log.info("Field Name = "+item.getFieldName()+
						", File Name = "+item.getName()+
						", Content type = "+item.getContentType()+
						", File Size = "+item.getSize());
					
					if(item.getSize() > 0) {
						pdfItem = item;
						fileName = item.getName();
						fileName = fileName.replaceAll(" ", "_"); // Remove spaces from file name
					}				
				}

			}
			
			Type type = new TypeToken<org.smap.sdal.model.Survey>(){}.getType();
			Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
			org.smap.sdal.model.Survey survey = gson.fromJson(settings, type);
			
			// Start transaction
			sd.setAutoCommit(false);
			
			// Get the existing survey display name, plain old name and project id
			String sqlGet = "select name, display_name, p_id, version, default_logo from survey where s_id = ?";
			pstmtGet = sd.prepareStatement(sqlGet);	
			pstmtGet.setInt(1, sId);
			
			String originalDisplayName = null;
			String originalName = null;
			int originalProjectId = 0;
			String originalDefaultLogo = null;
			ResultSet rs = pstmtGet.executeQuery();
			if(rs.next()) {
				originalName = rs.getString(1);
				originalDisplayName = rs.getString(2);
				originalProjectId = rs.getInt(3);
				version = rs.getInt(4) + 1;
				originalDefaultLogo = rs.getString(5);
			}
			
			if(originalName != null) {
				int idx = originalName.lastIndexOf('/');
				if(idx > 0) {
					newSurveyName = originalName.substring(0, idx + 1) + GeneralUtilityMethods.convertDisplayNameToFileName(survey.displayName, false) + ".xml";
				}
			}
			
			/*
			 * PDF Template
			 * No longer updated through settings
			 */

			String archivedTemplateName = null;
			boolean updatePDFName = true;
			if(fileName != null) {  // Save the file
				
				// Temporary save the old file.  This will no longer be necessary once all clients have uploaded their PDFs with the filename saved to the change log
				renamePdf(request, survey.displayName, survey.displayName + "__prev__", survey.p_id, true);
				
				archivedTemplateName = writePdfLegacy(request, survey.displayName, pdfItem, true, survey.p_id);		
				writePdfLegacy(request, survey.displayName, pdfItem, false, survey.p_id);				// Save the "current" version of the file			
			
			} else if(pdfSet.equals("no")) {
				
				// Temporary save the old file.  This will no longer be necessary once all clients have uploaded their PDFs with the filename saved to the change log
				renamePdf(request, survey.displayName, survey.displayName + "_previous", survey.p_id, true);
				
				// Try to delete the template file if it exists
				delPdf(request, survey.displayName, survey.p_id);
			} else {
				  // If the survey name has been changed then change the name of the template on disk
				if(originalDisplayName != null && !originalDisplayName.equals(survey.displayName)) {
					renamePdf(request, originalDisplayName, survey.displayName, survey.p_id, false);
				}

				updatePDFName = false;	// PDF was not changed
			}
			
			String sqlChangeLog = "insert into survey_change " +
					"(s_id, version, changes, user_id, apply_results, updated_time) " +
					"values(?, ?, ?, ?, 'true', ?)";
			
			// Update the settings
			String sql1 = "update survey set display_name = ?, name = ?, def_lang = ?, task_file = ?, "
					+ "timing_data = ?, "
					+ "p_id = ?, "
					+ "instance_name = ?, "
					+ "version = ?, "
					+ "class = ?,"
					+ "hrk = ?,"
					+ "key_policy = ?, "
					+ "exclude_empty = ?, "
					+ "compress_pdf = ?, "
					+ "hide_on_device = ?, "
					+ "search_local_data = ?, "
					+ "data_survey = ?, "
					+ "oversight_survey = ?, "
					+ "audit_location_data = ?, "
					+ "track_changes = ?,"
					+ "default_logo = ? ";
			String sql2 = ",pdf_template = ? ";
			String sql3 = "where s_id = ?";
			String sql = sql1 + (updatePDFName ? sql2 : "") + sql3;
		
			if(survey.surveyClass != null && survey.surveyClass.equals("none")) {
				survey.surveyClass = null;
			}
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, survey.displayName);
			pstmt.setString(2, newSurveyName);
			pstmt.setString(3, survey.def_lang);
			pstmt.setBoolean(4, survey.task_file);
			pstmt.setBoolean(5, survey.timing_data);
			pstmt.setInt(6, survey.p_id);
			pstmt.setString(7, survey.instanceNameDefn);
			pstmt.setInt(8, version);
			pstmt.setString(9, survey.surveyClass);
			pstmt.setString(10, survey.hrk);
			pstmt.setString(11, survey.key_policy);
			pstmt.setBoolean(12, survey.exclude_empty);
			pstmt.setBoolean(13, survey.compress_pdf);
			pstmt.setBoolean(14, survey.getHideOnDevice());
			pstmt.setBoolean(15, survey.getSearchLocalData());
			pstmt.setBoolean(16, survey.dataSurvey);
			pstmt.setBoolean(17, survey.oversightSurvey);
			pstmt.setBoolean(18, survey.audit_location_data);
			pstmt.setBoolean(19, survey.track_changes);
			pstmt.setString(20, survey.default_logo);
			if(updatePDFName) {
				pstmt.setString(21, fileName);
				pstmt.setInt(22, sId);
			} else {
				pstmt.setInt(21, sId);
			}
			
			log.info("Saving survey: " + pstmt.toString());
			int count = pstmt.executeUpdate();

			if(count == 0) {
				log.info("Error: Failed to update survey");
			} else {
				log.info("Info: Survey updated");
				
				int userId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				
				ChangeElement change = new ChangeElement();
				change.action = "settings_update";
				change.fileName = archivedTemplateName;
				change.origSId = sId;
				change.msg = localisation.getString("name") + ": " + survey.displayName 
						+ ", " + localisation.getString("cr_lang") + ": " + survey.def_lang 
						+ ", " + localisation.getString("a_in") + ": " + survey.instanceNameDefn
						+ ", " + localisation.getString("ar_project") + ": " 
								+ GeneralUtilityMethods.getProjectName(sd, survey.p_id) 
						+ ", " + localisation.getString("cr_key") + ": " + survey.hrk 
						+ ", " + localisation.getString("cr_kp") + ": " + survey.key_policy
						+ ", " + localisation.getString("cr_default_logo") + ": " + survey.default_logo;
				
				// Write to the change log
				pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
				pstmtChangeLog.setInt(1, sId);
				pstmtChangeLog.setInt(2, version);
				pstmtChangeLog.setString(3, gson.toJson(change));
				pstmtChangeLog.setInt(4, userId);
				pstmtChangeLog.setTimestamp(5, GeneralUtilityMethods.getTimeStamp());
				pstmtChangeLog.execute();
			}
			
			// If the human readable key (HRK) is not null then make sure the HRK column exists in the results file
			if(survey.hrk != null) {
				cResults = ResultsDataSource.getConnection(connectionString);
				String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
				if(tableName != null){
					boolean hasHrk = GeneralUtilityMethods.hasColumn(cResults, tableName, "_hrk");
					if(!hasHrk) {
						String sqlAddHrk = "alter table " + tableName + " add column _hrk text;";
						pstmtAddHrk = cResults.prepareStatement(sqlAddHrk);
						pstmtAddHrk.executeUpdate();
					}
				}
			}
			
			sd.commit();
			sd.setAutoCommit(true);
			
			// If the project id has changed update the project in the upload events so that the monitor will still show all events
			if(originalProjectId != survey.p_id) {
				GeneralUtilityMethods.updateUploadEvent(sd, survey.p_id, sId);
			}
			
			// If the display name or project id has changed rename template files
			if((originalDisplayName != null && survey.displayName != null && !originalDisplayName.equals(survey.displayName)) 
					|| originalProjectId != survey.p_id) {
		
				// Rename files
				String basePath = GeneralUtilityMethods.getBasePath(request); 	// Get base path to files
				GeneralUtilityMethods.renameTemplateFiles(originalDisplayName, survey.displayName, basePath, originalProjectId, survey.p_id);
			}
			
			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);
		
			response = Response.ok(fileName).build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE,"sql error", e);
			try{sd.rollback();} catch(Exception ex) {};
		    response = Response.serverError().entity(e.getMessage()).build();
		    try {sd.setAutoCommit(true);} catch(Exception ex) {}
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception loading settings", e);
			try{sd.rollback();} catch(Exception ex) {};
		    response = Response.serverError().entity(e.getMessage()).build();
		    try {sd.setAutoCommit(true);} catch(Exception ex) {}
		} finally {
			
			if (pstmtGet != null) try {pstmtGet.close();} catch (SQLException e) {}
			if (pstmt != null) try {pstmt.close();} catch (SQLException e) {}
			if (pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (SQLException e) {}
			if (pstmtAddHrk != null) try {pstmtAddHrk.close();} catch (SQLException e) {}
			
			try {sd.setAutoCommit(true);} catch(Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}

		return response;
	}
	
	/*
	 * Add a new survey template
	 */
	@Path("/add_template/{sId}")
	@POST
	public Response addTemplate(@Context HttpServletRequest request,
			@PathParam("sId") int sId) { 
		
		Response response = null;
		String connectionString = "SurveyKPI - AddTemplate";
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();	
		fileItemFactory.setSizeThreshold(20*1024*1024);
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
		FileItem pdfItem = null;
		String name = null;
				
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		PreparedStatement pstmtUpdate = null;
		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtChangeLog = null;
		
		try {
				
			// Localisation			
			//Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			//ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();

			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();
				
				if(item.isFormField()) {
					log.info("Form field:" + item.getFieldName() + " - " + item.getString());
		
					if(item.getFieldName().equals("templateName")) {
						try {
							name = item.getString("UTF-8");  // Set encoding type to UTF-8 as per http://stackoverflow.com/questions/22025999/sending-files-and-text-with-ajax-multipart-form-data-utf-8-encoding
						} catch (Exception e) {
							
						}
					}
					
				} else if(!item.isFormField()) {
					// Handle Uploaded files.
					log.info("Field Name = "+item.getFieldName()+
						", File Name = "+item.getName()+
						", Content type = "+item.getContentType()+
						", File Size = "+item.getSize());
					
					if(item.getSize() > 0) {
						pdfItem = item;
					}				
				}

			}
			
			if(pdfItem != null && name != null) {

				String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);							
				String filepath = writePdf(request, name, pdfItem, true, sIdent);	
				int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				
				String sqlChangeLog = "insert into survey_change " +
						"(s_id, version, changes, user_id, apply_results, updated_time, msg) " +
						"values(?, (select version from survey where ident = ?), ?, ?, 'true', now(), ?)";
				
				// Update a Survey Template
				String sqlUpdate = "update survey_template "
						+ "set filepath = ?, "
						+ "user_id = ?, "
						+ "updated_time = now() "
						+ "where ident = ? "
						+ "and name = ?";
				
				// Insert a new survey template
				String sqlInsert = "insert into survey_template "
						+ "(ident, name, filepath, template_type, user_id, updated_time) "
						+ "values(?, ?, ?, ?, ?, now()) ";
		
				
				/*
				 * Try updating the survey template first
				 */
				String action = "template_update";
				pstmtUpdate = sd.prepareStatement(sqlUpdate);
				pstmtUpdate.setString(1, filepath);
				pstmtUpdate.setInt(2,  uId);
				pstmtUpdate.setString(3, sIdent);
				pstmtUpdate.setString(4, name);
				
				log.info("Updating template: " + pstmtUpdate.toString());
				int count = pstmtUpdate.executeUpdate();
	
				if(count == 0) {
					/*
					 * Insert the new template
					 */
					pstmtInsert = sd.prepareStatement(sqlInsert);
					pstmtInsert.setString(1, sIdent);
					pstmtInsert.setString(2,  name);
					pstmtInsert.setString(3, filepath);
					pstmtInsert.setString(4, "pdf");
					pstmtInsert.setInt(5,  uId);
					
					log.info("Inserting template: " + pstmtInsert.toString());
					pstmtInsert.executeUpdate();
					
					action = "template_add";
				} 
					
				ChangeElement change = new ChangeElement();
				change.action = action;
				change.fileName = filepath;
				change.origSId = sId;
				
				// Write to the change log
				pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
				pstmtChangeLog.setInt(1, sId);
				pstmtChangeLog.setString(2, sIdent);
				pstmtChangeLog.setString(3, gson.toJson(change));
				pstmtChangeLog.setInt(4, uId);
				pstmtChangeLog.setString(5, name);
				pstmtChangeLog.execute();
			
				response = Response.ok("{}").build();
			} else {
				 response = Response.serverError().entity("Template not specified").build();
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception loading settings", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		    try {sd.setAutoCommit(true);} catch(Exception ex) {}
		} finally {
			
			if (pstmtUpdate != null) try {pstmtUpdate.close();} catch (SQLException e) {}
			if (pstmtInsert != null) try {pstmtInsert.close();} catch (SQLException e) {}
			if (pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (SQLException e) {}

			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
	
	/*
	 * Delete a template
	 */
	@Path("/delete_template/{sId}/{tId}")
	@DELETE
	public Response deleteTemplate(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("tId") int tId) { 
		
		Response response = null;
		String connectionString = "SurveyKPI - DeleteTemplate";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		if(tId > 0) {
			aUpdate.isValidPdfTemplate(sd, request.getRemoteUser(), tId);
		}
		// End Authorisation
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmtChangeLog = null;
		PreparedStatement pstmtSurvey = null;
		PreparedStatement pstmtGetLegacy = null;
		
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		try {
				
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			/*
			 * Delete the entry for the file in the table
			 * The file itself will not be deleted until the survey is deleted so
			 * that it can be recovered using the changes page
			 */
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);								
			int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
			String name = null;
			SurveyManager sm = new SurveyManager(localisation, "UTC");
				
			String sqlChangeLog = "insert into survey_change " +
					"(s_id, version, changes, user_id, apply_results, updated_time, msg) " +
					"values(?, (select version from survey where ident = ?), ?, ?, 'true', now(), ?)";
				
			String sqlGet = "select name, filepath "
					+ "from survey_template "
					+ "where t_id = ? and ident = ?";
			
			String sql = "delete from survey_template "
					+ "where t_id = ? and ident = ? ";
				
			String sqlSurvey = "update survey "
					+ "set pdf_template = null "
					+ "where s_id = ?";
			
			String sqlGetLegacy = "select pdf_template, p_id "
					+ "from survey "
					+ "where s_id = ?";
			
			if(tId == 0) {
				// Get the filepath and name for the change log
				pstmtGetLegacy = sd.prepareStatement(sqlGetLegacy);
				pstmtGetLegacy.setInt(1, sId);
				
				ResultSet rs = pstmtGetLegacy.executeQuery();
				if(rs.next()) {
					name = rs.getString(1);
				}
					
				// Delete the template
				pstmtSurvey = sd.prepareStatement(sqlSurvey);
				pstmtSurvey.setInt(1, sId);
				pstmtSurvey.executeUpdate();
				
				File templateFile = sm.getLegacyPdfTemplateFile(sd, sIdent, basePath);
				if(templateFile != null && templateFile.exists()) {
					templateFile.delete();
				}
			} else {
				// Get the filepath and name for the change log
				pstmtGet = sd.prepareStatement(sqlGet);
				pstmtGet.setInt(1, tId);
				pstmtGet.setString(2,  sIdent);

				ResultSet rs = pstmtGet.executeQuery();				
				if(rs.next()) {
					name = rs.getString("name");
				}

				// Delete the pdf template
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, tId);
				pstmt.setString(2,  sIdent);

				log.info("deleting pdf template: " + pstmt.toString());
				pstmt.executeUpdate();


			}
			ChangeElement change = new ChangeElement();
			change.action = "template_delete";
			change.fileName = null;		// Do not need to show path to deleted files only added files
			change.origSId = sId;

			// Write to the change log
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
			pstmtChangeLog.setInt(1, sId);
			pstmtChangeLog.setString(2, sIdent);
			pstmtChangeLog.setString(3, gson.toJson(change));
			pstmtChangeLog.setInt(4, uId);
			pstmtChangeLog.setString(5, name);
			pstmtChangeLog.execute();

			response = Response.ok("{}").build();


		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception loading settings", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			if (pstmt != null) try {pstmt.close();} catch (SQLException e) {}
			if (pstmtGet != null) try {pstmtGet.close();} catch (SQLException e) {}
			if (pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (SQLException e) {}
			if (pstmtSurvey != null) try {pstmtSurvey.close();} catch (SQLException e) {}
			if (pstmtGetLegacy != null) try {pstmtGetLegacy.close();} catch (SQLException e) {}

			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
	
	class TemplateProperty {
		public int id;
		public String property;
		public String value;
	}
	
	/*
	 * Update a template property
	 */
	@Path("/update_template_property")
	@POST
	public Response updateTemplateProperty(@Context HttpServletRequest request,
			@FormParam("prop") String prop) { 
		
		Response response = null;
		String connectionString = "SurveyKPI - UpdateTemplateProperty";
	
		TemplateProperty tp = new Gson().fromJson(prop, TemplateProperty.class);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidPdfTemplate(sd, request.getRemoteUser(), tp.id);	// Validate that the user can access this template
		// End Authorisation

		/*
		 * Validate property
		 */
		String col = null;
		boolean value = false;
		if(tp.property.equals("not_available") || tp.property.equals("default_template")
				|| tp.property.equals("rule")) {
			
			col = tp.property;
			
			if(tp.value != null && (tp.property.equals("not_available") || tp.property.equals("default_template"))) {
				if(tp.value.toLowerCase().equals("true") || tp.value.equals("1") || tp.value.toLowerCase().equals("yes")) {
					value = true;
				}
			}
		}
		if(col != null) {
				
			String sqlClear = "update survey_template "
					+ "set " + col + " = false "
					+ "where ident = (select ident from survey_template where t_id = ?) ";
			PreparedStatement pstmtClear = null;
			
			/*
			 * Update the property
			 */
			String sql = "update survey_template "
					+ "set " + col + " = ?, "
					+ "user_id = ?, "
					+ "updated_time = now() "
					+ "where t_id = ? ";
			PreparedStatement pstmt = null;
			
			try {
				/*
				 * Clear defaults if required
				 */
				if(tp.property.equals("default_template")) {
					pstmtClear = sd.prepareStatement(sqlClear);
					pstmtClear.setInt(1, tp.id);
					pstmtClear.executeUpdate();
				}
				
				int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				pstmt = sd.prepareStatement(sql);
				if(tp.property.equals("not_available") || tp.property.equals("default_template")) {
					pstmt.setBoolean(1,  value);
				} else {
					pstmt.setString(1,  tp.value);
				}
				pstmt.setInt(2, uId);
				pstmt.setInt(3,  tp.id);
				log.info("Update pdf template property: " + pstmt.toString());
				pstmt.executeUpdate();
				
				response = Response.ok("{}").build();
				
			} catch (Exception e) {
				log.log(Level.SEVERE,"Exception loading settings", e);
			    response = Response.serverError().entity(e.getMessage()).build();
			} finally {
				if (pstmt != null) try {pstmt.close();} catch (SQLException e) {}
				if (pstmtClear != null) try {pstmtClear.close();} catch (SQLException e) {}
			}
		}
		
		return response;
	}
	
	/*
	 * Add a meta item
	 */
	@Path("/add_meta/{sIdent}")
	@POST
	public Response saveMetaItem(@Context HttpServletRequest request,
			@PathParam("sIdent") String sIdent,
			@FormParam("item") String metaString) { 
		
		Response response = null;
		
		String connectionString = "SurveyKPI=Survey-AddMeta";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurveyIdent(sd, request.getRemoteUser(), sIdent, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
		MetaItem item  = new Gson().fromJson(metaString, MetaItem.class);
		if(item.columnName == null) {
			item.columnName = GeneralUtilityMethods.cleanName(item.name, true, true, false);
		}
		item.isPreload = true;
		if(item.type == null) {
			if(item.sourceParam.equals("start") || item.sourceParam.equals("end")) {
				item.type = "dateTime";
				item.dataType = "timestamp";
			} else if(item.sourceParam.equals("today")) {
				item.type = "date";
				item.dataType = "date";
			} else if(item.sourceParam.equals("start-geopoint")) {
				item.type = "geopoint";
				item.dataType = "geopoint";
			} else if(item.sourceParam.equals("background-audio")) {
				item.type = "audio";
				item.dataType = "background-audio";
			} else {
				item.type = "string";
				item.dataType = "property";
			}
		}
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtChangeLog = null;
		int version;
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			/*
			 * Lock the survey
			 * update version number of survey and get the new version
			 */
			sd.setAutoCommit(false);
			
			String sqlUpdateVersion = "update survey set version = version + 1 where ident = ?";
			String sqlGetVersion = "select version from survey where ident = ?";
			pstmt = sd.prepareStatement(sqlUpdateVersion);
			pstmt.setString(1, sIdent);
			pstmt.execute();
			pstmt.close();
			
			pstmt = sd.prepareStatement(sqlGetVersion);
			pstmt.setString(1, sIdent);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			version = rs.getInt(1);
			pstmt.close();
			
			int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
			ArrayList<MetaItem> preloads = GeneralUtilityMethods.getPreloads(sd, sId);
			
			if(item.display_name != null && item.display_name.trim().isEmpty()) {
				item.display_name = null;
			}
			/*
			 * Loop though the existing meta items and update or replace
			 */
			boolean replace = false;
			int id = MetaItem.INITIAL_ID;
			for(MetaItem mi : preloads) {
				
				if(mi.id <= id) {			// Get a unique id if we need to insert a preload
					id = mi.id - 1;
				}
				
				if(mi.isPreload && mi.sourceParam.equals(item.sourceParam)) {
					mi.name = item.name;
					mi.display_name = item.display_name;
					mi.settings = item.settings;
					replace = true;
					break;
				} 
			}
			
			if(!replace) {
				item.id = id;			
				preloads.add(item);
			}
			GeneralUtilityMethods.setPreloads(sd, sId, preloads);
			
			// Write the change log
			int userId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				
			ChangeElement change = new ChangeElement();
			change.action = "add_preload";
			change.msg = localisation.getString("cr_add_preload");
			change.msg = change.msg.replace("%s1", item.columnName);
			change.msg = change.msg.replace("%s2", item.sourceParam);
			
			// Write to the change log
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String sqlChangeLog = "insert into survey_change " +
					"(s_id, version, changes, user_id, apply_results, updated_time) " +
					"values(?, ?, ?, ?, 'true', ?)";
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
				
			// Write the change log
			pstmtChangeLog.setInt(1, sId);
			pstmtChangeLog.setInt(2, version);
			pstmtChangeLog.setString(3, gson.toJson(change));
			pstmtChangeLog.setInt(4, userId);
			pstmtChangeLog.setTimestamp(5, GeneralUtilityMethods.getTimeStamp());
			pstmtChangeLog.execute();

			sd.commit();
			sd.setAutoCommit(true);
			
			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);
		
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception loading settings", e);
			try {sd.setAutoCommit(true);} catch(Exception ex) {}
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			if (pstmt != null) try {pstmt.close();} catch (SQLException e) {}
			if (pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
	
	/*
	 * Delete a meta item
	 */
	@Path("/meta/{sIdent}/{ident}")
	@DELETE
	public Response deleteMetaItem(@Context HttpServletRequest request,
			@PathParam("sIdent") String sIdent,
			@PathParam("ident") String ident) { 
		
		Response response = null;
		
		String connectionString = "SurveyKPI=Survey-AddMeta";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurveyIdent(sd, request.getRemoteUser(), sIdent, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtChangeLog = null;
		int version;
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			/*
			 * Lock the survey
			 * update version number of survey and get the new version
			 */
			sd.setAutoCommit(false);
			
			String sqlUpdateVersion = "update survey set version = version + 1 where ident = ?";
			String sqlGetVersion = "select version from survey where ident = ?";
			pstmt = sd.prepareStatement(sqlUpdateVersion);
			pstmt.setString(1, sIdent);
			pstmt.execute();
			pstmt.close();
			
			pstmt = sd.prepareStatement(sqlGetVersion);
			pstmt.setString(1, sIdent);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			version = rs.getInt(1);
			pstmt.close();
			
			int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
			ArrayList<MetaItem> preloads = GeneralUtilityMethods.getPreloads(sd, sId);
			
			ArrayList<MetaItem> newPreloads = new ArrayList<> ();
			for(MetaItem mi : preloads) {
				if(mi.isPreload && mi.sourceParam != null && !mi.sourceParam.equals(ident)) {
					newPreloads.add(mi);
				} else if(!mi.isPreload && !mi.name.equals(ident)) {
					newPreloads.add(mi);
				}
			}
			
			GeneralUtilityMethods.setPreloads(sd, sId, newPreloads);
			
			// Write the change log
			int userId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				
			ChangeElement change = new ChangeElement();
			change.action = "del_preload";
			change.msg = localisation.getString("cr_del_preload");
			change.msg = change.msg.replace("%s1", ident);
			
			// Write to the change log
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String sqlChangeLog = "insert into survey_change " +
					"(s_id, version, changes, user_id, apply_results, updated_time) " +
					"values(?, ?, ?, ?, 'true', ?)";
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
				
			// Write the change log
			pstmtChangeLog.setInt(1, sId);
			pstmtChangeLog.setInt(2, version);
			pstmtChangeLog.setString(3, gson.toJson(change));
			pstmtChangeLog.setInt(4, userId);
			pstmtChangeLog.setTimestamp(5, GeneralUtilityMethods.getTimeStamp());
			pstmtChangeLog.execute();

			sd.commit();
			sd.setAutoCommit(true);
			
			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);
			
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception loading settings", e);
			try {sd.setAutoCommit(true);} catch(Exception ex) {}
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			if (pstmt != null) try {pstmt.close();} catch (SQLException e) {}
			if (pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (SQLException e) {}

			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
	
	/*
	 * Set questions to required
	 */
	@Path("/set_required/{sId}/{required}")
	@POST
	public Response setRequired(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("required") boolean required) { 
		
		Response response = null;
		int version;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Survey");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
				
		PreparedStatement pstmtNotRequired = null;
		PreparedStatement pstmtRequired = null;
		PreparedStatement pstmtChangeLog = null;
		PreparedStatement pstmt = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			/*
			 * Lock the survey
			 * update version number of survey and get the new version
			 */
			sd.setAutoCommit(false);
			
			String sqlUpdateVersion = "update survey set version = version + 1 where s_id = ?";
			String sqlGetVersion = "select version from survey where s_id = ?";
			pstmt = sd.prepareStatement(sqlUpdateVersion);
			pstmt.setInt(1, sId);
			pstmt.execute();
			pstmt.close();
			
			pstmt = sd.prepareStatement(sqlGetVersion);
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			version = rs.getInt(1);
			pstmt.close();
			

			// Set all questions to not required
			// Do this even if the next step is to set questions that should be 
			//  required as required
			String sqlNotRequired = "update question set mandatory = 'false' "
					+ "where f_id in (select f_id from form where s_id = ?);";
			pstmtNotRequired = sd.prepareStatement(sqlNotRequired);	
			pstmtNotRequired.setInt(1, sId);

			log.info("SQL: Setting questions not required: " + pstmtNotRequired.toString());
			pstmtNotRequired.executeUpdate();

			if(required) {
				// Set all questions to required
				String sqlRequired = "update question set mandatory = 'true' "
						+ "where readonly = 'false' "
						+ "and visible = 'true' "
						+ "and qtype != 'begin repeat' "
						+ "and qtype != 'begin group' "
						+ "and qtype != 'geopolygon' "
						+ "and qtype != 'geolinestring' "
						+ "and qtype != 'note' "
						+ "and (appearance is null or appearance not like '%label%' or appearance like '%nolabel') "
						+ "and f_id in (select f_id from form where s_id = ?);"; 
			
				pstmtRequired = sd.prepareStatement(sqlRequired);	
				pstmtRequired.setInt(1, sId);
				
				log.info("SQL: Setting questions required: " + pstmtRequired.toString());
				pstmtRequired.executeUpdate();
			}
				
			// Write the change log
			int userId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				
			ChangeElement change = new ChangeElement();
			change.action = "set_required";
			change.msg = required ? "Questions set required" : "Questions set not required"; 
				
			// Write to the change log
			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String sqlChangeLog = "insert into survey_change " +
					"(s_id, version, changes, user_id, apply_results, updated_time) " +
					"values(?, ?, ?, ?, 'true', ?)";
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
				
			// Write the change log
			pstmtChangeLog.setInt(1, sId);
			pstmtChangeLog.setInt(2, version);
			pstmtChangeLog.setString(3, gson.toJson(change));
			pstmtChangeLog.setInt(4, userId);
			pstmtChangeLog.setTimestamp(5, GeneralUtilityMethods.getTimeStamp());
			pstmtChangeLog.execute();

			sd.commit();
			sd.setAutoCommit(true);
			
			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);
		
			response = Response.ok().build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE,"sql error", e);
		    try {sd.setAutoCommit(true);} catch(Exception ex) {}
		    response = Response.serverError().entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception loading settings", e);
		    try {sd.setAutoCommit(true);} catch(Exception ex) {}
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			if (pstmtNotRequired != null) try {pstmtNotRequired.close();} catch (SQLException e) {}
			if (pstmtRequired != null) try {pstmtRequired.close();} catch (SQLException e) {}
			if (pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (SQLException e) {}
			if (pstmt != null) try {pstmt.close();} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-Survey", sd);
			
		}

		return response;
	}
	
	/*
	 * Translate a survey
	 */
	@PUT
	@Path("/translate/{sId}/{from}/{to}/{fromCode}/{toCode}")
	public Response translateSurvey(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("from") int fromLanguageIndex,
			@PathParam("to") int toLanguageIndex,
			@PathParam("fromCode") String fromCode,
			@PathParam("toCode") String toCode,
			@QueryParam("overwrite") boolean overwrite
			) { 
		
		log.info("translate survey:" + sId + " : " + fromCode + " : " + toCode);
		String connectionString = "surey-KPI - Translate Survey";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString );
		aUpdate.isAuthorised(sd, request.getRemoteUser());	
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, true);
		// End Authorisation

		Response response = null;

		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String basePath = GeneralUtilityMethods.getBasePath(request);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");			
			String result = sm.translate(sd, request.getRemoteUser(), sId,
					fromLanguageIndex,
					toLanguageIndex,
					fromCode,
					toCode,
					overwrite,
					basePath);
			
			if(result != null) {
				response = Response.serverError().entity(
						localisation.getString(result).replace("%s1",  LogManager.TRANSLATE)).build();	
			} else {
				response = Response.ok("").build();
			}
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);		
			
		}

		return response;
	}
	/*
	 * Legacy method to Write the PDF to disk
	 * Return the name
	 */
	private String writePdfLegacy(HttpServletRequest request, 
			String fileName, 
			FileItem pdfItem,
			boolean archiveVersion,		// If set then add date time to file so it can be recovered
			int pId) {
	
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		fileName = GeneralUtilityMethods.convertDisplayNameToFileName(fileName, false);
		if(archiveVersion) {
			// Add date and time to the display name
			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HHmmss");
			dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));		// Store all dates in UTC
			fileName += dateFormat.format(cal.getTime());
		}
		fileName += "_template.pdf";
		
		String folderPath = basePath + "/templates/" + pId ;						
		String filePath = folderPath + "/" + fileName;
	    File savedFile = new File(filePath);
	    
	    log.info("userevent: " + request.getRemoteUser() + " : saving pdf template : " + filePath);
	    
	    try {
	    	FileUtils.forceMkdir(new File(folderPath));
			pdfItem.write(savedFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	 
	    return fileName;
	}
	
	/*
	 * Write the PDF to disk
	 * Return the name
	 */
	private String writePdf(HttpServletRequest request, 
			String fileName, 
			FileItem pdfItem,
			boolean archiveVersion,		// If set then add date time to file so it can be recovered
			String sIdent) {
	
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		fileName = GeneralUtilityMethods.convertDisplayNameToFileName(fileName, false);

		// Add date and time to the file name
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HHmmss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));		// Store all dates in UTC
		fileName += dateFormat.format(cal.getTime());
		fileName += ".pdf";
		
		String folderPath = basePath + "/templates/survey/" + sIdent ;						
		String filePath = folderPath + "/" + fileName;
	    File savedFile = new File(filePath);
	    
	    log.info("userevent: " + request.getRemoteUser() + " : saving pdf template : " + filePath);
	    
	    try {
	    	FileUtils.forceMkdir(new File(folderPath));
			pdfItem.write(savedFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	 
	    return filePath;
	}
	
	/*
	 * Delete the pdf template
	 */
	private void delPdf(HttpServletRequest request, 
			String fileName, 
			int pId) {
	
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		fileName = GeneralUtilityMethods.convertDisplayNameToFileName(fileName, false);
		fileName = fileName + "_template.pdf";
		
		String folderPath = basePath + "/templates/" + pId ;						
		String filePath = folderPath + "/" + fileName;
	    File delFile = new File(filePath);
	    
	    log.info("userevent: " + request.getRemoteUser() + " : delete pdf template : " + filePath);

	    try {
			delFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	/*
	 * Rename the pdf template
	 */
	private String renamePdf(HttpServletRequest request, 
			String originalName,
			String newName, 
			int pId,
			boolean copy) {
	
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		newName = GeneralUtilityMethods.convertDisplayNameToFileName(newName, false) + "_template.pdf";
		originalName = GeneralUtilityMethods.convertDisplayNameToFileName(originalName, false) + "_template.pdf";
		
		String folderPath = basePath + "/templates/" + pId ;						
		String newFilePath = folderPath + "/" + newName;
		String originalFilePath = folderPath + "/" + originalName;
	    
		File newFile = new File(newFilePath);
		File renFile = new File(originalFilePath);
	   
	    try {
	    	if(copy) {
	    		FileUtils.copyFile(renFile, newFile);
	    	} else {
	    		renFile.renameTo(newFile);
	    	}
		} catch (Exception e) {
			e.printStackTrace();
		}
	 
	    return newName;
	}
	

	
	
}

