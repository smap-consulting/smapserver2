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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.LogonLimiter;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveySettingsManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Alert;
import org.smap.sdal.model.ConsoleSettings;
import org.smap.sdal.model.GroupSurvey;
import org.smap.sdal.model.PasswordDetails;
import org.smap.sdal.model.User;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Manages the logged in user's details
 */
@Path("/user")
public class UserSvc extends Application {
	
	// Authorisation not required

	private static Logger log =
			 Logger.getLogger(UserSvc.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	@GET
	@Produces("application/json")
	public Response getUserDetails(@Context HttpServletRequest request) { 

		Response response = null;

		// Authorisation 
		Connection sd = SDDataSource.getConnection("surveyKPI-UserSvc");
		try {
			LogonLimiter.isPermitted(request, sd, lm);
			
			User user = null;
			try {
				// Localisation			
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				ServerSettings.setBasePath(request);	// Make sure basePath is in settings but we don't need it right away sp ignore returned response
				
				UserManager um = new UserManager(localisation);
				user = um.getByIdent(sd, request.getRemoteUser());

				Gson gson = new GsonBuilder().disableHtmlEscaping().create();
				String resp = gson.toJson(user);
				response = Response.ok(resp).build();
				
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
				response = Response.serverError().build();
			} 
			
		} catch(Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			return response;
		} finally {
			SDDataSource.closeConnection("surveyKPI-UserSvc", sd);
		}
		
		return response;
	}
	
	/*
	 * Get alerts assigned to a user
	 */
	@GET
	@Path("/alerts")
	@Produces("application/json")
	public Response getMyAlerts(@Context HttpServletRequest request) { 

		Response response = null;

		// Authorisation - Not required
		String connectionString = "surveyKPI-UserSvc-getAlerts";
		Connection sd = SDDataSource.getConnection(connectionString);
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			UserManager um = new UserManager(localisation);
			ArrayList<Alert> alerts = um.getAlertsByIdent(sd, request.getRemoteUser());

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(alerts);
			response = Response.ok(resp).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		

		return response;
	}
	
	class KeyDetails {
		String apiKey;
	}
	
	/*
	 * Get the user's API key
	 * In all cases these are called by a user to on their own keys and authorisation is not requried
	 */	
	@GET
	@Path("/api_key")
	@Produces("application/json")
	public Response getMyApiKey(@Context HttpServletRequest request) { 

		Response response = null;
		String connectionString = "surveyKPI-UserSvc-GetApiKey";

		// Authorisation - Not required
		Connection sd = SDDataSource.getConnection(connectionString);
		
		try {
			KeyDetails key = new KeyDetails();
			UserManager um = new UserManager(null);
			key.apiKey = um.getKey(sd, request.getRemoteUser(), "api");

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(key);
			response = Response.ok(resp).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	
	@DELETE
	@Path("/api_key")
	public Response deleteMyApiKey(@Context HttpServletRequest request) { 

		Response response = null;
		String connectionString = "surveyKPI-UserSvc-DeleteApiKey";

		// Authorisation - Not required
		Connection sd = SDDataSource.getConnection(connectionString);
		
		try {
			UserManager um = new UserManager(null);
			um.deleteKey(sd, request.getRemoteUser(), "api");

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
	 * Create a new API key
	 */
	@POST
	@Path("/api_key/create")
	public Response createApiKey(@Context HttpServletRequest request) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		KeyDetails key = new KeyDetails();
		String authString = "surveyKPI-UserSvc-CreateApiKey";
		
		// Authorisation - Not Required - the user is updating their own settings
		Connection sd = SDDataSource.getConnection(authString);
		
		try {	

			UserManager um = new UserManager(null);
			key.apiKey = um.createKey(sd, request.getRemoteUser(), "api");
				
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(key);
			response = Response.ok(resp).build();
				
		} catch (Exception e) {

			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			SDDataSource.closeConnection(authString, sd);
		}
		
		return response;
	}
	
	/*
	 * Update the user settings
	 */
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response updateUser(@Context HttpServletRequest request,
			@FormDataParam("user") String user) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String authString = "SurveyKpi - Change Settings";

		// Authorisation - Not Required - the user is updating their own settings
		Connection sd = SDDataSource.getConnection(authString);
		
		Type type = new TypeToken<User>(){}.getType();		
		User u = new Gson().fromJson(user, type);		// The user settings
		
		PreparedStatement pstmt = null;
		try {	
			
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// Ensure email is null if it has not been set
			if(u.email != null && u.email.trim().isEmpty()) {
				u.email = null;
			}
			
			boolean updateOrg = false;
			if(u.o_id > 0) {
				updateOrg = true;
			} 
			
			if(updateOrg) {
				/*
				 * The user is moving themselves to a different organisation
				 * If the new settings are null then they are not authorised to move to this new organisation
				 */				
				UserManager um = new UserManager(localisation);
				um.switchUsersOrganisation(sd, u.o_id,	request.getRemoteUser(), true);			
			}			

			/*
			 * Update what can be updated by the user, excluding the current project id, survey id, form id and task group
			 * Password is not updated
			 */
			String sql = null;
			String ident = request.getRemoteUser();
			sql = "update users set "
					+ "name = ?, "
					+ "settings = ?, "
					+ "language = ?, "
					+ "email = ?, "
					+ "timezone = ? "
					+ "where "
					+ "ident = ?";
					
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, HtmlSanitise.checkCleanName(u.name, localisation));
			pstmt.setString(2, u.settings);
			pstmt.setString(3, HtmlSanitise.checkCleanName(u.language, localisation));
			pstmt.setString(4, HtmlSanitise.checkCleanName(u.email, localisation));
			pstmt.setString(5, HtmlSanitise.checkCleanName(u.timezone, localisation));
			pstmt.setString(6, ident);
			
			log.info("Update user details: " + pstmt.toString());
			pstmt.executeUpdate();
			
			// Write logs
			log.info("userevent: " + request.getRemoteUser() + (u.password == null ? " : updated user details : " : " : updated password : ") + u.name);
			lm.writeLog(sd, -1, request.getRemoteUser(), LogManager.USER_DETAILS, localisation.getString("msg_ud_changed"), 0, request.getServerName());
			
			UserManager um = new UserManager(localisation);
			User userResp = um.getByIdent(sd, request.getRemoteUser());

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(userResp);
			response = Response.ok(resp).build();
			
			
		} catch (ApplicationException e) {
			String msg = "{\"error\": true, \"msg\": \"" + e.getMessage() + "\"}";
			response = Response.ok(msg).build();
			
		} catch (Exception e) {

			response = Response.serverError().build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(authString, sd);
		}
		
		return response;
	}
	
	/*
	 * Update the user password
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/password")
	public Response updateUserPassword(@Context HttpServletRequest request,
			@FormParam("passwordDetails") String passwordDetails) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String authString = "surveyKPI - change password";
		
		// Authorisation - Not Required - the user is updating their own settings
		Connection sd = SDDataSource.getConnection(authString);
		
		Type type = new TypeToken<PasswordDetails>(){}.getType();		
		PasswordDetails pwd = new Gson().fromJson(passwordDetails, type);		// The user settings
		
		try {	
			
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		

			UserManager um = new UserManager(localisation);
			response = um.setPassword(sd, locale, localisation, request.getRemoteUser(), request.getServerName(), pwd);
					
			response = Response.ok().build();
			
			
		} catch (Exception e) {

			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			SDDataSource.closeConnection(authString, sd);
		}
		
		return response;
	}
	
	/*
	 * Update the current survey, project and task group
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("/currentproject")
	public Response updateCurrentProject(@Context HttpServletRequest request, User u) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString  = "SurveyKPI - current project";

		// Authorisation - Not Required
		Connection sd = SDDataSource.getConnection(connectionString);
			
		PreparedStatement pstmt = null;
		try {	
			
			// Localisation			
			//Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			//ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
		
			//User u = new Gson().fromJson(user, User.class);		// The user settings
			
			String sql = null;
			if(u.current_project_id > 0) {
				sql = "update users set current_project_id = ?, "
						+ "current_survey_id = ?, "
						+ "current_survey_ident = (select ident from survey where s_id = ?), "
						+ "current_task_group_id = ? "
						+ "where ident = ?";
			} else if(u.current_survey_id > 0) {
				// Only update the survey id
				sql = "update users set current_survey_id = ?, "
						+ "current_survey_ident = (select ident from survey where s_id = ?) "
						+ "where ident = ?";
			} else if(u.current_task_group_id > 0) {
				// Only update the task group id
				sql = "update users set current_task_group_id = ? where ident = ?";
			}
						
			pstmt = sd.prepareStatement(sql);
			if(u.current_project_id > 0) {
				pstmt.setInt(1, u.current_project_id);
				pstmt.setInt(2, u.current_survey_id);
				pstmt.setInt(3, u.current_survey_id);
				pstmt.setInt(4, u.current_task_group_id);
				pstmt.setString(5, request.getRemoteUser());
			} else if(u.current_survey_id > 0) {
				pstmt.setInt(1, u.current_survey_id);
				pstmt.setInt(2, u.current_survey_id);
				pstmt.setString(3, request.getRemoteUser());
			} else if(u.current_task_group_id > 0) {
				pstmt.setInt(1, u.current_task_group_id);
				pstmt.setString(2, request.getRemoteUser());
			}
				
			int count = pstmt.executeUpdate();
			if(count == 0) {
				log.info("Failed to update current project id and survey id");
			}  

			response = Response.ok().build();
				
		} catch (Exception e) {

			response = Response.serverError().build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Update the current group survey
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/groupsurvey")
	public Response updateGroupSurvey(@Context HttpServletRequest request, String group) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "SurveyKPI - save groupsurvey";
		// Authorisation - Not Required
		Connection sd = SDDataSource.getConnection(connectionString);
		
		GroupSurvey gs = new Gson().fromJson(group, GroupSurvey.class);
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtDel = null;
		try {	
			
			// Localisation			
			//Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			//ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
		
			String sqlDel = "delete from group_survey "
						+ "where u_ident = ? "
						+ "and s_id = ? ";
			pstmtDel = sd.prepareStatement(sqlDel);
			
			String sql = "insert into group_survey (u_ident, s_id, group_ident, f_name) "
					+ "values (?, ?, ?, ?) ";
			pstmt = sd.prepareStatement(sql);
			
			pstmtDel.setString(1, request.getRemoteUser());
			pstmtDel.setInt(2, gs.sId);
			pstmtDel.executeUpdate();	
			
			// Add the group ident if it is not zero length (ie set to none)
			if(gs.groupIdent != null) {
				if(gs.groupIdent.trim().length() == 0) {
					gs.groupIdent = null;
				}
			}
			if(gs.fName != null) {
				if(gs.fName.trim().length() == 0) {
					gs.fName = null;
				}
			}
			if(gs.groupIdent != null || gs.fName != null) {			
				pstmt.setString(1, request.getRemoteUser());
				pstmt.setInt(2, gs.sId);
				pstmt.setString(3, gs.groupIdent);
				pstmt.setString(4, gs.fName);
				log.info("Update group survey: " + pstmt.toString());
				pstmt.executeUpdate();
			}


			response = Response.ok().build();
				
		} catch (Exception e) {

			response = Response.serverError().build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtDel != null) {pstmtDel.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}


	/*
	 * No Key - Get the user from apache authentication
	 */
	@POST
	@Path("/details")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response postUserDetailsNoKey(
			@Context HttpServletRequest request) throws IOException {
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		return updateUserDetails(request, null);
	}
	
	/*
	 * No Key Get the user from the provided key
	 */
	@POST
	@Path("/details/key/{instanceId}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response postUpdateInstance(
			@Context HttpServletRequest request,
	        @PathParam("key") String key) throws IOException {
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		return updateUserDetails(request, key);
	}

	/*
	 * Update the user details
	 * This service should be merged with the preceeding one however:
	 *  current the user settings can be updated from many places
	 *  user details on the webform screen adds a user signature which requires
	 *  a multi part mime update however for the moment we will try and avoid
	 *  affecting the other locations that update settings
	 */
	private Response updateUserDetails(@Context HttpServletRequest request, String key) { 
		
		Response response = null;
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();	
		fileItemFactory.setSizeThreshold(5*1024*1024); // 5 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);

		// Authorisation - Not Required
		
		Connection sd = SDDataSource.getConnection("surveyKPI-UserSvc");
		
		
		FileItem sigItem = null;
		String fileName = null;
		String sigPath = null;
		String userFolderPath = null;
		String sigFolderPath = null;
		String settings = null;
		String user = null;
		
		PreparedStatement pstmt = null;
		try {	
			
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			/*
			 * Get the user
			 */
			if(key != null) {
				try {
					user = GeneralUtilityMethods.getDynamicUser(sd, key);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				log.info("Getting user with key: " + key);
			} else {
				user = request.getRemoteUser();
			}
			log.info("user: " + user);
			
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
							settings = item.getString();
						} catch (Exception e) {
							
						}
					}
					
					
				} else if(!item.isFormField()) {
					// Handle Uploaded files.
					log.info("Field Name = "+item.getFieldName()+
						", File Name = "+item.getName()+
						", Content type = "+item.getContentType()+
						", File Size = "+item.getSize());
					
					// Get the base path
					String basePath = GeneralUtilityMethods.getBasePath(request);
					
					if(item.getSize() > 0) {
						sigItem = item;
						fileName = String.valueOf(UUID.randomUUID());
						int userId = GeneralUtilityMethods.getUserId(sd, user);
						
						userFolderPath = basePath + "/media/users/" +  userId;
						sigFolderPath = userFolderPath + "/sig";
						sigPath = sigFolderPath + "/" + fileName;				
						// 1. Create the users folder if it does not exist
						File folder = new File(basePath + "/media/users/");
						FileUtils.forceMkdir(folder);
						
						// 2. Create the folder for the individual user if it does not exist
						folder = new File(userFolderPath);
					    FileUtils.forceMkdir(folder);
					    
					    // 3. Delete any existing signature files
					    folder = new File(sigFolderPath);
					    try { 	
							FileUtils.deleteDirectory(folder);
						} catch (IOException e) {
							log.log(Level.SEVERE, "Error deleting signature directory");
						}
					    
					    // 4. Create the signature folder
					    folder = new File(sigFolderPath);
					    FileUtils.forceMkdir(folder);
					    
					    // 5. Save the file
					    File savedFile = new File(sigPath);
					    item.write(savedFile);
					}					
				}

			}
			
			Type type = new TypeToken<User>(){}.getType();		
			User u = new Gson().fromJson(settings, type);		
			
			// Ensure email is null if it has not been set
			if(u.email != null && u.email.trim().isEmpty()) {
				u.email = null;
			}
		

			String sql = null;
			String ident = user;
			
			if(sigPath == null && !u.delSig) {
				// Do not update the signature
				sql = "update users set " +
						" name = ?, " + 
						" settings = ? " + 
						" where " +
						" ident = ?;";
			} else {
				// Update the signature
				sql = "update users set " +
						" name = ?, " + 
						" settings = ?, " + 
						" signature = ? " +
						" where " +
						" ident = ?;";
				
			}
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, HtmlSanitise.checkCleanName(u.name, localisation));
			pstmt.setString(2, u.settings);
			if(sigPath == null && !u.delSig) {
				pstmt.setString(3, ident);
			} else {
				pstmt.setString(3, HtmlSanitise.checkCleanName(fileName, localisation));
				pstmt.setString(4, ident);
			}
			
			log.info("Update user details: " + pstmt.toString());
			log.info("userevent: " + ident + (u.password == null ? " : updated user details : " : " : updated password : ") + u.name);
			pstmt.executeUpdate();
			
			// Set the updated signature and return it in the user id 
			if(fileName != null) {
				u.signature = "/surveyKPI/file/" + fileName + "/users?type=sig";
			} else if(u.delSig) {
				u.signature = null;
			}
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(u);
			
			response = Response.ok(resp).build();
						
		} catch (Exception e) {

			response = Response.serverError().build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-UserSvc", sd);
		}
		
		return response;
	}


}

