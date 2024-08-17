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
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.DocumentUploadManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;
import org.smap.sdal.model.UserSimple;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import utilities.XLSUsersManager;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Returns a list of all users that are in the same organisation as the user making the request
 */
@Path("/userList")
public class UserList extends Application {
	
	Authorise a = null;
	Authorise aUpdate = null;
	Authorise aSM = null;
	Authorise aSimpleList = null;

	private static Logger log =
			 Logger.getLogger(UserList.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	public UserList() {
		
		// Allow administrators and analysts to view the list of users
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.SECURITY);
		authorisations.add(Authorise.ORG);
		a = new Authorise(authorisations, null);
		
		// Also allow users with View rights to view the simple list of users
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.MANAGE_TASKS);
		aSimpleList = new Authorise(authorisations, null);
		
		// Only allow administrators, org administrators and security managers to update user list
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.SECURITY);
		authorisations.add(Authorise.ORG);
		aUpdate = new Authorise(authorisations, null);
		
		// Only allow security administrators and organisational administrators to view or update the roles
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.SECURITY);
		authorisations.add(Authorise.ORG);
		aSM = new Authorise(authorisations, null);
		
	}
	
	@GET
	@Produces("application/json")
	public Response getUsers(
			@Context HttpServletRequest request
			) { 

		Response response = null;
		String requestName = "surveyKPI-getUsers";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		aSimpleList.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);	
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			boolean isOrgUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ORG_ID);
			boolean isSecurityManager = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.SECURITY_ID);
			boolean isAdminUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ADMIN_ID);
			
			UserManager um = new UserManager(localisation);
			ArrayList<User> users = um.getUserList(sd, oId, isOrgUser, isSecurityManager, isAdminUser, request.getRemoteUser());
			String resp = gson.toJson(users);
			response = Response.ok(resp).build();
						
			
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
			
			SDDataSource.closeConnection(requestName, sd);
		}

		return response;
	}
	
	@GET
	@Produces("application/json")
	@Path("/simple")
	public Response getUsersSimple(
			@Context HttpServletRequest request
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getUsersSimple";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aSimpleList.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		ArrayList<UserSimple> users = null;
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);	
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			boolean isOnlyViewData = GeneralUtilityMethods.isOnlyViewData(sd, request.getRemoteUser());
			UserManager um = new UserManager(localisation);
			users = um.getUserListSimple(sd, oId, true, isOnlyViewData, request.getRemoteUser(), false);		// Always sort by name
			String resp = gson.toJson(users);
			response = Response.ok(resp).build();
						
			
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	@GET
	@Path("/temporary")
	@Produces("application/json")
	public Response getTemporaryUsers(
			@Context HttpServletRequest request,
			@QueryParam("action") String action,
			@QueryParam("pId") int pId
			) { 

		Response response = null;
		String requestName = "surveyKPI - getTemporaryUsers";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation			
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";	// Set default for timezone
			
			ActionManager am = new ActionManager(localisation, tz);
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			ArrayList<User> users = am.getTemporaryUsers(sd, o_id, action, null, pId);			
			String resp = gson.toJson(users);
			response = Response.ok(resp).build();			
			
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
		
			SDDataSource.closeConnection(requestName, sd);
		}

		return response;
	}
	
	/*
	 * Get the users who have access to a specific project
	 */
	@Path("/{projectId}")
	@GET
	@Produces("application/json")
	public Response getUsersForProject(
			@Context HttpServletRequest request,
			@PathParam("projectId") int projectId
			) { 

		Response response = null;
		String requestName = "surveyKPI - getUsersForProject";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidProject(sd, request.getRemoteUser(), projectId);
		// End Authorisation
		
		/*
		 * 
		 */	
		PreparedStatement pstmt = null;
		ArrayList<User> users = new ArrayList<User> ();
		
		try {
			String sql = null;
			ResultSet resultSet = null;
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			/*
			 * Get the users for this project
			 */
			sql = "SELECT u.id as id, " +
					"u.ident as ident, " +
					"u.name as name, " +
					"u.email as email " +
					"from users u, user_project up " +			
					"where u.id = up.u_id " +
					"and up.p_id = ? " +
					"and u.o_id = ? " +
					"and not u.temporary " +
					"order by u.name";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, projectId);
			pstmt.setInt(2, o_id);
			log.info("Get users for project: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
							
			User user = null;
			while(resultSet.next()) {
				user = new User();
				user.id = resultSet.getInt("id");
				user.ident = resultSet.getString("ident");
				user.name = resultSet.getString("name");
				user.email = resultSet.getString("email");
				users.add(user);
			}
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(users);
			response = Response.ok(resp).build();
					
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(requestName, sd);
		}

		return response;
	}
	
	/*
	 * Get the users who have access to a specific survey
	 */
	@Path("/survey/{survey}")
	@GET
	@Produces("application/json")
	public Response getUsersForSurvey(
			@Context HttpServletRequest request,
			@PathParam("survey") int sId
			) { 

		Response response = null;
		String requestName = "surveyKPI - getUsersForSurvey";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}		
		aSimpleList.isAuthorised(sd, request.getRemoteUser());
		aSimpleList.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		PreparedStatement pstmt = null;
		ArrayList<UserSimple> users = new ArrayList<> ();
		
		try {
			ResultSet resultSet = null;
			
			StringBuffer sql = new StringBuffer("select u.id, u.ident, u.name from survey s, users u, user_project up, project p "
					+ "where u.id = up.u_id "
					+ "and p.id = up.p_id "
					+ "and s.p_id = up.p_id "
					+ "and s.s_id = ? "
					+ "and not temporary");
			
			String sqlRBAC = " and ((s.ident not in (select survey_ident from survey_role where enabled = true)) " // No roles on survey
					+ "or (s.ident in (select sr.survey_ident from users u2, user_role ur, survey_role sr where u2.ident = u.ident and sr.enabled = true and u.id = ur.u_id and ur.r_id = sr.r_id)) " // User also has role
					+ "or (select count(*) from users u3, user_group ug "		// Include super users
					+ "where u.ident = u3.ident "
					+ "and u3.id = ug.u_id "
					+ "and (ug.g_id = 6 or ug.g_id = 4)) > 0 "
					+ ") ";
			
			sql.append(sqlRBAC);
			
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setInt(1, sId);
			log.info("Get users of survey: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
							
			UserSimple user = null;
			while(resultSet.next()) {
				user = new UserSimple();
				user.id = resultSet.getInt("id");
				user.ident = resultSet.getString("ident");
				user.name = resultSet.getString("name");
				users.add(user);
			}
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(users);
			response = Response.ok(resp).build();
					
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(requestName, sd);
		}

		return response;
	}
	
	/*
	 * Update the settings or create new user
	 * Called when saving changes to a user from the users page
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateUser(@Context HttpServletRequest request, String users) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String requestName = "surveyKPI - updateUser";
		
		Type type = new TypeToken<ArrayList<User>>(){}.getType();		
		ArrayList<User> uArray = new Gson().fromJson(users, type);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		
		// That the user is in the administrators organisation is validated on update
		
		// End Authorisation
	
		PreparedStatement pstmt = null;
		try {	
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String sql = null;
			int o_id;
			String adminName = null;
			String adminEmail = null;
			ResultSet resultSet = null;
			boolean isOrgUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ORG_ID);
			boolean isSecurityManager = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.SECURITY_ID);
			boolean isEnterpriseManager = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ENTERPRISE_ID);
			boolean isServerOwner = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.OWNER_ID);
			
			/*
			 * Get the organisation and name of the user making the request
			 */
			sql = "SELECT u.o_id, u.name, u.email " +
					" FROM users u " +  
					" WHERE u.ident = ?";				
						
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, request.getRemoteUser());
			log.info("SQL: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				o_id = resultSet.getInt(1);
				adminName = resultSet.getString(2);	
				adminEmail = resultSet.getString(3);
				
				for(int i = 0; i < uArray.size(); i++) {
					User u = uArray.get(i);
					
					// Ensure email is null if it has not been set
					if(u.email != null && u.email.trim().isEmpty()) {
						u.email = null;
					}
					
					UserManager um = new UserManager(localisation);
					String msg = null;
					if(u.id == -1) {
						// New user
						um.createUser(sd, u, o_id,
								isOrgUser,
								isSecurityManager,
								isEnterpriseManager,
								isServerOwner,
								request.getRemoteUser(),
								request.getScheme(),
								request.getServerName(),
								adminName,
								adminEmail,
								localisation);
						
						msg = localisation.getString("lm_new_user");
								
					} else {
						// Existing user
						um.updateUser(sd, u, o_id,
								isOrgUser,
								isSecurityManager,
								isEnterpriseManager,
								isServerOwner,
								request.getRemoteUser(),
								request.getServerName(),
								adminName,
								false);
						
						msg = localisation.getString("lm_user");
					}
							
					msg = msg.replace("%s1", u.ident);
					msg = msg.replace("%s2", getGroups(u.groups));
					msg = msg.replace("%s3", getRoles(sd, o_id, u.roles));
					lm.writeLogOrganisation(sd, 
							o_id, request.getRemoteUser(), LogManager.USER, msg, 0);
					
					// Record the user change so that devices can be notified
					MessagingManager mm = new MessagingManager(localisation);
					mm.userChange(sd, u.ident);
				}

				
				response = Response.ok().build();
			} else {
				log.log(Level.SEVERE,"Error: No organisation");
				response = Response.serverError().entity("Error: No organisation").build();
			}
				
		} catch (SQLException e) {
			
			String state = e.getSQLState();
			log.info("sql state:" + state);
			if(state.startsWith("23")) {
				response = Response.status(Status.CONFLICT).entity(e.getMessage()).build();
				log.log(Level.SEVERE,"Error", e);
			} else {
				response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
				log.log(Level.SEVERE,"Error", e);
			}
		} catch (Exception e) {
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(requestName, sd);
		}
		
		return response;
	}
	
	/*
	 * Delete users
	 */
	@DELETE
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response delUser(@Context HttpServletRequest request, String users) {
		
		Response response = null;
		String requestName = "surveyKPI - delUser";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		Type type = new TypeToken<ArrayList<User>>(){}.getType();		
		ArrayList<User> uArray = new Gson().fromJson(users, type);
		
		PreparedStatement pstmt = null;
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		try {	
			int o_id;
			ResultSet resultSet = null;
			
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// Get the organisation of the person calling this service
			String sql = "SELECT u.o_id " +
					" FROM users u " +  
					" WHERE u.ident = ?;";										
			pstmt = sd.prepareStatement(sql);	
			
			// Get the organisation id
			pstmt.setString(1, request.getRemoteUser());
			log.info("Get user organisation and id: " + pstmt.toString());			
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				o_id = resultSet.getInt(1);
							
				UserManager um = new UserManager(localisation);
				
				for(int i = 0; i < uArray.size(); i++) {
					User u = uArray.get(i);					
					um.deleteUser(sd, request.getRemoteUser(), 
							basePath, u.id, o_id, u.all);
				}
		
				response = Response.ok().build();
			} else {
				log.log(Level.SEVERE,"Error: No organisation");
			    response = Response.serverError().build();
			}
				
		} catch (SQLException e) {
			String state = e.getSQLState();
			log.info("sql state:" + state);
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(requestName, sd);
		}
		
		return response;
	}
	
	/*
	 * Export users
	 */
	@GET
	@Path ("/xls")
	@Produces("application/x-download")
	public Response exportUsers(@Context HttpServletRequest request, 
			@QueryParam("tz") String tz,
			@Context HttpServletResponse response
		) throws Exception {

		String connectionString = "Export users";
		Connection sd = SDDataSource.getConnection(connectionString);	
		
		// Authorisation - Access
		a.isAuthorised(sd, request.getRemoteUser());		
		// End Authorisation 
		
		try {
			
			// Localisation
			Organisation organisation = UtilityMethodsEmail.getOrganisationDefaults(sd, null, request.getRemoteUser());
			Locale locale = new Locale(organisation.locale);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String filename = null;
			filename = localisation.getString("mf_us") + ".xlsx";			
			GeneralUtilityMethods.setFilenameInResponse(filename, response); // Set file name
			
			UserManager um = new UserManager(localisation);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			boolean isOrgUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ORG_ID);
			boolean isSecurityManager = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.SECURITY_ID);
			boolean isAdminUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ADMIN_ID);

			ArrayList<User> users = um.getUserList(sd, oId, isOrgUser, isSecurityManager,isAdminUser, request.getRemoteUser());
			
			// Create User XLS File
			XLSUsersManager xu = new XLSUsersManager(request.getScheme(), request.getServerName());
			xu.createXLSFile(sd, response.getOutputStream(), users, localisation, tz);
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			throw new Exception("Exception: " + e.getMessage());
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);	
			
		}
		return Response.ok("").build();
	}

	/*
	 * Import users from an xls file
	 */
	@POST
	@Produces("application/json")
	@Path("/xls")
	public Response importProjects(
			@Context HttpServletRequest request
			) throws IOException {
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		boolean clear = false;
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();		

		log.info("userevent: " + request.getRemoteUser() + " : import users ");

		fileItemFactory.setSizeThreshold(20*1024*1024); 	// 20 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		Connection cResults = null;
		String fileName = null;
		String filetype = null;
		FileItem file = null;
		String requester = "UsersList - Users Upload";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requester);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation	
		
		PreparedStatement pstmt = null;
		
		try {
			cResults = ResultsDataSource.getConnection(requester);
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			boolean isOrgUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ORG_ID);
			boolean isSecurityManager = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.SECURITY_ID);
			String tz = "UTC";	// Set default for timezone
			
			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();

			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();
				
				// Get form parameters
				
				if(item.isFormField()) {
					log.info("Form field:" + item.getFieldName() + " - " + item.getString());
					if(item.getFieldName().equals("file_clear")) {
						clear = Boolean.valueOf(item.getString());
					}
					
				} else if(!item.isFormField()) {
					// Handle Uploaded files.
					log.info("Field Name = "+item.getFieldName()+
						", File Name = "+item.getName()+
						", Content type = "+item.getContentType()+
						", File Size = "+item.getSize());
					
					fileName = item.getName();
					
					/*
					 * Validate the upload
					 */
					DocumentUploadManager dum = new DocumentUploadManager(localisation);
					dum.validateDocument(fileName, item, DocumentUploadManager.SETTINGS_IMPORT_TYPES);
					
					if(fileName.endsWith("xlsx") || fileName.endsWith("xlsm")) {
						filetype = "xlsx";
					} else if(fileName.endsWith("xls")) {
						filetype = "xls";
					} else {
						log.info("unknown file type for item: " + fileName);
						continue;	
					}
					
					file = item;
				}
			}
	
			if(file != null) {
				// Authorisation - Access
				a.isAuthorised(sd, request.getRemoteUser());			
				// End authorisation
				
				/*
				 * Get the organisation and name of the user making the request
				 */
				String sql = "select u.o_id, u.name, u.email "
						+ "from users u " 
						+ "where u.ident = ?";				
							
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, request.getRemoteUser());
				log.info("SQL: " + pstmt.toString());
				ResultSet resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					int oId = resultSet.getInt(1);
					String adminName = resultSet.getString(2);	
					String adminEmail = resultSet.getString(3);	
				
					String scheme = request.getScheme();
					String serverName = request.getServerName();
					ArrayList<String> added = new ArrayList<> ();
					
					// Process xls file
					XLSUsersManager xum = new XLSUsersManager();
					ArrayList<User> users = xum.getXLSUsersList(sd, filetype, file.getInputStream(), localisation, tz, oId);	
							
					// Save users in the database
					UserManager um = new UserManager(localisation);
					
					ArrayList<UserSimple> emptyUsers = null;
					String basePath = GeneralUtilityMethods.getBasePath(request);
					
					if(clear) {
						
						emptyUsers = um.getUserListSimple(sd, oId, true, false, null, true);
						
						if(emptyUsers.size() > 0) {
							for(UserSimple us : emptyUsers) {
								um.deleteUser(sd, request.getRemoteUser(), basePath, us.id, oId, true);
							}
						}
					}
					
					boolean userError = false;
					for(User u : users) {
						try {
							HtmlSanitise.checkCleanName(u.name, localisation);
							um.createUser(sd, u, oId, 
									isOrgUser, isSecurityManager, false, false, 
									request.getRemoteUser(), scheme, serverName, adminName, adminEmail, localisation);
							added.add(u.name);
						} catch (Exception e) {
							String msg = localisation.getString("ar_user_not_created") + " " + u.name + " " + e.getMessage();
							log.info(msg);
							lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), LogManager.USER, msg, 0);
							userError = true;
						}
					}
					if(userError) {
						throw new ApplicationException(localisation.getString("u_import_err"));
					}
					
					
					String note = localisation.getString("u_import");
					if(emptyUsers == null) {
						note = note.replaceFirst("%s1", "0");
					} else {
						note = note.replaceFirst("%s1", String.valueOf(emptyUsers.size()));
					}
					
					note = note.replaceFirst("%s2", String.valueOf(users.size()));
					note = note.replaceFirst("%s3", String.valueOf(added.size()));
					note = note.replaceFirst("%s4", added.toString());
					lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), 
							LogManager.PROJECT, note, 0);
					
				
					response = Response.ok(note).build();
				} else {
					response = Response.serverError().entity("Admin user not found").build();
				}
			} else {
				response = Response.serverError().entity("File not found").build();
			}
			
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
			try {if(!sd.getAutoCommit()) { sd.rollback();}} catch(Exception e) {}
		} finally {
			
			try {if(!sd.getAutoCommit()) { sd.setAutoCommit(true);}} catch(Exception e) {}
			try {if(pstmt != null) {pstmt.close();}} catch(Exception e) {};
			
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
			
		}
		
		return response;
		
	}
	
	private String getGroups(ArrayList<UserGroup> groups) {
		StringBuffer g = new StringBuffer("");
		if(groups != null) {
			for(UserGroup ug : groups) {
				String name = null;
				switch(ug.id) {
					case 1: name = "admin";
							break;
					case 2: name = "analyst";
							break;
					case 3: name = "enum";
							break;
					case 4: name = "org admin";
							break;
					case 5: name = "manage";
							break;
					case 6: name = "security";
							break;
					case 7: name = "view data";
							break;
				}
				if(name != null) {
					if(g.length() > 0) {
						g.append(", ");
					}
					g.append(name);
				}
				
			}
		}
		return g.toString();
	}
	
	private String getRoles(Connection sd, int oId, ArrayList<Role> roles) throws SQLException {
		StringBuffer rList = new StringBuffer("");
		PreparedStatement pstmt = null;
		String sql = "select name from role where id = ? and o_id = ?";
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(2, oId);
			ResultSet rs = null;
			if(roles != null) {
				for(Role r : roles) {
					pstmt.setInt(1,  r.id);
					rs = pstmt.executeQuery();
					if(rs.next()) {
						String name = rs.getString(1);
						if(name != null) {
							if(rList.length() > 0) {
								rList.append(", ");
							}
							rList.append(name);
						}	
					}
					rs.close();		
				}
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		return rList.toString();
	}
	

}

