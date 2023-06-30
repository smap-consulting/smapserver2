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
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.model.ChangeElement;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.RoleColumnFilter;
import org.smap.sdal.model.RoleName;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import utilities.XLSRolesManager;

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
 * Services for managing roles
 */

@Path("/role")
public class Roles extends Application {
	
	Authorise aSM = null;
	Authorise aLowPriv = null;

	private static Logger log =
			 Logger.getLogger(Roles.class.getName());
	
	LogManager lm = new LogManager(); // Application log
	
	public Roles() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		
		// Only allow security administrators and organisational administrators to view or update the roles
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.SECURITY);
		authorisations.add(Authorise.ORG);
		aSM = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.MANAGE_TASKS);
		authorisations.add(Authorise.VIEW_DATA);
		aLowPriv = new Authorise(authorisations, null);
	}
	
	/*
	 * Get the roles in the organisation
	 */
	@Path("/roles")
	@GET
	@Produces("application/json")
	public Response getRoles(
			@Context HttpServletRequest request
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getRoles";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aSM.isAuthorised(sd, request.getRemoteUser());
		
		// End Authorisation
		
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			RoleManager rm = new RoleManager(localisation);
			
			int o_id  = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			ArrayList<Role> roles = rm.getRoles(sd, o_id);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(roles);
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
	 * Update the role details
	 */
	@Path("/roles")
	@POST
	@Consumes("application/json")
	public Response updateRoles(@Context HttpServletRequest request, @FormParam("roles") String roles) { 
		
		Response response = null;
		String connectionString = "surveyKPI-updateRoles";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aSM.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		Type type = new TypeToken<ArrayList<Role>>(){}.getType();		
		ArrayList<Role> rArray = new Gson().fromJson(roles, type);
		
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			RoleManager rm = new RoleManager(localisation);
			
			for(int i = 0; i < rArray.size(); i++) {
				Role r = rArray.get(i);
				String msg = null;
				if(r.id == -1) {
					
					// New role
					rm.createRole(sd, r, o_id, request.getRemoteUser(), false);
					msg = localisation.getString("r_created");
					
				} else {
					// Existing role
					rm.updateRole(sd, r, o_id, request.getRemoteUser());
					msg = localisation.getString("r_modified");	
					StringBuilder userList = new StringBuilder("");
					for(int id : r.users) {
						String ident = GeneralUtilityMethods.getUserIdent(sd, id);
						if(ident != null) {
							if(userList.length() > 0) {
								userList.append(", ");
							}
							userList.append(ident);
						}
					}
					msg = msg.replace("%s2", userList.toString());
					msg = msg.replace("%s3", r.desc);
				}
				msg = msg.replace("%s1",  r.name);
				lm.writeLogOrganisation(sd, o_id, request.getRemoteUser(), LogManager.ROLE, msg, 0);
				
				response = Response.ok().build();
			}
				
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Delete roles
	 */
	@Path("/roles")
	@DELETE
	@Consumes("application/json")
	public Response delRole(@Context HttpServletRequest request, @FormParam("roles") String roles) { 
		
		Response response = null;
		String requestName = "surveyKPI- delete roles";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requestName);
		aSM.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation			
					
		Type type = new TypeToken<ArrayList<Role>>(){}.getType();		
		ArrayList<Role> rArray = new Gson().fromJson(roles, type);
		
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			RoleManager rm = new RoleManager(localisation);
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			rm.deleteRoles(sd, rArray, o_id, request.getRemoteUser());
			
			response = Response.ok().build();			
		}  catch (Exception ex) {
			log.log(Level.SEVERE, ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
			
		} finally {			
			SDDataSource.closeConnection(requestName, sd);
		}
		
		return response;
	}
	
	
	/*
	 * Get the roles in a survey
	 * Any administator / analyst can call this but they will only get the roles they have
	 */
	@Path("/survey/{sId}")
	@GET
	@Produces("application/json")
	public Response getSurveyRoles(
			@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("enabled") boolean enabledOnly
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getSurveyRoles";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aLowPriv.isAuthorised(sd, request.getRemoteUser());
		aLowPriv.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		
		// End Authorisation
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
	
			RoleManager rm = new RoleManager(localisation);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			ArrayList<Role> roles = rm.getSurveyRoles(sd, sIdent, oId, enabledOnly, request.getRemoteUser(), superUser);
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(roles);
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
	 * Update a survey role
	 */
	@Path("/survey/{sId}/{property}")
	@POST
	@Consumes("application/json")
	@Produces("application/json")
	public Response updateSurveyRoles(
			@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("property") String property,
			@FormParam("role") String roleString
			) { 

		Response response = null;
		
		Role role = new Gson().fromJson(roleString, Role.class);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-updateSurveyRoles");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aSM.isAuthorised(sd, request.getRemoteUser());
		aSM.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		aSM.isValidRole(sd, request.getRemoteUser(), role.id);
		// End Authorisation
		
		String sqlChangeLog = "insert into survey_change " +
				"(s_id, version, changes, user_id, apply_results, updated_time) " +
				"values(?, ?, ?, ?, 'true', ?)";
		PreparedStatement pstmtChangeLog = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			RoleManager rm = new RoleManager(localisation);
			ChangeElement change = new ChangeElement();
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			
			if(property.equals("enabled")) {
				role.linkid = rm.updateSurveyLink(sd, sIdent, role.id, role.linkid, role.enabled);
				change.msg = localisation.getString(role.enabled ? "ed_c_re" : "ed_c_rne");
			} else if(property.equals("row_filter")) {
				rm.updateSurveyRoleRowFilter(sd, sIdent, role, localisation);
				change.msg = localisation.getString("ed_c_rrf");
				change.msg = change.msg.replace("%s2", GeneralUtilityMethods.getSafeText(role.row_filter, true));
			} else if(property.equals("column_filter")) {
				rm.updateSurveyRoleColumnFilter(sd, sIdent, role, localisation);
				change.msg = localisation.getString("ed_c_rcf");
				StringBuilder colMsg = new StringBuilder("");
				for(RoleColumnFilter c : role.column_filter) {
					String col = GeneralUtilityMethods.getQuestionNameFromId(sd, sId, c.id);
					if(colMsg.length() > 0) {
						colMsg.append(", ");
					}
					colMsg.append(col);
				}
				change.msg = change.msg.replace("%s2", GeneralUtilityMethods.getSafeText(colMsg.toString(), true));
			}

			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

			// Record change in change log
			int userId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
			int version = GeneralUtilityMethods.getSurveyVersion(sd, sId);

			change.action = "role";
			change.origSId = sId;	
			change.msg = change.msg.replace("%s1", GeneralUtilityMethods.getSafeText(role.name, true));
			
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
			pstmtChangeLog.setInt(1, sId);
			pstmtChangeLog.setInt(2, version);
			pstmtChangeLog.setString(3, gson.toJson(change));
			pstmtChangeLog.setInt(4, userId);
			pstmtChangeLog.setTimestamp(5, GeneralUtilityMethods.getTimeStamp());
			pstmtChangeLog.execute();			
			String resp = gson.toJson(role);
			response = Response.ok(resp).build();
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			if(pstmtChangeLog != null) try {pstmtChangeLog.close();}catch(Exception e) {}
			SDDataSource.closeConnection("surveyKPI-updateSurveyRoles", sd);
		}

		return response;
	}

	/*
	 * Get the roles names in the organisation
	 * This is a low privilege service to allow users who are not the security manager to get role names for purposes
	 *  such as assigning tasks to members of a role
	 */
	@Path("/roles/names")
	@GET
	@Produces("application/json")
	public Response getRolesNames(
			@Context HttpServletRequest request
			) { 

		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-getRoleNames");
		aLowPriv.isAuthorised(sd, request.getRemoteUser());
		
		// End Authorisation
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
						
			RoleManager rm = new RoleManager(localisation);
						
			int o_id  = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			ArrayList<RoleName> roles = rm.getRoleNames(sd, o_id);
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(roles);
			response = Response.ok(resp).build();
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			
			SDDataSource.closeConnection("surveyKPI-getRoleNames", sd);
		}

		return response;
	}
	
	/*
	 * Export roles
	 */
	@GET
	@Path ("/xls")
	@Produces("application/x-download")
	public Response exportRoles(@Context HttpServletRequest request, 
			@QueryParam("tz") String tz,
			@Context HttpServletResponse response
		) throws Exception {

		String connectionString = "Export Roles";
		Connection sd = SDDataSource.getConnection(connectionString);	
		// Authorisation - Access
		aSM.isAuthorised(sd, request.getRemoteUser());		
		// End Authorisation 
		
		try {
			
			// Localisation
			Organisation organisation = UtilityMethodsEmail.getOrganisationDefaults(sd, null, request.getRemoteUser());
			Locale locale = new Locale(organisation.locale);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String filename = null;
			filename = localisation.getString("rep_roles") + ".xlsx";			
			GeneralUtilityMethods.setFilenameInResponse(filename, response); // Set file name
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			RoleManager rm = new RoleManager(localisation);
			ArrayList<Role> roles = rm.getRoles(sd, oId);

			// Create Project XLS File
			XLSRolesManager xr = new XLSRolesManager(request.getScheme(), request.getServerName());
			xr.createXLSFile(response.getOutputStream(), roles, localisation, tz);

		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			throw new Exception("Exception: " + e.getMessage());
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);	
			
		}
		return Response.ok("").build();
	}
	
	/*
	 * Import roles from an xls file
	 */
	@POST
	@Produces("application/json")
	@Path("/xls")
	public Response importProjects(
			@Context HttpServletRequest request
			) throws IOException {
		
		Response response = null;
		boolean clear = false;
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();		

		log.info("userevent: " + request.getRemoteUser() + " : import roles ");

		fileItemFactory.setSizeThreshold(20*1024*1024); 	// 20 MB 
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		String fileName = null;
		String filetype = null;
		FileItem file = null;
		String connectionString = "Roles - Roles Upload";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aSM.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation	
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
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

				// Process xls file
				XLSRolesManager xrm = new XLSRolesManager();
				ArrayList<Role> roles = xrm.getXLSRolesList(filetype, file.getInputStream(), localisation, tz);	
						
				RoleManager rm = new RoleManager(localisation);
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());				
				int deletedCount = 0;
				
				// Delete previously imported roles
				if(clear) {
					deletedCount = rm.deleteImportedRoles(sd, oId);
				}

				// Save roles to the database
				ArrayList<String> added = new ArrayList<> ();
				for(Role r : roles) {
					int id = rm.createRole(sd, r, oId, request.getRemoteUser(), true);
					if(id > 0) {
						added.add(r.name);
					}
				}
					
				String note = localisation.getString("r_import")
					.replace("%s1", String.valueOf(deletedCount))
					.replace("%s2", String.valueOf(roles.size()))
					.replace("%s3", String.valueOf(added.size()))
					.replaceFirst("%s4", added.toString());
				lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), 
						LogManager.ROLE, note, 0);
				
				response = Response.ok(note).build();
			} else {
				response = Response.serverError().entity("File not found").build();
			}
			
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
			try {if(!sd.getAutoCommit()) { sd.rollback();}} catch(Exception e) {}
		} finally {
			
			try {if(!sd.getAutoCommit()) { sd.setAutoCommit(true);}} catch(Exception e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}
		
		return response;
		
	}

}

