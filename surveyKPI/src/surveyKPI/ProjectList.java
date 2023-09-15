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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Project;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import utilities.XLSProjectsManager;

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
 * Returns a list of all projects that are in the same organisation as the user making the request
 */
@Path("/projectList")
public class ProjectList extends Application {
	
	Authorise a = new Authorise(null, Authorise.ADMIN);
	Authorise aSM = new Authorise(null, Authorise.SECURITY);

	private static Logger log =
			 Logger.getLogger(ProjectList.class.getName());

	LogManager lm = new LogManager(); // Application log
	
	@GET
	@Produces("application/json")
	public Response getProjects(@Context HttpServletRequest request) { 

		Response response = null;
		String connectionString = "surveyKPI-ProjectList";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		ArrayList<Project> projects = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			ProjectManager pm = new ProjectManager(localisation);
			projects = pm.getProjects(sd, request.getRemoteUser(), 
					true,		// always get all projects in organisation
					false, 		// Don't get links
					null,		// Don't need url prefix
					false,		// Don't just want empty projects
					false		// Don't just want imported projects
					);
				
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(projects);
			response = Response.ok(resp).build();
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
		    response = Response.serverError().build();
		    
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Update the project details
	 */
	@POST
	@Consumes("application/json")
	public Response updateProject(@Context HttpServletRequest request, @FormParam("projects") String projects) throws ApplicationException { 
		
		Response response = null;
		
		// Authorisation - Access
		String user = request.getRemoteUser();
		Connection sd = SDDataSource.getConnection("surveyKPI-ProjectList");
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		Type type = new TypeToken<ArrayList<Project>>(){}.getType();		
		ArrayList<Project> pArray = new Gson().fromJson(projects, type);
		
		PreparedStatement pstmt = null;
		try {	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String sql = null;
			int o_id;
			int u_id;
			ResultSet resultSet = null;
			
			/*
			 * Get the organisation and the user id
			 */
			sql = "SELECT u.o_id, u.id " +
					" FROM users u " +  
					" WHERE u.ident = ?;";				
						
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, user);
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				o_id = resultSet.getInt(1);
				u_id = resultSet.getInt(2);
				
				ProjectManager pm = new ProjectManager(localisation);
				
				for(int i = 0; i < pArray.size(); i++) {
					Project p = pArray.get(i);
					
					if(p.id == -1) {
						
						// New project
						p.id = pm.createProject(sd, user, p, o_id, u_id, request.getRemoteUser());
						
					} else {
						// Existing project
						
						// Check the project is in the same organisation as the administrator doing the editing
						sql = "SELECT p.id " +
								" FROM project p " +  
								" WHERE p.id = ? " +
								" AND p.o_id = ?;";				
						
						try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
						pstmt = sd.prepareStatement(sql);
						pstmt.setInt(1, p.id);
						pstmt.setInt(2, o_id);
						log.info("SQL: " + pstmt.toString());
						resultSet = pstmt.executeQuery();
						
						if(resultSet.next()) {
							sql = "update project set " +
									" name = ?, " + 
									" description = ?, " + 
									" changed_by = ?, " + 
									" changed_ts = now() " + 
									" where " +
									" id = ?;";
						
							try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
							pstmt = sd.prepareStatement(sql);
							pstmt.setString(1, HtmlSanitise.checkCleanName(p.name, localisation));
							pstmt.setString(2, HtmlSanitise.checkCleanName(p.desc, localisation));
							pstmt.setString(3, request.getRemoteUser());
							pstmt.setInt(4, p.id);
							
							log.info("update project: " + pstmt.toString());
							pstmt.executeUpdate();
							
							// Remove users from project
							sql = "delete from user_project " +
									" where p_id = ? "
									+ "and u_id not in (select id from users where temporary)";
							try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
							pstmt = sd.prepareStatement(sql);
							pstmt.setInt(1, p.id);
							log.info("Delete existing non temp users from project " + pstmt.toString());
							pstmt.executeUpdate();

						}
						
						// Record the project change so that devices can be notified
						MessagingManager mm = new MessagingManager(localisation);
						mm.projectChange(sd, p.id, o_id);
					}
					
					// add users from project
					if(p.users.size() > 0) {
						for(int uId : p.users) {
							pm.addUser(sd, p.id, o_id, uId);
						}
					}
				}
			
				response = Response.ok().build();
			} else {
				log.log(Level.SEVERE,"Error: No organisation");
			    response = Response.serverError().entity("No Organisation").build();
			}
				
		} catch (SQLException e) {
			String state = e.getSQLState();
			log.info("sql state:" + state);
			if(state.startsWith("23")) {
				response = Response.status(Status.CONFLICT).entity("Duplicate Project Name").build();
			} else {
				response = Response.serverError().entity(e.getMessage()).build();
				log.log(Level.SEVERE,"Error", e);
			}
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-ProjectList", sd);
		}
		
		return response;
	}
	
	/*
	 * Delete project
	 */
	@DELETE
	@Consumes("application/json")
	public Response delProject(@Context HttpServletRequest request, @FormParam("projects") String projects) { 
		
		Response response = null;
		String connectionString = "surveyKPI-ProjectList";
		Connection cResults = null;
		
		Type type = new TypeToken<ArrayList<Project>>(){}.getType();		
		ArrayList<Project> pArray = new Gson().fromJson(projects, type);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(pArray != null && pArray.size() > 0) {
			for(Project p : pArray) {
				a.isValidProject(sd, request.getRemoteUser(), p.id);
			}
		}
		// End Authorisation			
		
		try {	
			cResults = ResultsDataSource.getConnection(connectionString);
			
			// Localisation
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			sd.setAutoCommit(false);			
			ProjectManager pm = new ProjectManager(localisation);			
			pm.deleteProjects(sd, cResults,
					a,
					pArray, 
					request.getRemoteUser(),
					GeneralUtilityMethods.getBasePath(request));		
			sd.commit();
			
			response = Response.ok().build();
				
		} catch (SQLException e) {
			String state = e.getSQLState();
			log.info("sql state:" + state);
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}
			
		} catch (Exception ex) {
			log.info(ex.getMessage());
			response = Response.serverError().entity(ex.getMessage()).build();
			
			try{sd.rollback();} catch(Exception e2) {}
			
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}
		
		return response;
	}
	
	/*
	 * Export projects
	 */
	@GET
	@Path ("/xls")
	@Produces("application/x-download")
	public Response exportProjects(@Context HttpServletRequest request, 
			@QueryParam("tz") String tz,
			@Context HttpServletResponse response
		) throws Exception {

		String connectionString = "Export Projects";
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
			filename = localisation.getString("ar_projects") + ".xlsx";			
			GeneralUtilityMethods.setFilenameInResponse(filename, response); // Set file name
			
			ProjectManager pm = new ProjectManager(localisation);
			ArrayList<Project> projects = pm.getProjects(sd, request.getRemoteUser(), 
					true,		// always get all projects in organisation
					false, 		// Don't get links
					null,		// Don't need url prefix
					false,		// Don't just want empty projects
					false		// Don't just want imported projects
					);
			
			// Create Project XLS File
			XLSProjectsManager xp = new XLSProjectsManager(request.getScheme(), request.getServerName());
			xp.createXLSFile(response.getOutputStream(), projects, localisation, tz);
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			throw new Exception("Exception: " + e.getMessage());
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);	
			
		}
		return Response.ok("").build();
	}
	
	/*
	 * Import projects from an xls file
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

		log.info("userevent: " + request.getRemoteUser() + " : import projects ");

		fileItemFactory.setSizeThreshold(20*1024*1024); 	// 20 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		Connection cResults = null;
		String fileName = null;
		String filetype = null;
		FileItem file = null;
		String requester = "Projects - Projects Upload";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requester);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation	
		

		
		try {
			cResults = ResultsDataSource.getConnection(requester);
			
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
				// Authorisation - Access
				a.isAuthorised(sd, request.getRemoteUser());
				
				// End authorisation

				// Process xls file
				XLSProjectsManager xpm = new XLSProjectsManager();
				ArrayList<Project> projects = xpm.getXLSProjectList(filetype, file.getInputStream(), localisation, tz);	
						
				// Save projects to the database
				ProjectManager pm = new ProjectManager(localisation);
				
				ArrayList<Project> emptyProjects = null;
				
				if(clear) {

					emptyProjects = pm.getProjects(sd, 
							request.getRemoteUser(), true, false, null, true, true);
					
					if(emptyProjects.size() > 0) {
						sd.setAutoCommit(false);
						pm.deleteProjects(sd, cResults,
								a,
								emptyProjects, 
								request.getRemoteUser(),
								GeneralUtilityMethods.getBasePath(request));		
						sd.commit();
						sd.setAutoCommit(true);
					}
				}
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());				
				ArrayList<String> added = pm.writeProjects(sd, projects, oId, request.getRemoteUser(), true);
					
				String note = localisation.getString("p_import");
				if(emptyProjects == null) {
					note = note.replaceFirst("%s1", "0");
				} else {
					note = note.replaceFirst("%s1", String.valueOf(emptyProjects.size()));
				}
				
				note = note.replaceFirst("%s2", String.valueOf(projects.size()));
				note = note.replaceFirst("%s3", String.valueOf(added.size()));
				note = note.replaceFirst("%s4", added.toString());
				lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), 
						LogManager.PROJECT, note, 0);
				
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
			
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
			
		}
		
		return response;
		
	}
	
}

