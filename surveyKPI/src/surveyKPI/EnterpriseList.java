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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CsvTableManager;
import org.smap.sdal.managers.OrganisationManager;
import org.smap.sdal.model.DeviceSettings;
import org.smap.sdal.model.Enterprise;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.SensitiveData;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Returns a list of all projects that are in the same organisation as the user making the request
 */
@Path("/enterpriseList")
public class EnterpriseList extends Application {
	
	Authorise a = null;

	private static Logger log =
			 Logger.getLogger(EnterpriseList.class.getName());
	
	public EnterpriseList() {
		
		a = new Authorise(null, Authorise.ENTERPRISE);
		
	}
	
	@GET
	@Produces("application/json")
	public Response getEnterprises(@Context HttpServletRequest request,
			@QueryParam("tz") String tz) { 

		Response response = null;
		String connectionString = "surveyKPI-EnterpriseList-getEnterprises";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		tz = (tz == null) ? "UTC" : tz;
		
		PreparedStatement pstmt = null;
		ArrayList<Enterprise> enterprises = new ArrayList<Enterprise> ();
		
		try {
			String sql = null;
			ResultSet resultSet = null;
			
			/*
			 * Get the organisation
			 */
			sql = "select id, "
					+ "name, "
					+ "changed_by, "
					+ "timezone(?, changed_ts) as changed_ts "
					+ "from enterprise "
					+ "order by name asc;";			
						
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tz);
			log.info("Get enterprises: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			
			while(resultSet.next()) {
				Enterprise e = new Enterprise();
				e.id = resultSet.getInt("id");
				e.name = resultSet.getString("name");
				e.changed_by = resultSet.getString("changed_by");
				e.changed_ts = resultSet.getTimestamp("changed_ts");
				
				enterprises.add(e);
			}
	
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(enterprises);
			response = Response.ok(resp).build();
			
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
		    response = Response.serverError().build();
		    
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Update the organisation details or create a new organisation
	 */
	@POST
	public Response updateOrganisation(@Context HttpServletRequest request) throws Exception { 
		
		Response response = null;
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();	
		fileItemFactory.setSizeThreshold(1*1024*1024); //1 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-OrganisationList-updateOrganisation");
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation

		FileItem logoItem = null;
		String fileName = null;
		String organisations = null;
		try {
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
							organisations = item.getString();
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
						logoItem = item;
						fileName = item.getName();
						fileName = fileName.replaceAll(" ", "_"); // Remove spaces from file name
					}
					
				}

			}
			
			Type type = new TypeToken<ArrayList<Organisation>>(){}.getType();		
			ArrayList<Organisation> oArray = new Gson().fromJson(organisations, type);
				
			String requestUrl = request.getRequestURL().toString();
			String userIdent = request.getRemoteUser();
			String basePath = GeneralUtilityMethods.getBasePath(request);
				
			OrganisationManager om = new OrganisationManager();
			for(int i = 0; i < oArray.size(); i++) {
				Organisation o = oArray.get(i);
				if(o.timeZone != null && !o.timeZone.equals("UTC")) {
					if(!GeneralUtilityMethods.isValidTimezone(sd, o.timeZone)) {
						throw new ApplicationException("Invalid Timezone: " + o.timeZone);
					}
				}
				if(o.id == -1) {
					// New organisation
						
					om.createOrganisation(
							sd, 
							o, 
							userIdent, 
							fileName,
							requestUrl,
							basePath,
							logoItem,
							null);
					
						 
				} else {
					// Existing organisation

					om.updateOrganisation(
							sd, 
							o, 
							userIdent, 
							fileName,
							requestUrl,
							basePath,
							logoItem);	
				}
			
				response = Response.ok().build();
			}
				
		} catch (SQLException e) {
			String state = e.getSQLState();
			log.info("Update Organisation: sql state:" + state);
			if(state.startsWith("23")) {
				response = Response.status(Status.CONFLICT).entity(e.getMessage()).build();
			} else {
				response = Response.serverError().entity(e.getMessage()).build();
				log.log(Level.SEVERE,"Error", e);
			}
		} catch (FileUploadException ex) {
			response = Response.serverError().entity(ex.getMessage()).build();
			log.log(Level.SEVERE,"Error", ex);
			
		} finally {
			
			SDDataSource.closeConnection("surveyKPI-OrganisationList-updateOrganisation", sd);
		}
		
		return response;
	}
	


	
	/*
	 * Delete an organisation
	 */
	@DELETE
	@Consumes("application/json")
	public Response delOrganisation(@Context HttpServletRequest request, @FormParam("organisations") String organisations) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-OrganisationList-delOrganisation");
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		Type type = new TypeToken<ArrayList<Organisation>>(){}.getType();		
		ArrayList<Organisation> oArray = new Gson().fromJson(organisations, type);
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtDrop = null;
		try {	
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String sql = null;
			ResultSet resultSet = null;
			sd.setAutoCommit(false);
				
			for(int i = 0; i < oArray.size(); i++) {
				Organisation o = oArray.get(i);
				
				/*
				 * Ensure that there are no undeleted projects with surveys in this organisation
				 */
				sql = "SELECT count(*) " +
						" from project p, survey s " +  
						" where p.id = s.p_id " +
						" and p.o_id = ? " +
						" and s.deleted = 'false';";
					
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, o.id);
				log.info("SQL check for projects in an organisation: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					int count = resultSet.getInt(1);
					if(count > 0) {
						log.info("Count of undeleted projects:" + count);
						throw new Exception("Error: Organisation " + o.name + " has undeleted projects.");
					}
				} else {
					throw new Exception("Error getting project count");
				}
					
				sql = "DELETE FROM organisation o " +  
						" WHERE o.id = ?; ";			
				
				if(pstmt != null) try{pstmt.close();}catch(Exception e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, o.id);
				log.info("SQL: " + sql + ":" + o.id);
				pstmt.executeUpdate();
				
			    // Delete the organisation shared resources - not necessary
			    CsvTableManager tm = new CsvTableManager(sd, localisation);
			    tm.delete(o.id, 0, null);		
			    
				// Delete the organisation folder
				String basePath = GeneralUtilityMethods.getBasePath(request);
				String fileFolder = basePath + "/media/organisation/" + o.id;
			    File folder = new File(fileFolder);
			    try {
			    	log.info("Deleting organisation folder: " + fileFolder);
					FileUtils.deleteDirectory(folder);
				} catch (IOException e) {
					log.info("Error deleting organisation folder:" + fileFolder + " : " + e.getMessage());
				}	    
			}
			
			response = Response.ok().build();
			sd.commit();
				
		} catch (SQLException e) {
			String state = e.getSQLState();
			log.info("Delete organisation: sql state:" + state);
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}
			
		} catch (Exception ex) {
			log.info(ex.getMessage());
			response = Response.serverError().entity(ex.getMessage()).build();
			
			try{
				sd.rollback();
			} catch(Exception e2) {
				
			}
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtDrop != null) {pstmtDrop.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-OrganisationList-delOrganisation", sd);
		}
		
		return response;
	}
	
	/*
	 * Change the organisation a user belongs to
	 */
	@POST
	@Path("/setOrganisation")
	@Consumes("application/json")
	public Response changeOrganisation(@Context HttpServletRequest request,
			@FormParam("orgId") int orgId,
			@FormParam("users") String users,
			@FormParam("projects") String projects) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-OrganisationList-setOrganisation");
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		Type type = new TypeToken<ArrayList<User>>(){}.getType();		
		ArrayList<User> uArray = new Gson().fromJson(users, type);
		
		type = new TypeToken<ArrayList<Project>>(){}.getType();		
		ArrayList<Project> pArray = new Gson().fromJson(projects, type);
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		PreparedStatement pstmt3 = null;
		PreparedStatement pstmt4 = null;
		try {	
			sd.setAutoCommit(false);
			
			String sql = "update users set o_id =  ? " +  
					" WHERE id = ?; ";			
			String sql2 = "delete from user_project where u_id = ? and " +
					"p_id not in (select id from project where o_id = ?);";	
			String sql3 = "update project set o_id =  ? " +  
					" WHERE id = ?; ";			
			String sql4 = "delete from user_project where p_id = ? and " +
					"u_id not in (select id from users where o_id = ?); ";	
			
	
			pstmt = sd.prepareStatement(sql);
			pstmt2 = sd.prepareStatement(sql2);	
			pstmt3 = sd.prepareStatement(sql3);	
			pstmt4 = sd.prepareStatement(sql4);	

			// Move Users
			for(int i = 0; i < uArray.size(); i++) {
				pstmt.setInt(1, orgId);
				pstmt.setInt(2, uArray.get(i).id);

				log.info("Move User: " + pstmt.toString());
				pstmt.executeUpdate();
				
				log.info("userevent: " + request.getRemoteUser() + " : move user : " + uArray.get(i).id + " to: " + orgId);
			}
			
			// Move Projects
			for(int i = 0; i < pArray.size(); i++) {
				pstmt3.setInt(1, orgId);
				pstmt3.setInt(2, pArray.get(i).id);
				
				log.info("Move Project: " + pstmt3.toString());
				pstmt3.executeUpdate();
				
				log.info("userevent: " + request.getRemoteUser() + " : move project : " + pArray.get(i).id + " to: " + orgId);
			}
			
			// Remove projects from users if they are in a different organisation
			for(int i = 0; i < uArray.size(); i++) {
				
				if(!uArray.get(i).keepProjects) {	// Org admin users keep all of their projects
				
					pstmt2.setInt(1, uArray.get(i).id);
					pstmt2.setInt(2, orgId);
					log.info("Delete Links to projects: " + pstmt2.toString());
					pstmt2.executeUpdate();
				}
			}
			
			// Move users from projects if they are in a different organisation
			for(int i = 0; i < pArray.size(); i++) {
				
				pstmt4.setInt(1, pArray.get(i).id);
				pstmt4.setInt(2, orgId);
				log.info("Delete Links to users: " + pstmt4.toString());
				pstmt4.executeUpdate();

			}
			
			response = Response.ok().build();
			sd.commit();
				
		} catch (SQLException e) {
			String state = e.getSQLState();
			log.info("Change organisation. sql state:" + state);
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}
			
		} catch (Exception ex) {
			log.info(ex.getMessage());
			response = Response.serverError().entity(ex.getMessage()).build();			
			try{	sd.rollback();	} catch(Exception e2) {}
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}	} catch (SQLException e) {}
			try {if (pstmt2 != null) {pstmt2.close();}	} catch (SQLException e) {}
			try {if (pstmt3 != null) {pstmt3.close();}	} catch (SQLException e) {}
			try {if (pstmt4 != null) {pstmt4.close();}	} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-OrganisationList-setOrganisation", sd);
		}
		
		return response;
	}

}

