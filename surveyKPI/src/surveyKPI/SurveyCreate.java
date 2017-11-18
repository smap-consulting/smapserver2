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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.Location;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import utilities.XLSFormManager;
import utilities.XLSTaskManager;

/*
 * Creates an XLS Form from the survey definition
 */

@Path("/survey/create")
public class SurveyCreate extends Application {
	
	Authorise a = new Authorise(null, Authorise.ANALYST);
	
	LogManager lm = new LogManager();		// Application log
	
	private static Logger log =
			 Logger.getLogger(SurveyCreate.class.getName());
	
	
	

	/*
	 * Upload a form
	 */
	@POST
	@Produces("application/json")
	@Path("/upload")
	public Response uploadForm(
			@Context HttpServletRequest request) {
		
		Response response = null;
		
		log.info("upload survey -----------------------");
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();
		String displayName = null;
		int projectId = -1;
		String surveyIdent = null;
		String projectName = null;
		String fileName = null;
		String serverName = request.getServerName();
		FileItem uploadedFile = null;
		ArrayList<String> warnings = new ArrayList<String>(); 

		fileItemFactory.setSizeThreshold(5*1024*1024); //1 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		Connection sd = SDDataSource.getConnection("CreateXLSForm-uploadForm"); 

		try {
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
												
			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();
			while(itr.hasNext()) {
				
				FileItem item = (FileItem) itr.next();

				if(item.isFormField()) {
					if(item.getFieldName().equals("templateName")) {
						displayName = item.getString("utf-8");
						if(displayName != null) {
							displayName = displayName.trim();
						}
						log.info("Template: " + displayName);
						
						
					} else if(item.getFieldName().equals("projectId")) {
						projectId = Integer.parseInt(item.getString());
						log.info("Template: " + projectId);
						
						// Authorisation - Access
						if(projectId < 0) {
							throw new Exception("No project selected");
						} else {
							a.isAuthorised(sd, request.getRemoteUser());
							a.isValidProject(sd, request.getRemoteUser(), projectId);
						}
						// End Authorisation
						
						// Get the project name
						PreparedStatement pstmt = null;
						try {
							String sql = "select name from project where id = ?;";
							pstmt = sd.prepareStatement(sql);
							pstmt.setInt(1, projectId);
							ResultSet rs = pstmt.executeQuery();
							if(rs.next()) {
								projectName = rs.getString(1);
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							if (pstmt != null) { try {pstmt.close();} catch (SQLException e) {}}
						}
					} else if(item.getFieldName().equals("surveyIdent")) {
						surveyIdent = item.getString();
						if(surveyIdent != null) {
							surveyIdent = surveyIdent.trim();
						}
						log.info("Survey Ident: " + surveyIdent);
					
					}else {
						log.info("Unknown field name = "+item.getFieldName()+", Value = "+item.getString());
					}
				} else {
					uploadedFile = (FileItem) item;
				}
			} 
			
			System.out.println("Uploading: " + uploadedFile.getName());
			
			
		} catch(FileUploadException ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
	
			SDDataSource.closeConnection("CreateXLSForm-uploadForm", sd);
			
		}
		
		return response;
	}
}
