package surveyKPI;

import java.io.IOException;

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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Creates an XLS Form from the survey definition
 * curl -u neil -i -X POST -H "Content-Type: multipart/form-data" -F "projectId=1" -F "templateName=age" -F "tname=@x.xls" http://localhost/surveyKPI/survey/create/upload
 */

@Path("/survey/create")
public class SurveyCreate extends Application {
	
	Authorise a = new Authorise(null, Authorise.ANALYST);
	
	LogManager lm = new LogManager();		// Application log
	
	private static Logger log =
			 Logger.getLogger(SurveyCreate.class.getName());
	
	private class Message {
		String status;
		String host;
		ArrayList<String> mesgArray;
		String project;
		String survey;
		String fileName;
		String administrator;
		ArrayList<String> hints;
		ArrayList<String> warnings;
	}

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
	
		ArrayList<String> mesgArray = new ArrayList<String> ();
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
			
			fileName = uploadedFile.getName();

			// If the survey display name already exists on this server, for this project, then throw an error		
			SurveyManager sm = new SurveyManager(localisation);
			if(sm.surveyExists(sd, displayName, projectId)) {
				mesgArray.add("$c_survey");
				mesgArray.add(" '");
				mesgArray.add(displayName);
				mesgArray.add("' ");
				mesgArray.add("$e_u_exists");
				mesgArray.add(" '");
				mesgArray.add(projectName);
				mesgArray.add("'");

				ArrayList<String> hints = new ArrayList<String>(); 
				hints.add("$e_h_rename");

				return getErrorResponse(request,  mesgArray, hints, warnings, serverName, projectName, displayName, fileName);
			} 	
			
			
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
	
	private Response getErrorResponse(HttpServletRequest request, 
			ArrayList<String> mesgArray, 
			ArrayList<String> hints, 
			ArrayList<String> warnings,
			String serverName, 
			String projectName, 
			String surveyName, 
			String fileName) throws ServletException, IOException {

		Connection connectionSD = SDDataSource.getConnection("fieldManager-Template Upload");
		String admin_email = "administrator@smap.com.au";

		// Get the email address of the organisational administrator
		PreparedStatement pstmt = null;
		try {
			String sql = "select admin_email from organisation o, users u " +
					"where o.id = u.o_id " +
					"and u.ident = ?";
			pstmt = connectionSD.prepareStatement(sql);
			pstmt.setString(1, request.getRemoteUser());
			log.info("Get admin email:" + pstmt.toString());

			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String email = rs.getString(1);
				if(email != null && email.trim().length() > 0) {
					admin_email = email;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) { try {pstmt.close();} catch (SQLException e) {}}
			SDDataSource.closeConnection("fieldManager-Template Upload", connectionSD);
		}

		Message m = new Message();
		if(mesgArray != null) {
			m.status = "error";
		} else {
			m.status = "warning";
		}
		m.mesgArray = mesgArray;
		m.host = serverName;
		m.project = projectName;
		m.survey = surveyName;
		m.fileName = fileName;
		m.administrator = admin_email;
		m.hints = hints;
		m.warnings = warnings;

		log.info("Returning error response: " + m.toString());

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		return Response.ok(gson.toJson(m)).build();


	}
}
