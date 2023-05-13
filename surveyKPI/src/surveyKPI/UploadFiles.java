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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import model.MediaResponse;
import utilities.JavaRosaUtilities;
import utilities.XLSTemplateUploadManager;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.smap.model.SurveyTemplate;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.ApplicationWarning;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.CsvTableManager;
import org.smap.sdal.managers.LanguageCodeManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.ChangeElement;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.FormLength;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MediaItem;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.Survey;
import org.smap.server.utilities.PutXForm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/upload")
public class UploadFiles extends Application {

	// Allow analysts and admin to upload resources for the whole organisation
	Authorise auth = null;

	LogManager lm = new LogManager();		// Application log

	public UploadFiles() {

		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		auth = new Authorise(authorisations, null);
	}

	private static Logger log =
			Logger.getLogger(UploadFiles.class.getName());
	
	@POST
	@Produces("application/json")
	@Path("/media")
	public Response sendMedia(
			@QueryParam("getlist") boolean getlist,
			@QueryParam("survey_id") int sId,
			@Context HttpServletRequest request
			) throws IOException {

		Response response = null;

		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();		
		String user = request.getRemoteUser();

		log.info("upload files - media -----------------------");

		fileItemFactory.setSizeThreshold(5*1024*1024);
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);

		int oId = 0;
		Connection sd = null; 
		boolean superUser = false;
		String connectionString = "surveyKPI - uploadFiles - sendMedia";
		
		try {
			
			sd = SDDataSource.getConnection(connectionString);
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// Authorisation - Access
			auth.isAuthorised(sd, request.getRemoteUser());	
			if(sId > 0) {
				try {
					superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
				} catch (Exception e) {
				}
				auth.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
			} 
			// End authorisation

			
			String basePath = GeneralUtilityMethods.getBasePath(request);
			MediaInfo mediaInfo = new MediaInfo();
			
			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();

			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();

				// Get form parameters

				if(item.isFormField()) {

					if(item.getFieldName().equals("survey_id")) {
						try {
							sId = Integer.parseInt(item.getString());
						} catch (Exception e) {

						}
					}
					// Check authorisation for this survey id
					if(sId > 0) {
						try {
							superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
						} catch (Exception e) {
						}
						auth.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
					} 

				} else if(!item.isFormField()) {
					// Handle Uploaded files.
					log.info("Field Name = "+item.getFieldName()+
							", File Name = "+item.getName()+
							", Content type = "+item.getContentType()+
							", File Size = "+item.getSize());

					String fileName = item.getName();
					fileName = fileName.replaceAll(" ", "_"); // Remove spaces from file name

					if(sId > 0) {
						mediaInfo.setFolder(basePath, sId, null, sd);
					} else {	
						// Upload to organisations folder
						oId = GeneralUtilityMethods.getOrganisationId(sd, user);
						mediaInfo.setFolder(basePath, user, oId, false);				 
					}
					mediaInfo.setServer(request.getRequestURL().toString());

					String folderPath = mediaInfo.getPath();
					fileName = mediaInfo.getFileName(fileName);

					if(folderPath != null) {		
						
						String contentType = UtilityMethodsEmail.getContentType(fileName);
						
						String filePath = folderPath + "/" + fileName;
						File savedFile = new File(filePath);
						File oldFile = new File (filePath + ".old");
						
						// If this is a CSV file save the old version if it exists so that we can do a diff on it
						if(savedFile.exists() && (contentType.equals("text/csv") || fileName.endsWith(".csv"))) {
							Files.copy(savedFile.toPath(), oldFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
						}

						item.write(savedFile);  // Save the new file

						// Create thumbnails
						UtilityMethodsEmail.createThumbnail(fileName, folderPath, savedFile);

						// Upload any CSV data into a table
						if(contentType.equals("text/csv") || fileName.endsWith(".csv")) {
							putCsvIntoTable(sd, localisation, user, sId, fileName, savedFile, oldFile, basePath, mediaInfo);
						}

						// Create a message so that devices are notified of the change
						MessagingManager mm = new MessagingManager(localisation);
						if(sId > 0) {
							mm.surveyChange(sd, sId, 0);
						} else {
							mm.resourceChange(sd, oId, fileName);
						}

					} else {
						log.log(Level.SEVERE, "Media folder not found");
						response = Response.serverError().entity("Media folder not found").build();
					}
				}
			}
			
			if(getlist) {
				MediaResponse mResponse = new MediaResponse ();
				mResponse.files = mediaInfo.get(sId, null);			
				Gson gson = new GsonBuilder().disableHtmlEscaping().create();
				String resp = gson.toJson(mResponse);
				log.info("Responding with " + mResponse.files.size() + " files");

				response = Response.ok(resp).build();
			} else {
				response = Response.ok().build();
			}

		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;

	}

	@DELETE
	@Produces("application/json")
	@Path("/media/organisation/{oId}/{filename}")
	public Response deleteMedia(
			@PathParam("oId") int oId,
			@PathParam("filename") String filename,
			@Context HttpServletRequest request
			) throws IOException {

		Response response = null;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-UploadFiles");
		auth.isAuthorised(sd, request.getRemoteUser());
		auth.isValidOrganisation(sd, request.getRemoteUser(), oId);
		// End Authorisation		

		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String basePath = GeneralUtilityMethods.getBasePath(request);
			String serverName = request.getServerName();

			deleteFile(request, sd, localisation, basePath, serverName, null, oId, filename, request.getRemoteUser());
			if(filename.endsWith(".csv")) {
				  // Delete the organisation shared resources - not necessary
			    CsvTableManager tm = new CsvTableManager(sd, localisation);
			    tm.delete(oId, 0, filename);		
			}
			
			MediaInfo mediaInfo = new MediaInfo();
			mediaInfo.setServer(request.getRequestURL().toString());
			mediaInfo.setFolder(basePath, request.getRemoteUser(), oId, false);				 

			MediaResponse mResponse = new MediaResponse ();
			mResponse.files = mediaInfo.get(0, null);			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(mResponse);
			response = Response.ok(resp).build();	

		} catch(Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection("surveyKPI-UploadFiles", sd);
		}

		return response;

	}

	@DELETE
	@Produces("application/json")
	@Path("/media/{sIdent}/{filename}")
	public Response deleteMediaSurvey(
			@PathParam("sIdent") String sIdent,
			@PathParam("filename") String filename,
			@Context HttpServletRequest request
			) throws IOException {

		Response response = null;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-UploadFiles");
		auth.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation		

		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String basePath = GeneralUtilityMethods.getBasePath(request);
			String serverName = request.getServerName(); 

			deleteFile(request, sd, localisation, basePath, serverName, sIdent, 0, filename, request.getRemoteUser());
			if(filename.endsWith(".csv")) {
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
				  // Delete the organisation shared resources - not necessary
			    CsvTableManager tm = new CsvTableManager(sd, localisation);
			    tm.delete(oId, sId, null);		
			}
			
			MediaInfo mediaInfo = new MediaInfo();
			mediaInfo.setServer(request.getRequestURL().toString());
			mediaInfo.setFolder(basePath, 0, sIdent, sd);

			MediaResponse mResponse = new MediaResponse ();
			mResponse.files = mediaInfo.get(0, null);			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(mResponse);
			response = Response.ok(resp).build();	

		} catch(Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection("surveyKPI-UploadFiles", sd);
		}

		return response;

	}
	
	/*
	 * Return available files
	 */
	@GET
	@Produces("application/json")
	@Path("/media")
	public Response getMedia(
			@Context HttpServletRequest request,
			@QueryParam("survey_id") int sId,
			@QueryParam("getall") boolean getall
			) throws IOException {

		Response response = null;
		String user = request.getRemoteUser();
		boolean superUser = false;

		/*
		 * Authorise
		 *  If survey ident is passed then check user access to survey
		 *  Else provide access to the media for the organisation
		 */
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-UploadFiles");
		if(sId > 0) {
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			auth.isAuthorised(sd, request.getRemoteUser());
			auth.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		} else {
			auth.isAuthorised(sd, request.getRemoteUser());
		}
		// End Authorisation		

		/*
		 * Get the path to the files
		 */
		String basePath = GeneralUtilityMethods.getBasePath(request);

		MediaInfo mediaInfo = new MediaInfo();
		mediaInfo.setServer(request.getRequestURL().toString());

		PreparedStatement pstmt = null;		
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			// Get the path to the media folder	
			if(sId > 0) {
				mediaInfo.setFolder(basePath, sId, null, sd);
			} else {		
				mediaInfo.setFolder(basePath, user, oId, false);				 
			}

			log.info("Media query on: " + mediaInfo.getPath());

			MediaResponse mResponse = new MediaResponse();
			mResponse.files = mediaInfo.get(sId, null);	
			
			if(sId > 0 && getall) {
				// Get a hashmap of the names to exclude
				HashMap<String, String> exclude = new HashMap<> ();
				for(MediaItem mi : mResponse.files) {
					exclude.put(mi.name, mi.name);
				}
				// Add the organisation level media
				mediaInfo.setFolder(basePath, user, oId, false);
				mResponse.files.addAll(mediaInfo.get(0, exclude));
				
			}
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(mResponse);
			response = Response.ok(resp).build();		

		}  catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().build();
		} finally {

			if (pstmt != null) { try {pstmt.close();} catch (SQLException e) {}}

			SDDataSource.closeConnection("surveyKPI-UploadFiles", sd);
		}

		return response;		
	}

	private class Message {
		@SuppressWarnings("unused")
		String status;
		@SuppressWarnings("unused")
		String message;
		@SuppressWarnings("unused")
		String name;
		
		public Message(String status, String message, String name) {
			this.status = status;
			this.message = message;
			this.name = name;
		}
	}
	
	/*
	 * Upload a survey form
	 * curl -u neil -i -X POST -H "Content-Type: multipart/form-data" -F "projectId=1" -F "templateName=age" -F "tname=@x.xls" http://localhost/surveyKPI/upload/surveytemplate
	 */
	@POST
	@Produces("application/json")
	@Path("/surveytemplate")
	public Response uploadForm(
			@Context HttpServletRequest request) {
		
		Response response = null;
		String connectionString = "CreateXLSForm-uploadForm";
		
		log.info("upload survey -----------------------");
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();
		String displayName = null;
		int projectId = -1;
		int surveyId = -1;
		String fileName = null;
		String type = null;			// xls or xlsx or xml
		boolean isExcel;
		FileItem fileItem = null;
		String user = request.getRemoteUser();
		String action = null;
		int existingSurveyId = 0;	// The ID of a survey that is being replaced
		String bundleSurveyIdent = null;

		fileItemFactory.setSizeThreshold(5*1024*1024); 
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		Connection sd = SDDataSource.getConnection(connectionString); 
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		ArrayList<ApplicationWarning> warnings = new ArrayList<> ();

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		PreparedStatement pstmtChangeLog = null;
		PreparedStatement pstmtUpdateChangeLog = null;
		
		try {
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			
			boolean superUser = false;
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			
			// Authorise the user
			auth.isAuthorised(sd, request.getRemoteUser());
			
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
						log.info("Project: " + projectId);
						
						if(projectId < 0) {
							throw new Exception("No project selected");
						} else {
							// Authorise access to project
							auth.isValidProject(sd, request.getRemoteUser(), projectId);
						}
						
					} else if(item.getFieldName().equals("surveyId")) {
						try {
							// Authorise access to existing survey
							surveyId = Integer.parseInt(item.getString());
							if(surveyId > 0) {
								auth.isValidSurvey(sd, request.getRemoteUser(), surveyId, false, superUser);	// Check the user has access to the survey
							}
						} catch (Exception e) {
							
						}
						log.info("Add to bundle: " + surveyId);
						
					} else if(item.getFieldName().equals("action")) {						
						action = item.getString();
						log.info("Action: " + action);
						
					} else {
						log.info("Unknown field name = "+item.getFieldName()+", Value = "+item.getString());
					}
				} else {
					fileItem = (FileItem) item;
				}
			} 
			
			// Get the file type from its extension
			fileName = fileItem.getName();
			if(fileName == null || fileName.trim().length() == 0) {
				throw new ApplicationException(localisation.getString("tu_nfs"));
			} else if(fileName.endsWith(".xlsx")) {
				type = "xlsx";
				isExcel = true;
			} else if(fileName.endsWith(".xls")) {
				type = "xls";
				isExcel = true;
			} else if(fileName.endsWith(".xml")) {
				type = "xml";
				isExcel = false;
			} else {
				throw new ApplicationException(localisation.getString("tu_uft"));
			}
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			Survey existingSurvey = null;
			String basePath = GeneralUtilityMethods.getBasePath(request);
			
			HashMap<String, String> groupForms = null;		// Maps form names to table names - When merging to an existing survey
			HashMap<String, QuestionForm> questionNames = null;	// Maps unabbreviated question names to abbreviated question names
			HashMap<String, String> optionNames = null;		// Maps unabbreviated option names to abbreviated option names
			int existingVersion = 1;						// Make the version of a survey that replaces an existing survey one greater
			boolean merge = false;							// Set true if an existing survey is to be replaced or this survey is to be merged with an existing survey
			
			if(surveyId > 0) {
				
				/*
				 * Either a survey is being replaced, in which case the action is "replace"
				 * or a survey is being added to a bundle in which case the action is "add"
				 */
				// Hack.  Because the client is sending surveyId's instead of idents we need to get the latest
				// survey id or we risk updating an old version
				surveyId = GeneralUtilityMethods.getLatestSurveyId(sd, surveyId);
				
				merge = true;
				bundleSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, surveyId);
				groupForms = sm.getGroupForms(sd, bundleSurveyIdent);
				questionNames = sm.getGroupQuestionsMap(sd, bundleSurveyIdent, null, false);	
				optionNames = sm.getGroupOptions(sd, bundleSurveyIdent);
				
				/*
				 * Check that the bundle of the new / replaced survey does not have roles
				 * If it does only the super user can add a new or replace a survey
				 */
				if(!superUser && GeneralUtilityMethods.bundleHasRoles(sd, request.getRemoteUser(), bundleSurveyIdent)) {
					throw new ApplicationException(localisation.getString("tu_roles"));
				}
			}
			
			if(action == null) {
				action = "add";
			} else if(action.equals("replace")) {
				
				existingSurvey = sm.getById(sd, cResults, user, false, surveyId, 
						false, basePath, null, false, false, false, 
						false, false, null, false, false, superUser, null, 
						false,
						false,		// launched only
						false		// Don't merge set values into default value
						);
				displayName = existingSurvey.displayName;
				existingVersion = existingSurvey.version;
				existingSurveyId = existingSurvey.id;
			}

			// If the survey display name already exists on this server, for this project, then throw an error		

			if(!action.equals("replace") && sm.surveyExists(sd, displayName, projectId)) {
				String msg = localisation.getString("tu_ae");
				msg = msg.replaceAll("%s1", displayName);
				throw new ApplicationException(msg);
			}
			
			Survey s = null;					// Used for XLS uploads (The new way)
			SurveyTemplate model = null;		// Used for XML uploads (The old way), Convert to Survey after loading
			if(isExcel) {
				XLSTemplateUploadManager tum = new XLSTemplateUploadManager(localisation, warnings);
				s = tum.getSurvey(sd, 
						oId, 
						type, 
						fileItem.getInputStream(), 
						displayName,
						projectId,
						questionNames,
						optionNames,
						merge,
						existingVersion);
			} else if(type.equals("xml")) {

				// Parse the form into an object model
				PutXForm loader = new PutXForm(localisation);
				model = loader.put(fileItem.getInputStream(), 
						request.getRemoteUser(),
						basePath);	// Load the XForm into the model
				
				// Set the survey name to the one entered by the user 
				if(displayName != null && displayName.length() != 0) {
					model.getSurvey().setDisplayName(displayName);
				} else {
					throw new Exception("No Survey name");
				}

				// Set the project id to the one entered by the user 
				if(projectId != -1) {
					model.getSurvey().setProjectId(projectId);
				} 
				
			} else {
				throw new ApplicationException("Unsupported file type");
			}
				
			/*
			 * Get information on a survey group if this survey is to be added to one
			 * TODO make XML uploads work with this
			 */
			if(surveyId > 0) {
				if(!action.equals("replace")) {
					s.groupSurveyIdent = bundleSurveyIdent;
					
				} else {
					// Set the group survey ident to the same value as the original survey
					s.groupSurveyIdent = existingSurvey.groupSurveyIdent;
					s.publicLink = existingSurvey.publicLink;
				}

				/*
				 * Validate that the survey is compatible with any groups that it
				 * has been added to
				 */
				for(Form f : s.forms) {
					for(Question q : f.questions) {
						QuestionForm qt = questionNames.get(q.name);
						if(qt != null) {
							if(!qt.reference && !qt.formName.equals(f.name) && GeneralUtilityMethods.isDatabaseQuestion(qt.qType)) {
								String msg = localisation.getString("tu_gq");
								msg = msg.replace("%s1", q.name);
								msg = msg.replace("%s2", f.name);
								msg = msg.replace("%s3", qt.formName);
								throw new ApplicationException(msg);
							}
						}
					}
				}
			} 
			
			/*
			 * Save the survey to the database
			 */
			if(isExcel) {
				s.write(sd, cResults, localisation, request.getRemoteUser(), groupForms, existingSurveyId, oId);
				String msg = null;
				String title = null;
				if(action.equals("replace")) {
					msg = localisation.getString("log_sr");
					title = LogManager.REPLACE;
				} else {
					msg = localisation.getString("log_sc");
					title = LogManager.CREATE;
				}
				
				msg = msg.replace("%s1", s.getDisplayName()).replace("%s2", s.getIdent());
				lm.writeLog(sd, s.getId(), user, title, msg, 0, request.getServerName());
				
				/*
				 * If the survey is being replaced and if it has an existing  geometry called the_geom and there is no
				 * geometry of that name in the new form then create a warning
				 * This warning can be removed in version 21.06
				 */
				if(action.equals("replace")) {
					for(Form f : s.forms) {
						
						boolean newTheGeom = false;
						boolean newNonTheGeom = false;
						ArrayList<String> newGeomQuestions = new ArrayList<> ();
						for(Question q : f.questions) {
							if(q.type.equals("geopoint") || q.type.equals("geotrace") || q.type.equals("geoshape") || q.type.equals("geocompound")) {
								if(q.name.equals("the_geom")) {	// keep this
									newTheGeom = true;
								} else {
									newNonTheGeom = true;
									newGeomQuestions.add(q.name);
								}
							}
	
						}
						if(newNonTheGeom && !newTheGeom) {
							if(GeneralUtilityMethods.hasTheGeom(sd, existingSurveyId, f.id)) {		
								String gMsg = localisation.getString("tu_geom");
								gMsg = gMsg.replace("%s1", f.name);
								gMsg = gMsg.replace("%s2", newGeomQuestions.toString());
								warnings.add(new ApplicationWarning(gMsg));
							}
						}
					}	
				}
				
			} else {
				int sId = model.writeDatabase();
				s = sm.getById(sd, cResults, user, false, sId, true, basePath, 
						null,					// instance id 
						false, 					// get results
						false, 					// generate dummy values
						true, 					// get property questions
						false, 					// get soft deleted
						false, 					// get HRK
						"internal", 			// Get External options
						false, 					// get change history
						true, 					// get roles
						superUser, 
						null	,				// geom format
						false,					// Include child surveys
						false,					// launched only
						false		// Don't merge set values into default value
						);
			}
			
			/*
			 * Validate the survey:
			 * 	 using the JavaRosa API
			 *    Ensure that it fits within the limit of 1,600 columns
			 */
			boolean valid = true;
			String errMsg = null;
			try {
				JavaRosaUtilities.javaRosaSurveyValidation(localisation, s.id, request.getRemoteUser(), tz, request);
			} catch (Exception e) {
								
				// Error! Delete the survey we just created
				log.log(Level.SEVERE, e.getMessage(), e);
				valid = false;
					
				errMsg = e.getMessage();			
				if(errMsg != null && errMsg.startsWith("no <translation>s defined")) {
					errMsg = localisation.getString("tu_nl");
				}					
				
			}
			if(valid) {
				ArrayList<FormLength> formLength = GeneralUtilityMethods.getFormLengths(sd, s.id);
				for(FormLength fl : formLength) {
					if(fl.isTooLong()) {
						valid = false;
						errMsg = localisation.getString("tu_tmq");
						errMsg = errMsg.replaceAll("%s1", String.valueOf(fl.questionCount));
						errMsg = errMsg.replaceAll("%s2", fl.name);
						errMsg = errMsg.replaceAll("%s3", String.valueOf(FormLength.MAX_FORM_LENGTH));
						errMsg = errMsg.replaceAll("%s4", fl.lastQuestionName);
						break;	// Only show one form that is too long - This error should very rarely be encountered!
					}
				}
			}
			
			if(!valid) {
				sm.delete(sd, 
						cResults, 
						s.id, 
						true,		// hard
						false,		// Do not delete the data 
						user, 
						basePath,
						"no",		// Do not delete the tables
						0);		// New Survey Id for replacement 
				throw new ApplicationException(errMsg);	// report the error
			}
					
			if(action.equals("replace")) {
				/*
				 * Soft delete the old survey
				 * Set task groups to use the new survey
				 */
				sm.delete(sd, 
						cResults, 
						surveyId, 
						false,		// set soft 
						false,		// Do not delete the data 
						user, 
						basePath,
						"no",		// Do not delete the tables
						s.id		   // New Survey Id for replacement 
					);		
			}
			
			/*
			 * Save the file to disk
			 */
			String fileFolder = basePath + "/templates/" + projectId +"/"; 
			String targetName = GeneralUtilityMethods.convertDisplayNameToFileName(displayName, false);
			String filePath = fileFolder + targetName + "." + type;
			
			// 1. Create the project folder if it does not exist
			File folder = new File(fileFolder);
			FileUtils.forceMkdir(folder);

			// 2. Save the file
			File savedFile = new File(filePath);
			fileItem.write(savedFile);	
			
			/*
			 * Update the change history for the survey
			 */
			int newVersion = existingVersion;
			if(action.equals("replace")) {
				newVersion++;
				String sqlUpdateChangeLog = "insert into survey_change "
						+ "(s_id, version, changes, user_id, apply_results, visible, updated_time) "
						+ "select "
						+ s.id
						+ ",version, changes, user_id, apply_results, visible, updated_time "
						+ "from survey_change where s_id = ? "
						+ "order by version asc";
				pstmtUpdateChangeLog = sd.prepareStatement(sqlUpdateChangeLog);
				pstmtUpdateChangeLog.setInt(1, existingSurveyId);
				pstmtUpdateChangeLog.execute();
			}
			/*
			 * Add a new entry to the change history
			 */
			String sqlChangeLog = "insert into survey_change " +
					"(s_id, version, changes, user_id, apply_results, visible, updated_time) " +
					"values(?, ?, ?, ?, 'true', ?, ?)";
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
			ChangeItem ci = new ChangeItem();
			ci.fileName = fileItem.getName();
			ci.origSId = s.id;
			pstmtChangeLog.setInt(1, s.id);
			pstmtChangeLog.setInt(2, newVersion);
			pstmtChangeLog.setString(3, gson.toJson(new ChangeElement(ci, "upload_template")));
			pstmtChangeLog.setInt(4, GeneralUtilityMethods.getUserId(sd, user));	
			pstmtChangeLog.setBoolean(5,true);	
			pstmtChangeLog.setTimestamp(6, GeneralUtilityMethods.getTimeStamp());
			pstmtChangeLog.execute();
			
			StringBuilder responseMsg = new StringBuilder("");
			String responseCode = "success";
			if(warnings.size() > 0) {
				for(ApplicationWarning w : warnings) {
					responseMsg.append("<br/> - ").append(w.getMessage());
				}
				responseCode = "warning";
			}
			
			/*
			 * Apply auto translations
			 */
			if(s.autoTranslate && s.languages.size() > 1) {
				
				LanguageCodeManager lcm = new LanguageCodeManager();
				Language fromLanguage = s.languages.get(0);
				if(fromLanguage.code != null && lcm.isSupported(sd, fromLanguage.code, LanguageCodeManager.LT_TRANSLATE)) {
					for(int i = 1; i < s.languages.size(); i++) {
						Language toLanguage = s.languages.get(i);
						if(toLanguage.code != null && lcm.isSupported(sd, toLanguage.code, LanguageCodeManager.LT_TRANSLATE)) {
							
							String result = sm.translate(sd, request.getRemoteUser(), s.id,
									0,	// from language index
									i,	// to language index
									fromLanguage.code,
									toLanguage.code,
									false,	// Do not overwrite
									basePath
									);
							if(result != null) {
								responseCode = "warning";
								responseMsg.append(localisation.getString(result).replace("%s1",  LogManager.TRANSLATE));
							}
						} else {
							responseCode = "warning";
							if(toLanguage.code == null) {
								responseMsg.append(localisation.getString("aws_t_nlc").replace("%s1",  toLanguage.name));
							} else {
								responseMsg.append(localisation.getString("aws_t_ilc").replace("%s1",  toLanguage.code));
							}
						}
					}
				} else {
					responseCode = "warning";
					if(fromLanguage.code == null) {
						responseMsg.append(localisation.getString("aws_t_nlc").replace("%s1",  fromLanguage.name));
					} else {
						responseMsg.append(localisation.getString("aws_t_ilc").replace("%s1",  fromLanguage.code));
					}
				}
				
			}
			
			response = Response.ok(gson.toJson(new Message(responseCode, responseMsg.toString(), displayName))).build();
			
		} catch(AuthorisationException ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			throw ex;
		}catch(ApplicationException ex) {		
			response = Response.ok(gson.toJson(new Message("error", ex.getMessage(), displayName))).build();
		} catch(FileUploadException ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			if(pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (Exception e) {}
			if(pstmtUpdateChangeLog != null) try {pstmtUpdateChangeLog.close();} catch (Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}
		
		return response;
	}
	
	/*
	 * Put the CSV file into a database table
	 */
	private void putCsvIntoTable(
		Connection sd, 
		ResourceBundle localisation,
		String user, 
		int sId, 
		String csvFileName, 
		File csvFile,
		File oldCsvFile,
		String basePath,
		MediaInfo mediaInfo) throws Exception {
		
		int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
		CsvTableManager csvMgr = new CsvTableManager(sd, localisation, oId, sId, csvFileName);
		csvMgr.updateTable(csvFile, oldCsvFile);
		
	}
	
	/*
	 * Delete the file
	 */
	private void deleteFile(
			HttpServletRequest request, 
			Connection sd, 
			ResourceBundle localisation,
			String basePath, 
			String serverName, 
			String sIdent, 
			int oId, 
			String filename, 
			String user) throws Exception {

		String path = null;
		String thumbsFolder = null;
		String fileBase = null;

		int idx = filename.lastIndexOf('.');
		if(idx >= 0) {
			fileBase = filename.substring(0, idx);
		}

		if(filename != null) {
			if(sIdent != null) {
				path = basePath + "/media/" + sIdent + "/" + filename;
				if(fileBase != null) {
					thumbsFolder = basePath + "/media/" + sIdent + "/thumbs";
				}
			} else if( oId > 0) {
				path = basePath + "/media/organisation/" + oId + "/" + filename;
				if(fileBase != null) {
					thumbsFolder = basePath + "/media/organisation/" + oId + "/thumbs";
				}
			}

			// Apply changes from CSV files to survey definition	
			File f = new File(path);
			File oldFile = new File(path + ".old");
			String fileName = f.getName();
			
			// Delete options added to the database for this file
			if(fileName.endsWith(".csv")) {
				int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
				MediaInfo mediaInfo = new MediaInfo();
				if(sId > 0) {
					mediaInfo.setFolder(basePath, sId, null, sd);
				} else {	
					// Upload to organisations folder
					mediaInfo.setFolder(basePath, user, oId, false);				 
				}
				mediaInfo.setServer(request.getRequestURL().toString());
				
				//applyCSVChanges(sd, null, localisation, user, sId, fileName, null, null, basePath, mediaInfo);
			}

			f.delete();		
			if(oldFile.exists()) {
				oldFile.delete();	
			}
			
			log.info("userevent: " + user + " : delete media file : " + filename);

			// Delete any matching thumbnails
			if(fileBase != null) {
				File thumbs = new File(thumbsFolder);
				for(File thumb : thumbs.listFiles()) {
					if(thumb.getName().startsWith(fileBase)) {
						thumb.delete();
					}
				}
			}

		}


	}

}