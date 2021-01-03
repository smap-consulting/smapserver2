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

/*
 * Manage CSS files used for styling
 */
@Path("/css")
public class CssFiles extends Application {

	// Allow analysts and admin to upload resources for the whole organisation
	Authorise auth = null;
	Authorise authServer = null;

	LogManager lm = new LogManager();		// Application log

	public CssFiles() {

		// Administrators
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		auth = new Authorise(authorisations, null);

		// Owner / server level
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);
		authServer = new Authorise(authorisations, null);
	}

	private static Logger log =
			Logger.getLogger(CssFiles.class.getName());

	@POST
	public Response uploqd(
			@Context HttpServletRequest request
			) throws IOException {

		Response response = null;

		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();		

		fileItemFactory.setSizeThreshold(5*1024*1024);
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);

		int oId = 0;
		Connection sd = null; 
		boolean superUser = false;
		String connectionString = "surveyKPI - cssFiles - upload";

		try {

			sd = SDDataSource.getConnection(connectionString);

			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Authorisation - Access
			authServer.isAuthorised(sd, request.getRemoteUser());	

			// End authorisation

			String basePath = GeneralUtilityMethods.getBasePath(request);

			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();

			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();

				// Get form parameters

				if(item.isFormField()) {

				} else if(!item.isFormField()) {

					if(item.getFieldName().equals("file")) {
						String fileName = item.getName();
						fileName = fileName.replaceAll(" ", "_"); // Remove spaces from file name
	
						// Validation
						String contentType = UtilityMethodsEmail.getContentType(fileName);
						if(!contentType.equals("text/css")) {
							throw new ApplicationException(localisation.getString("css_type"));
						}
						if(item.getSize() > 300000) {
							throw new ApplicationException(localisation.getString("css_size"));
						}
	
						// Make sure the folder exists
						String folderPath = basePath + File.separator + "css";
						File folder = new File(folderPath);
						FileUtils.forceMkdir(folder);	
						
						// save the file
						String filePath = folderPath + File.separator + fileName;
						File savedFile = new File(filePath);
						item.write(savedFile);  // Save the new file
					}

				}
			}

			response = Response.ok().build();


		} catch(Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
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
			mediaInfo.setFolder(basePath, request.getRemoteUser(), oId, sd, false);				 

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
				mediaInfo.setFolder(basePath, user, oId, sd, false);				 
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
				mediaInfo.setFolder(basePath, user, oId, sd, false);
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
					mediaInfo.setFolder(basePath, user, oId, sd, false);				 
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