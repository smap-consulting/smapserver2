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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import model.MediaResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.CssManager;
import org.smap.sdal.managers.CsvTableManager;
import org.smap.sdal.managers.LogManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

		Connection sd = null; 
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
			CssManager cm = new CssManager(basePath);
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
						
						// save the file
						File folder = cm.getCssServerFolder();
						String filePath = folder.getAbsolutePath() + File.separator + fileName;
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


	/*
	 * Return available CSS files
	 */
	@GET
	@Produces("application/json")
	public Response getMedia(
			@Context HttpServletRequest request
			) throws IOException {

		Response response = null;
		ArrayList<String> fileNames = new ArrayList<String> ();
		String connectionString = "surveyKPI - cssFiles - get";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		authServer.isAuthorised(sd, request.getRemoteUser());	

		try {
			/*
			 * Get the path to the files
			 */
			String basePath = GeneralUtilityMethods.getBasePath(request);
			CssManager cm = new CssManager(basePath);
			File folder = cm.getCssServerFolder();
			
			ArrayList <File> files = new ArrayList<File> (FileUtils.listFiles(folder, FileFilterUtils.fileFileFilter(), null));
			
			// Sort the files alphabetically
			Collections.sort( files, new Comparator<File>() {
			    public int compare( File a, File b ) {
			    	return a.getName().toLowerCase().compareTo(b.getName().toLowerCase());
			    }
			} );
		
			for(File f : files) {
				fileNames.add(f.getName());
			}

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(fileNames);
			response = Response.ok(resp).build();		

		}  catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().build();
		} finally {

			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;		
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