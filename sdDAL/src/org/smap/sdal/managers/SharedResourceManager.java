package org.smap.sdal.managers;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.FileItem;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Message;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/*****************************************************************************

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

 ******************************************************************************/

/*
 * Manage shared resources
 */
public class SharedResourceManager {
	
	private static Logger log =
			 Logger.getLogger(SharedResourceManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	public static long MAX_FILE_SIZE = 5000000;	// 5 Million Bytes
	
	ResourceBundle localisation;
	
	public SharedResourceManager(ResourceBundle localisation) {
		this.localisation = localisation;
	}
	/*
	 * Get the limit for a resource
	 */
	public Response add(Connection sd, 
			int sId, 
			int oId, 
			String basePath, 
			String user, 
			String resourceName, 
			FileItem fileItem, 
			String action) throws Exception {
		
		String responseCode = "success";
		StringBuilder responseMsg = new StringBuilder("");
		
		MediaInfo mediaInfo = new MediaInfo();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();


		
		// Get the file type from the extension of the uploaded file
		String uploadedFileName = fileItem.getName();
		String contentType = UtilityMethodsEmail.getContentType(uploadedFileName);
		
		String extension = "";
		if(uploadedFileName.lastIndexOf('.') > 0) {
			extension = uploadedFileName.substring(uploadedFileName.lastIndexOf('.'));
		}
		
		if(sId > 0) {
			mediaInfo.setFolder(basePath, sId, null, sd);
		} else {	
			// Upload to organisations folder
			oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			mediaInfo.setFolder(basePath, user, oId, false);				 
		}

		String folderPath = mediaInfo.getPath();

		if(folderPath != null) {		
			
			// Change the name of the resource to that specified by the user but keep the extension
			String filePath = folderPath + "/" + resourceName + extension;
			File savedFile = new File(filePath);		
			
			if(fileItem.getSize() > MAX_FILE_SIZE) {		// Check the size of the file
				
				responseCode = "error";
				String msg = localisation.getString("sr_tl");
				msg = msg.replace("%s1", String.format("%,d", fileItem.getSize()));
				msg = msg.replace("%s2", String.format("%,d", MAX_FILE_SIZE));
				responseMsg = new StringBuilder(msg);
				
			} else if(resourceName == null || resourceName.trim().length() == 0) {		// Validate the resource name
				
				responseCode = "error";
				responseMsg = new StringBuilder(localisation.getString("tu_nfs"));
				
			} else if(action.equals("add") && savedFile.exists()) {				// Make sure file does not already exist if adding
				
				responseCode = "error";
				String msg = localisation.getString("sr_ae");
				msg = msg.replace("%s1", resourceName + extension);
				responseMsg = new StringBuilder(msg);
				
			} else {	// Save the new file
				
				fileItem.write(savedFile);  			
				if(savedFile.exists()) {
					if(contentType.equals("text/csv") || uploadedFileName.endsWith(".csv")) {				
						// Upload any CSV data into a table, also checks maximum number of columns
						CsvTableManager csvMgr = new CsvTableManager(sd, localisation, oId, sId, resourceName);
						csvMgr.updateTable(savedFile);		
					} else {
						// Create thumbnails
						UtilityMethodsEmail.createThumbnail(resourceName, folderPath, savedFile);
					}
	
					// Create a message so that devices are notified of the change
					MessagingManager mm = new MessagingManager(localisation);
					if(sId > 0) {
						mm.surveyChange(sd, sId, 0);
					} else {
						mm.resourceChange(sd, oId, resourceName);
					}
				} else {
					responseCode = "error";
					responseMsg = new StringBuilder("Failed to save shared resource file: " + resourceName);
				}
			}

		} else {
			log.log(Level.SEVERE, "Media folder not found");
			responseCode = "error";
			responseMsg = new StringBuilder("Media folder not found");
		}
	
	
		return Response.ok(gson.toJson(new Message(responseCode, responseMsg.toString(), resourceName))).build();
	
	}
}


