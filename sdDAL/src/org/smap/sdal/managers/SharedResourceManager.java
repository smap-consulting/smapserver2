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
	
	ResourceBundle localisation;
	
	public SharedResourceManager(ResourceBundle localisation) {
		this.localisation = localisation;
	}
	/*
	 * Get the limit for a resource
	 */
	public Response add(Connection sd, int sId, int oId, String basePath, String user, String resourceName, FileItem fileItem) throws Exception {
		
		String responseCode = "success";
		StringBuilder responseMsg = new StringBuilder("");
		
		MediaInfo mediaInfo = new MediaInfo();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();

		// Get the file type from its extension
		String fileName = fileItem.getName();
		if(fileName == null || fileName.trim().length() == 0) {
			throw new ApplicationException(localisation.getString("tu_nfs"));
		} 
		String contentType = UtilityMethodsEmail.getContentType(fileName);
		
		if(sId > 0) {
			mediaInfo.setFolder(basePath, sId, null, sd);
		} else {	
			// Upload to organisations folder
			oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			mediaInfo.setFolder(basePath, user, oId, false);				 
		}

		String folderPath = mediaInfo.getPath();

		if(folderPath != null) {		
			
			String filePath = folderPath + "/" + fileName;
			File savedFile = new File(filePath);
			File oldFile = new File (filePath + ".old");
			
			fileItem.write(savedFile);  // Save the new file
			
			if(savedFile.exists()) {
				// If this is a CSV file save the old version if it exists so that we can do a diff on it
				if(contentType.equals("text/csv") || fileName.endsWith(".csv")) {
					Files.copy(savedFile.toPath(), oldFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

					// Upload any CSV data into a table
					// Also checks maximum number of columns
					CsvTableManager csvMgr = new CsvTableManager(sd, localisation, oId, sId, fileName);
					csvMgr.updateTable(savedFile, oldFile);		
				}

				// Create thumbnails
				UtilityMethodsEmail.createThumbnail(fileName, folderPath, savedFile);

				// Create a message so that devices are notified of the change
				MessagingManager mm = new MessagingManager(localisation);
				if(sId > 0) {
					mm.surveyChange(sd, sId, 0);
				} else {
					mm.resourceChange(sd, oId, fileName);
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


