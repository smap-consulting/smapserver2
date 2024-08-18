package org.smap.sdal.managers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.commons.fileupload.FileItem;
import org.smap.sdal.Utilities.ApplicationException;

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
 * Sanitise uploaded documents
 * Uses code from: https://github.com/righettod/document-upload-protection
 * Based on OWASP article : https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
 */
public class DocumentUploadManager {
	
	private static Logger log =
			 Logger.getLogger(DocumentUploadManager.class.getName());

	public static ArrayList<String> SHARED_RESOURCE_TYPES = new ArrayList<>(List.of("csv", "excel", 
			"image", "video", "audio"));
	public static ArrayList<String> LOCATION_TYPES = new ArrayList<>(List.of("excel"));
	public static ArrayList<String> SETTINGS_IMPORT_TYPES = new ArrayList<>(List.of("excel", "excel_macro"));
	public static ArrayList<String> DATA_IMPORT_TYPES = new ArrayList<>(List.of("csv", "excel", "compress"));
	public static ArrayList<String> CSS_TYPES = new ArrayList<>(List.of("css"));
	
	private HashMap<String, ArrayList<String>> validExtensions = new HashMap<>();
	
	LogManager lm = new LogManager(); // Application log
	ResourceBundle localisation = null;
	
	public DocumentUploadManager(ResourceBundle l) {
		localisation = l;
		
		validExtensions.put("csv", new ArrayList<>(List.of("csv")));
		validExtensions.put("excel", new ArrayList<>(List.of("xls", "xlsx")));
		validExtensions.put("excel_macro", new ArrayList<>(List.of("xlsm")));
		validExtensions.put("image", new ArrayList<>(List.of("png", "jpg", "jpeg", "gif", "svg")));
		validExtensions.put("video", new ArrayList<>(List.of("mp4", "mpeg")));
		validExtensions.put("audio", new ArrayList<>(List.of("mp3")));
		validExtensions.put("compress", new ArrayList<>(List.of("zip")));
		validExtensions.put("css", new ArrayList<>(List.of("css")));
	}

	public void validateDocument(String fileName, FileItem item, ArrayList<String> validTypes) throws ApplicationException {
				
		/*
		 * Check the extension
		 */
		String docType = null;
		String ext = getExtension(fileName);
		ArrayList<String> extList = new ArrayList<> ();
		for(String type : validTypes) {
			ArrayList<String> v = validExtensions.get(type);
			if(v.contains(ext)) {
				docType = type;
				break;
			} else {
				extList.addAll(v);
			}	
		}
		if(docType == null) {
			String msg = localisation.getString("tu_ift");
			msg = msg.replaceAll("%s1", ext);
			msg = msg.replaceAll("%s2", extList.toString());
			throw new ApplicationException(msg);
		}
		
	}
	
	/*
	 * Get the file extension
	 */
	public String getExtension(String in) {
		String ext = "";
		if(in != null) {
			int idx = in.lastIndexOf('.');
			if(idx > 0) {
				ext = in.substring(idx+1).toLowerCase().trim();
			}
		}
		return ext;
	}

}
