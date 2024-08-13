package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.UniqueKey;

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

	LogManager lm = new LogManager(); // Application log
	ResourceBundle localisation = null;
	
	public DocumentUploadManager(ResourceBundle l) {
		localisation = l;
	}

	public void validateDocument(String fileType) {
		
	}

}
