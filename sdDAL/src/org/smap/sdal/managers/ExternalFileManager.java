package org.smap.sdal.managers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Pulldata;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.SqlFrag;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * 
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 * 
 ******************************************************************************/

/*
 * Manage creation of files
 */
public class ExternalFileManager {

	private static Logger log = Logger.getLogger(ExternalFileManager.class.getName());
	
	private static ResourceBundle localisation = null;
	LogManager lm = new LogManager(); // Application log

	public ExternalFileManager(ResourceBundle l) {
		localisation = l;
	}
	
	/*
	 * Class to return SQL
	 */
	class SqlDef {
		String sql;
		ArrayList<String> colNames;
		boolean hasRbacFilter = false;
		ArrayList<SqlFrag> rfArray = null;
	}

	/*
	 * Call this method when a linker survey, that is a survey that links to another
	 * survey changes. This will result in regenerate being called next time the
	 * survey is downloaded
	 */
	public void linkerChanged(Connection sd, int sId) throws SQLException {
		String sql = "delete from linked_forms where linker_s_id = ?;";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			log.info("Linker changed: " + pstmt.toString());
			pstmt.executeUpdate();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Linker changed", e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
				}
			}
			;
		}
	}

	/*
	 * Create a linked file
	 */
	public boolean createLinkedFile(Connection sd, Connection cRel, int oId, int sId, // The survey that contains the manifest item
			String filename, String logicalFilePath, String userName, String tz) throws Exception {

		boolean regenerate = false;
		
		log.info("createLinkedFile: " + filename);

		try {
			String physicalFilePath = getLinkedPhysicalFilePath(sd, logicalFilePath);
			File physicalFile = new File(physicalFilePath + ".csv"); // file path does not include the extension because getshape.sh adds it
			SurveyTableManager stm = new SurveyTableManager(sd, cRel, localisation, oId, sId, filename, userName);  

			regenerate = stm.testForRegenerateFile(sd, cRel,  sId, logicalFilePath, physicalFile);
			if(regenerate) {
				stm.regenerateCsvFile(cRel, physicalFile, sId, userName, logicalFilePath);
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			lm.writeLog(sd, sId, userName, LogManager.ERROR, "Creating CSV file: " + e.getMessage(), 0);
		} 

		return regenerate;
	}


	/*
	 * Get the path  to the current linked file
	 */
	public String getLinkedDirPath(String basepath, String sIdent) {
		return basepath
				+ File.separator
				+ "media" 
				+ File.separator 
				+ sIdent
				+ File.separator;
	
		
	}
	
	/*
	 * Get the logical path  to the current linked file
	 */
	public String getLinkedLogicalFilePath(String dirPath, String filename) {
		
		return dirPath + filename;
		
	}
	
	/*
	 * Get the physical path  to the current linked file
	 */
	public String getLinkedPhysicalFilePath(Connection sd, String logicalFilePath) throws SQLException {
		
		String physicalFilePath = logicalFilePath;	// Default is old approach of using logical file path
		String sql = "Select current_id from linked_files where logical_path = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  logicalFilePath);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				physicalFilePath = rs.getString(1);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
		return physicalFilePath;
	}
}
