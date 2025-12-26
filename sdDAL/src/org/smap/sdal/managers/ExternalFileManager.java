package org.smap.sdal.managers;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.model.CustomUserReference;
import org.smap.sdal.model.SqlFrag;

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
			// log.info("Linker changed: " + pstmt.toString());
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
			String filename, String logicalFilePath, String userName, String tz, String basePath,
			CustomUserReference cur) throws Exception {

		boolean regenerate = false;
		
		// log.info("createLinkedFile: " + filename);

		try {
			String physicalFilePath = getLinkedPhysicalFilePath(sd, logicalFilePath);
			File currentPhysicalFile = new File(physicalFilePath + ".csv"); // file path does not include the extension because getshape.sh adds it
			SurveyTableManager stm = new SurveyTableManager(sd, cRel, localisation, oId, sId, filename, userName);  

			regenerate = stm.testForRegenerateFile(sd, cRel,  sId, logicalFilePath, currentPhysicalFile);
			if(regenerate) {
				String newFilePath = getLinkedNewFilePath(sd, logicalFilePath);
				if(stm.generateCsvFile(cRel, new File(newFilePath + ".csv"), sId, userName, basePath, cur)) {
					updateCurrentPhysicalFile(sd, newFilePath, currentPhysicalFile, sId);
				}
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			lm.writeLog(sd, sId, userName, LogManager.ERROR, "Creating CSV file: " + e.getMessage(), 0, null);
		} 

		return regenerate;
	}


	/*
	 * Get the path  to the current linked file
	 */
	public String getLinkedDirPath(String basepath, String sIdent, String uIdent, boolean customUserFile) {
		StringBuilder path = new StringBuilder(basepath
				+ File.separator
				+ "media" 
				+ File.separator);	
		path.append(sIdent).append(File.separator);
		
		// Check to see if the path is specific to the calling user
		if(customUserFile) {
			path.append(uIdent).append(File.separator);
		}
		
		return path.toString();
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
				physicalFilePath = logicalFilePath + "__" + rs.getString(1);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
		// log.info("%%%%%: Returning current physical path: " + physicalFilePath + ".csv");
		return physicalFilePath;
	}
	
	/*
	 * Get a new File for linked csv data
	 */
	public String getLinkedNewFilePath(Connection sd, String logicalFilePath) throws SQLException {
		
		String newPath = null;
		String sql = "Select current_id from linked_files where logical_path = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  logicalFilePath);
			ResultSet rs = pstmt.executeQuery();
			
			String suffix = "1";
			if(rs.next()) {
				suffix = String.valueOf(rs.getInt(1) + 1);
			}
			newPath = logicalFilePath + "__" + suffix;
			
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
		
		// log.info("%%%%%: Getting a new file path: " + newPath + ".csv");
		
		return newPath;
	}
	
	/*
	 * Update the identifier for the current file
	 */
	public void updateCurrentPhysicalFile(Connection sd, String physicalFilePath, File currentPhysicalFile, int sId) throws SQLException {
		
		String [] physicalComponents = physicalFilePath.split("__");
		if(physicalComponents.length > 1) {
			String logicalPath = physicalComponents[0];
			String newId  = physicalComponents[1];
			
			String sql = "update linked_files set current_id = ? where logical_path = ?";
			PreparedStatement pstmt = null;
			
			String sqlDel = "insert into linked_files_old (file, deleted_time) values(?, now())";
			PreparedStatement pstmtDel = null;
		
			String sqlInsert = "insert into linked_files (s_id, logical_path, current_id) values(?, ?, ?)";
			PreparedStatement pstmtInsert = null;
			
			try {
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1,  Integer.valueOf(newId));
				pstmt.setString(2,  logicalPath);
				int count = pstmt.executeUpdate();
				
				if(count == 0) {  // create new entry
					pstmtInsert = sd.prepareStatement(sqlInsert);
					pstmtInsert.setInt(1, sId);
					pstmtInsert.setString(2, logicalPath);
					pstmtInsert.setInt(3, Integer.valueOf(newId));
					pstmtInsert.executeUpdate();
				}
				// log.info("%%%%%: Update current physical path to: " + physicalFilePath + ".csv");
				
				pstmtDel = sd.prepareStatement(sqlDel);
				pstmtDel.setString(1,  currentPhysicalFile.getAbsolutePath());
				pstmtDel.executeUpdate();
				
				// log.info("%%%%%: Marking file for deleting: " + currentPhysicalFile.getAbsolutePath());
			
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
				throw e;
			} finally {
				if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
				if(pstmtInsert != null) try {pstmtInsert.close();}catch(Exception e) {}
				if(pstmtDel != null) try {pstmtDel.close();}catch(Exception e) {}
			}
		}
	}
}
