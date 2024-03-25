package org.smap.sdal.managers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;

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

public class FileManager {
	
	private static Logger log =
			 Logger.getLogger(FileManager.class.getName());
	
	public FileManager() {
	}
	
	
	/*
	 * Get the file at the organisation level
	 */
	public Response getOrganisationFile(
			HttpServletRequest request, 
			HttpServletResponse response, 
			String user, 
			int requestedOrgId, 
			String filename, 
			boolean	settings,
			boolean thumbs) throws IOException, ApplicationException {
		
		Response r = null;
		
		log.info("Get File: " + filename + " for organisation: " + requestedOrgId);

		String basepath = GeneralUtilityMethods.getBasePath(request);
		String filepath = null;
		if(thumbs) {
			filepath = basepath + "/media/organisation/" + requestedOrgId + "/thumbs/" + filename;
		} else {
			filepath = basepath + "/media/organisation/" + requestedOrgId + (settings ? "/settings/" : "/") + filename;
		}
		getFile(response, filepath, filename);
			
		r = Response.ok("").build();
		
		return r;
	}
	
	/*
	 * Get a shared history file
	 */
	public Response getSharedHistoryFile(
			Connection sd,
			HttpServletResponse response, 
			int requestedOrgId, 
			String filename, 
			String surveyIdent,
			int id) throws IOException, ApplicationException, SQLException {
		
		Response r = null;
		
		String filepath = null;
		PreparedStatement pstmt = null;
		
		try {
			StringBuilder sql = new StringBuilder("select file_path from sr_history "
					+ "where o_id = ? "
					+ "and file_name = ? "
					+ "and id = ? ");
			if(surveyIdent != null) {
				sql.append("and survey_ident = ?");
			}
			
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setInt(1, requestedOrgId);
			pstmt.setString(2, filename);
			pstmt.setInt(3, id);
			
			if(surveyIdent != null) {
				pstmt.setString(4, surveyIdent);
			}
			
			ResultSet rs = pstmt.executeQuery();
			log.info("Get path of history file: " + pstmt.toString());
			if(rs.next()) {
				filepath = rs.getString(1);
			}
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		
		getFile(response, filepath, filename);
			
		r = Response.ok("").build();
		
		return r;
	}
	
	/*
	 * Get the latest shared history file
	 */
	public Response getLatestSharedHistoryFile(
			Connection sd,
			HttpServletResponse response, 
			int requestedOrgId, 
			String resourceName, 
			String surveyIdent) throws IOException, ApplicationException, SQLException {
		
		Response r = null;
		
		String filePath = null;
		String fileName = null;
		PreparedStatement pstmt = null;
		
		try {
			StringBuilder sql = new StringBuilder("select file_path, file_name from sr_history "
					+ "where o_id = ? "
					+ "and resource_name = ? ");
			if(surveyIdent != null) {
				sql.append("and survey_ident = ? ");
			}
			sql.append("order by id desc limit 1");
			
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setInt(1, requestedOrgId);
			pstmt.setString(2, resourceName);
			
			if(surveyIdent != null) {
				pstmt.setString(3, surveyIdent);
			}
			
			ResultSet rs = pstmt.executeQuery();
			log.info("Get path of latest history file: " + pstmt.toString());
			if(rs.next()) {
				filePath = rs.getString(1);
				fileName = rs.getString(2);
			}
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		
		getFile(response, filePath, setExtension(resourceName, getExtension(fileName)));
			
		r = Response.ok("").build();
		
		return r;
	}
	
	/*
	 * Get a background report
	 */
	public Response getBackgroundReport(
			Connection sd,
			HttpServletRequest request, 
			HttpServletResponse response, 
			String user, 	
			String filename,
			String reportname
		) throws IOException, ApplicationException {
		
		Response r = null;
		
		log.info("Get Report File: " + filename);

		String basepath = GeneralUtilityMethods.getBasePath(request);
		String filepath = null;
			filepath = basepath + "/reports/" + filename;
		
		getFile(response, filepath, reportname);
			
		r = Response.ok("").build();
		
		return r;
	}
	
	/*
	 * Add the file to the response stream
	 */
	public void getFile(HttpServletResponse response, String filepath, String filename) throws IOException, ApplicationException {
		
		log.info("getfile: " + filepath);
		File f = new File(filepath);
		if(!f.exists()) {
			log.info("Error: File not found: " + f.getAbsolutePath());
			throw new ApplicationException("File not found: " + f.getAbsolutePath());
		}
		response.setContentType(UtilityMethodsEmail.getContentType(filename));
			
		response.addHeader("Content-Disposition", "attachment; filename=" + filename);
		response.setContentLength((int) f.length());
		
		FileInputStream fis = new FileInputStream(f);
		OutputStream responseOutputStream = response.getOutputStream();
		
		try {						
			int bytes;
			while ((bytes = fis.read()) != -1) {
				responseOutputStream.write(bytes);
			}
		} finally {
			try {
				responseOutputStream.flush();
			} catch(Exception e) {
				log.info("Error flushing output stream for file: " + f.getAbsolutePath());
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			try {
				responseOutputStream.close();
			} catch(Exception e) {
				log.info("Error closing output stream for file: " + f.getAbsolutePath());
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			fis.close();
		}
	}

	private String getExtension(String name) {
		String ext = "";
		int idx = name.lastIndexOf('.');
		if( idx >= 0 && name.length() > idx + 1 ) {
			ext = name.substring(idx + 1);
		}
		return ext;
	}
	
	private String setExtension(String name, String ext) {
		
		String newName = name;
		int idx = name.lastIndexOf('.');
		if( idx >= 0 && name.length() > idx + 1 ) {
			newName = name.substring(0, idx + 1) + ext;
		}
		return newName;
	}
}
