package org.smap.sdal.managers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
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

		String basepath = GeneralUtilityMethods.getBasePath(request);
		String filepath = null;
		if(thumbs) {
			filepath = basepath + "/media/organisation/" + requestedOrgId + "/thumbs/" + filename;
		} else {
			filepath = basepath + "/media/organisation/" + requestedOrgId + (settings ? "/settings/" : "/") + filename;
		}
		return getFileResponse(filepath, filename);
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
			log.fine("Get path of history file: " + pstmt.toString());
			if(rs.next()) {
				filepath = rs.getString(1);
			}
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		
		return getFileResponse(filepath, filename);
	}

	/*
	 * Get the latest shared history file
	 */
	public Response getLatestSharedHistoryFile(
			Connection sd,
			HttpServletResponse response, 
			int requestedOrgId, 
			String resourceName, 
			String surveyIdent,
			String basePath) throws IOException, ApplicationException, SQLException {
		
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
			log.fine("Get path of latest history file: " + pstmt.toString());
			if(rs.next()) {
				filePath = rs.getString(1);
				fileName = rs.getString(2);
			} else {
				fileName = resourceName;
				filePath = basePath + "/media/organisation/" + requestedOrgId + "/" + fileName;
			}
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		
		return getFileResponse(filePath, setExtension(resourceName, getExtension(fileName)));
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
		
		log.fine("Get Report File: " + filename);

		String basepath = GeneralUtilityMethods.getBasePath(request);
		String filepath = null;
			filepath = basepath + "/reports/" + filename;
		
		return getFileResponse(filepath, reportname);
	}
	
	/*
	 * Return a JAX-RS Response that streams the file — use this from JAX-RS endpoints
	 */
	public Response getFileResponse(String filepath, String filename) throws ApplicationException {
		log.fine("getFileResponse: " + filepath);
		File f = new File(filepath);
		if(!f.exists()) {
			log.fine("Error: File not found: " + f.getAbsolutePath());
			throw new ApplicationException("File not found: " + f.getAbsolutePath());
		}
		String contentType = UtilityMethodsEmail.getContentType(filename);
		String escapedFileName = GeneralUtilityMethods.urlEncode(filename != null ? filename : "survey");
		String contentDisposition = "attachment; filename=" + escapedFileName + "; filename*=UTF-8''" + escapedFileName;
		StreamingOutput stream = output -> {
			try (FileInputStream fis = new FileInputStream(f)) {
				byte[] buffer = new byte[8192];
				int bytes;
				while ((bytes = fis.read(buffer)) != -1) {
					output.write(buffer, 0, bytes);
				}
			}
		};
		return Response.ok(stream)
				.type(contentType)
				.header("Content-Disposition", contentDisposition)
				.header("Content-Length", f.length())
				.build();
	}

	/*
	 * Get a file, honouring a byte range request so that a download interrupted by a poor
	 * connection can be resumed rather than started again.  Without a range header this
	 * behaves the same as getFileResponse().
	 */
	public Response getRangeFileResponse(HttpServletRequest request, String filepath, String filename,
			String etag) throws ApplicationException {

		File f = new File(filepath);
		if(!f.exists()) {
			log.info("Error: File not found: " + f.getAbsolutePath());
			throw new ApplicationException("File not found: " + f.getAbsolutePath());
		}

		long length = f.length();
		String contentType = UtilityMethodsEmail.getContentType(filename);
		String escapedFileName = GeneralUtilityMethods.urlEncode(filename != null ? filename : "file");
		String contentDisposition = "attachment; filename=" + escapedFileName + "; filename*=UTF-8''" + escapedFileName;

		String range = request.getHeader("Range");
		long start = 0;
		long end = length - 1;
		boolean partial = false;

		if(range != null && range.startsWith("bytes=")) {
			String spec = range.substring("bytes=".length()).trim();
			if(spec.indexOf(',') >= 0) {
				spec = spec.substring(0, spec.indexOf(','));	// Only the first range is served
			}
			int dash = spec.indexOf('-');
			if(dash >= 0) {
				try {
					String startStr = spec.substring(0, dash).trim();
					String endStr = spec.substring(dash + 1).trim();
					if(startStr.length() > 0) {
						start = Long.parseLong(startStr);
						if(endStr.length() > 0) {
							end = Long.parseLong(endStr);
						}
					} else if(endStr.length() > 0) {
						// Suffix range, the last n bytes
						long suffix = Long.parseLong(endStr);
						start = suffix >= length ? 0 : length - suffix;
					} else {
						throw new NumberFormatException("Empty range");
					}
					partial = true;
				} catch (NumberFormatException e) {
					partial = false;		// Ignore a malformed range and send the whole file
					start = 0;
					end = length - 1;
				}
			}
		}

		if(partial) {
			if(end > length - 1) {
				end = length - 1;
			}
			if(start > end || start >= length) {
				return Response.status(416)		// Requested range not satisfiable
						.header("Content-Range", "bytes */" + length)
						.header("Accept-Ranges", "bytes")
						.build();
			}
		}

		final long from = start;
		final long count = end - start + 1;

		StreamingOutput rangeStream = output -> {
			try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
				raf.seek(from);
				byte[] buffer = new byte[65536];
				long remaining = count;
				while(remaining > 0) {
					int toRead = (int) Math.min(buffer.length, remaining);
					int bytes = raf.read(buffer, 0, toRead);
					if(bytes == -1) {
						break;
					}
					output.write(buffer, 0, bytes);
					remaining -= bytes;
				}
			}
		};

		Response.ResponseBuilder builder = partial
				? Response.status(206).header("Content-Range", "bytes " + start + "-" + end + "/" + length)
				: Response.ok();

		builder.entity(rangeStream)
				.type(contentType)
				.header("Content-Disposition", contentDisposition)
				.header("Accept-Ranges", "bytes")
				.header("Content-Length", count);

		if(etag != null) {
			builder.header("ETag", "\"" + etag + "\"");
		}

		return builder.build();
	}

	/*
	 * Add the file to the response stream
	 */
	public void getFile(HttpServletResponse response, String filepath, String filename) throws IOException, ApplicationException {
		
		log.fine("getfile: " + filepath);
		File f = new File(filepath);
		if(!f.exists()) {
			log.fine("Error: File not found: " + f.getAbsolutePath());
			throw new ApplicationException("File not found: " + f.getAbsolutePath());
		}
		response.setContentType(UtilityMethodsEmail.getContentType(filename));
		GeneralUtilityMethods.setFilenameInResponse(filename, response);	
		response.setContentLength((int) f.length());
		
		FileInputStream fis = new FileInputStream(f);
		OutputStream responseOutputStream = response.getOutputStream();
		
		try {
			byte[] buffer = new byte[8192];
			int bytes;
			while ((bytes = fis.read(buffer)) != -1) {
				responseOutputStream.write(buffer, 0, bytes);
			}
		} finally {
			try {
				responseOutputStream.flush();
			} catch(Exception e) {
				log.fine("Error flushing output stream for file: " + f.getAbsolutePath());
			}
			try {
				responseOutputStream.close();
			} catch(Exception e) {
				log.fine("Error closing output stream for file: " + f.getAbsolutePath());
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
