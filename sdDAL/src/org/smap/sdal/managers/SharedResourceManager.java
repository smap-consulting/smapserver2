package org.smap.sdal.managers;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.CustomUserReference;
import org.smap.sdal.model.MediaItem;
import org.smap.sdal.model.MediaResponse;
import org.smap.sdal.model.Message;
import org.smap.sdal.model.SharedHistoryItem;

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
	
	public static long MAX_FILE_SIZE = 25000000;	// 25 Million Bytes
	
	Authorise aEnum = new Authorise(null, Authorise.ENUM);
	Authorise aOrg = new Authorise(null, Authorise.ORG);
	Authorise aAdmin = null;
	
	ResourceBundle localisation;
	String tz;
	
	public SharedResourceManager(ResourceBundle localisation, String tz) {
		this.localisation = localisation;
		this.tz = tz;
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ENUM);
		aAdmin = new Authorise(authorisations, null);	
	}
	
	/*
	 * Add a resource file
	 */
	public Response add(Connection sd, 
			String sIdent,
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
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		if(sIdent != null) {
			mediaInfo.setFolder(basePath, 0, sIdent, sd);
		} else {			
			mediaInfo.setFolder(basePath, user, oId, false);  // Upload to organisations folder				 
		}

		String folderPath = mediaInfo.getPath();

		if(folderPath != null) {		
			
			if(fileItem.getSize() > MAX_FILE_SIZE) {		// Check the size of the file
				
				responseCode = "error";
				String msg = localisation.getString("sr_tl");
				msg = msg.replace("%s1", String.format("%,d", fileItem.getSize()));
				msg = msg.replace("%s2", String.format("%,d", MAX_FILE_SIZE));
				responseMsg = new StringBuilder(msg);
				
			} else if(resourceName == null || resourceName.trim().length() == 0 
					|| resourceName.contains(".")
					|| resourceName.contains("/")) {		// Validate the resource name
				
				responseCode = "error";
				responseMsg = new StringBuilder(localisation.getString("mf_in") + " " + resourceName);
				
			} else {	// Save the new file
				
				// Get the file type from the extension of the uploaded file
				String uploadedFileName = fileItem.getName();
				String contentType = UtilityMethodsEmail.getContentType(uploadedFileName);
				
				// Set the extension of the output file
				String extension = UtilityMethodsEmail.getExtension(uploadedFileName);
				String filetype = "";
					
				if(extension.equals("xlsx") || extension.equals("xls")) {
					filetype = extension;
					extension = "csv";
				} 
				
				// Change the name of the resource to that specified by the user but keep the extension
				String resourceFileName = resourceName + "." + extension;
				String filePath = folderPath + "/" + resourceFileName;
				File savedFile = new File(filePath);	
				
				if(action.equals("add") && savedFile.exists()) {				// Make sure file does not already exist if adding
					
					responseCode = "error";
					String msg = localisation.getString("sr_ae");
					msg = msg.replace("%s1", resourceName + "." + extension);
					responseMsg = new StringBuilder(msg);
					
				} else {
					
					/*
					 * Save the file
					 */
					if(filetype.equals("xlsx") || filetype.equals("xls")) {
						
						// Write to a CSV file
						XLSXSharedResourceManager xlsx = new XLSXSharedResourceManager();
						xlsx.writeToCSV(filetype, fileItem.getInputStream(), savedFile, localisation, tz);
						
					} else {
						// no conversion required
						fileItem.write(savedFile);  
					}
					log.info("Uploaded file written to: " + savedFile.getAbsolutePath());
					
					if(savedFile.exists()) {
						
						if(contentType.equals("text/csv") || resourceFileName.endsWith(".csv")) {				
							// Upload any CSV data into a table, also checks maximum number of columns
							CsvTableManager csvMgr = new CsvTableManager(sd, localisation, oId, sId, resourceFileName);
							csvMgr.updateTable(savedFile);		
						} else {
							// Create thumbnails
							UtilityMethodsEmail.createThumbnail(resourceFileName, folderPath, savedFile);
						}
		
						// Create a message so that devices are notified of the change
						MessagingManager mm = new MessagingManager(localisation);
						if(sId > 0) {
							mm.surveyChange(sd, sId, 0);
						} else {
							mm.resourceChange(sd, oId, resourceName);
						}
						
						writeToHistory(sd, fileItem, folderPath, resourceFileName, uploadedFileName,
								oId, sIdent, user);	// Record all changes to the shared resource
						
						/*
						 * Backup file to S3
						 */
						GeneralUtilityMethods.sendToS3(sd, savedFile.getAbsolutePath(), oId, true);
						
					} else {
						responseCode = "error";
						responseMsg = new StringBuilder("Failed to save shared resource file: " + resourceName);
					}
				}
			}

		} else {
			log.log(Level.SEVERE, "Media folder not found");
			responseCode = "error";
			responseMsg = new StringBuilder("Media folder not found");
		}
		
		/*
		 * Log the add
		 */
		if(responseCode.equals("success")) {
			if(sIdent == null) {
				String msg = localisation.getString("sr_add");
				msg = msg.replace("%s1", resourceName);
				lm.writeLogOrganisation(sd, oId, user, action.equals("add") ? LogManager.CREATE : LogManager.REPLACE, msg, 0);
			} else {
				String msg = localisation.getString("sr_s_add");
				msg = msg.replace("%s1", resourceName);
				lm.writeLog(sd, sId, user, action.equals("add") ? LogManager.CREATE : LogManager.REPLACE, msg, 0, "");
			}
		}
		
		return Response.ok(gson.toJson(new Message(responseCode, responseMsg.toString(), resourceName))).build();
	
	}
	
	/*
	 * Delete a shared resource file
	 */
	public void delete(Connection sd, 
			String sIdent, 
			int sId,
			int oId, 
			String basePath, 
			String user, 
			String fileName) throws Exception {
		

		MediaInfo mediaInfo = new MediaInfo();		
		if(sIdent != null) {
			mediaInfo.setFolder(basePath, 0, sIdent, sd);
		} else {			
			mediaInfo.setFolder(basePath, user, oId, false);  // Upload to organisations folder				 
		}
		String folderPath = mediaInfo.getPath();
		String filePath = folderPath + File.separatorChar + fileName;
		File savedFile = new File(filePath);
		String extension = UtilityMethodsEmail.getExtension(fileName);
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtDel = null;
		try {
			if(folderPath != null) {
				
				// 1. Delete File
				if(savedFile.exists()) {
					savedFile.delete();
				}
				
				// 1.1 For CSV files delete table data also legacy .old copies (.old files are no longer created)
				if(extension.equals("csv")) {
					File oldFile = new File(filePath + "." + "old");
					if(oldFile.exists()) {
						oldFile.delete();
					}
					
					// Delete the CSV table data
					CsvTableManager tm = new CsvTableManager(sd, localisation);
				    tm.delete(oId, sId, fileName);		
				}
				
				// 1.2 Delete thumbnails
				String thumbsPath = folderPath + File.separatorChar + "thumbs";
				String fileBase = null;
				int idx = fileName.lastIndexOf('.');
				if(idx >= 0) {
					fileBase = fileName.substring(0, idx);
					
					// Delete matching thumbnails
					File thumbs = new File(thumbsPath);
					if(thumbs != null) {
						for(File thumb : thumbs.listFiles()) {
							if(thumb.getName().startsWith(fileBase)) {
								thumb.delete();
							}
						}
					}
				}
				
				// 2. Delete History
				StringBuilder sql = new StringBuilder("select file_path from "
						+ "sr_history "
						+ "where o_id = ? "
						+ "and resource_name = ?");
				
				StringBuilder sqlDel = new StringBuilder("delete from sr_history "
						+ "where o_id = ? "
						+ "and resource_name = ?");
				
				if(sIdent != null) {
					sql.append(" and survey_ident = ?");
					sqlDel.append(" and survey_ident = ?");
				}
				pstmt = sd.prepareStatement(sql.toString());
				pstmt.setInt(1, oId);
				pstmt.setString(2, fileName);
				
				pstmtDel = sd.prepareStatement(sqlDel.toString());
				pstmtDel.setInt(1, oId);
				pstmtDel.setString(2, fileName);			

				if(sIdent != null) {
					pstmt.setString(3,  sIdent);
					pstmtDel.setString(3,  sIdent);
				}
				
				// 2.1 Delete the stored history files	
				ResultSet rs = pstmt.executeQuery();
				if (rs.next()) {
					File history_file = new File(rs.getString(1));
					File history_dir = history_file.getParentFile();
					if(history_dir.exists()) {
						for(File h : history_dir.listFiles()) {
							h.delete();
						}
						history_dir.delete();
					}
				}
				
				//2.2 Delete the database table
				pstmtDel.executeUpdate();
				
				/*
				 * Log the delete
				 */
				if(sIdent == null) {
					String msg = localisation.getString("sr_del");
					msg = msg.replace("%s1", fileName);
					lm.writeLogOrganisation(sd, oId, user, LogManager.DELETE, msg, 0);
				} else {
					String msg = localisation.getString("sr_s_del");
					msg = msg.replace("%s1", fileName);
					lm.writeLog(sd, sId, user, LogManager.DELETE, msg, 0, "");
				}
				
			} else {
				throw new ApplicationException("Media folder not found");
			}
		} finally {
			try {if ( pstmt != null ) { pstmt.close(); }} catch (Exception e) {}
			try {if ( pstmtDel != null ) { pstmtDel.close(); }} catch (Exception e) {}
		}
	}
	
	/*
	 * Save a change history for the shared resource
	 */
	private void writeToHistory(Connection sd, 
			FileItem fileItem, 
			String folderPath, 
			String resourceFileName, 
			String uploadedFileName,
			int oId,
			String sIdent, 
			String user) throws Exception {
		
		// Create directories
		File archivePath = new File(folderPath + "/history/" + resourceFileName);
		if(!archivePath.exists()) {
			if(!archivePath.mkdirs()) {
				throw new ApplicationException("Failed to create shared resource archive folder");
			}
		}
		
		// Write Archived file
		File archiveFile = new File(archivePath.getAbsolutePath() + "/" + uploadedFileName + GeneralUtilityMethods.getUTCDateTimeSuffix());		
		fileItem.write(archiveFile);
		
		/*
		 * Record history in the database
		 */
		PreparedStatement pstmt = null;
		try {
			String sql = "insert into sr_history (o_id, survey_ident, resource_name, file_name, file_path, user_ident, uploaded_ts) "
					+ "values(?, ?, ?, ?, ?, ?, now())";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2,  sIdent);
			pstmt.setString(3,  resourceFileName);
			pstmt.setString(4, uploadedFileName);
			pstmt.setString(5,  archiveFile.getAbsolutePath());
			pstmt.setString(6,  user);
			log.info("Save shared resource archive record: " + pstmt.toString());
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch (Exception e) {}}
		}
		
		/*
		 * Only retain the last 10 shared resource files
		 */
		PreparedStatement pstmtList = null;
		PreparedStatement pstmtDel = null;
		try {
			String sql = "select count(*) from sr_history "
					+ "where o_id = ? and resource_name = ? "
					+ "and file_path is not null";
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, resourceFileName);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				int count = rs.getInt(1);
				if(count > 10) {
					
					String sqlDel = "update sr_history "
							+ "set file_path = null "
							+ "where id = ?";
					pstmtDel = sd.prepareStatement(sqlDel);
					
					sql = "select id, file_path from sr_history "
							+ "where o_id = ? "
							+ "and resource_name = ? "
							+ "and file_path is not null "
							+ "order by id asc limit ?";
					pstmtList = sd.prepareStatement(sql);
					pstmtList.setInt(1, oId);
					pstmtList.setString(2, resourceFileName);
					pstmtList.setInt(3, count - 10);
					log.info("Get excess files: " + pstmtList.toString());
					rs = pstmtList.executeQuery();
					while(rs.next()) {
						int id = rs.getInt(1);
						String filePath = rs.getString(2);
						
						File f = new File(filePath);
						if(f.exists()) {
							log.info("Deleting resource history file: " + f.getName());
							f.delete();
						}
						
						pstmtDel.setInt(1, id);
						pstmtDel.executeUpdate();
					}
				}
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch (Exception e) {}}
			if(pstmtList != null) {try{pstmtList.close();} catch (Exception e) {}}
			if(pstmtDel != null) {try{pstmtDel.close();} catch (Exception e) {}}
		}
		
	}
	
	/*
	 * Get the history for a shared resource
	 */
	public ArrayList<SharedHistoryItem> getHistory(Connection sd, 
			String sIdent, 
			int oId, 
			String user, 
			String resourceName,
			String tz) throws Exception {
		
		ArrayList<SharedHistoryItem> items = new ArrayList<>();
		
		
		PreparedStatement pstmt = null;
		StringBuilder sql = new StringBuilder("select "
				+ "id, file_name, user_ident, file_path, "
				+ "to_char(timezone(?, uploaded_ts), 'YYYY-MM-DD HH24:MI:SS') as uploaded_ts "
				+ "from sr_history "
				+ "where o_id = ? "
				+ "and resource_name = ?");
		if(sIdent != null) {
			sql.append(" and survey_ident = ?");
		}
		sql.append(" order by id desc");
				
		try {
			int idx = 1;
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setString(idx++,  tz);
			pstmt.setInt(idx++,  oId);
			pstmt.setString(idx++, resourceName);
			if(sIdent != null) {
				pstmt.setString(idx++,  sIdent);
			}
			
			log.info("Get shared history: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				SharedHistoryItem item = new SharedHistoryItem();
				item.file_name = rs.getString("file_name");
				item.user_ident = rs.getString("user_ident");
				item.uploaded = rs.getString("uploaded_ts");
				
				int id = rs.getInt("id");
				String filePath = rs.getString("file_path");
				if(filePath == null) {
					item.url = null;
				} else {
					String escapedFileName = GeneralUtilityMethods.urlEncode(item.file_name);
					item.url = "/surveyKPI/file/" + escapedFileName + "/history/" + id;
					if(sIdent != null) {
						item.url += "?sIdent=" + sIdent;
					}
				}
				items.add(item);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
	
		return items;
	
	}
	
	/*
	 * Get a shared resource file that has been loaded for a specific survey
	 */
	public Response getSurveyFile(HttpServletRequest request, 
			HttpServletResponse response,
			String filename, 
			int sId, 
			boolean thumbs,
			boolean linked) {
		
		Response r = null;
		String connectionString = "Get Survey File";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		String user = request.getRemoteUser();
		// If the user is still null try setting it using an authentication token
				if(user == null) {
					try {
						user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					}
				}
		aEnum.isAuthorised(sd, user);
		aEnum.isValidSurvey(sd, user, sId, false, superUser);
		// End Authorisation 
		
		try {
			
			ExternalFileManager efm = new ExternalFileManager(null);
			String basepath = GeneralUtilityMethods.getBasePath(request);
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			
			log.info("Getting file: " + filename + " : " + linked);
			String filepath = null;
			if(linked) {
				int idx = filename.indexOf(".csv");
				String baseFileName = filename;
				if(idx >= 0) {
					baseFileName = filename.substring(0, idx);		// External file management routines assume no extension
				}
				String linkedSurveyIdent = baseFileName.substring("linked_".length());
				CustomUserReference cur = GeneralUtilityMethods.hasCustomUserReferenceData(sd, linkedSurveyIdent);
				filepath = efm.getLinkedPhysicalFilePath(sd, 
						efm.getLinkedLogicalFilePath(efm.getLinkedDirPath(basepath, sIdent, user, cur.needCustomFile()), baseFileName)) 
						+ ".csv";
				log.info("%%%%%: Referencing: " + filepath);
			} else {
				if(thumbs) {
					filepath = basepath + "/media/" + sIdent+ "/thumbs/" + filename;
				} else {
					filepath = basepath + "/media/" + sIdent+ "/" + filename;
				}
			}
			
			log.info("File path: " + filepath);
			FileManager fm = new FileManager();
			fm.getFile(response, filepath, filename);
			
			r = Response.ok("").build();
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Error getting file", e);
			r = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}
	
	/*
	 * Get a shared resource file at the organisation level
	 */
	public Response getOrganisationFile(
			HttpServletRequest request, 
			HttpServletResponse response, 
			String user,
			int requestedOrgId, 
			String filename, 
			boolean settings,
			boolean isTemporaryUser,
			boolean thumbs) {
		
		int oId = 0;
		Response r = null;
		String connectionString = "Get Organisation File";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		// If the passed in user was null try setting it to the authenticated user
		if(user == null) {
			user = request.getRemoteUser();
		}
		// If the user is still null try setting it using an authentication token
		if(user == null) {
			try {
				user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		
		if (isTemporaryUser) {
			aAdmin.isValidTemporaryUser(sd, user);
		} 
		
		aAdmin.isAuthorised(sd, user);		
		try {		
			oId = GeneralUtilityMethods.getOrganisationId(sd, user);
		} catch(Exception e) {
			// ignore error
		}
		if(requestedOrgId > 0 && requestedOrgId != oId) {
			aOrg.isAuthorised(sd, user);	// Must be org admin to work on another organisations data
			oId = requestedOrgId;
		}
		// End Authorisation 
		
		
		log.info("Get File: " + filename + " for organisation: " + oId);
		try {
			
			FileManager fm = new FileManager();
			r = fm.getOrganisationFile(request, response, user, oId, 
					filename, settings, thumbs);
			
		}  catch (Exception e) {
			log.info("Error: Failed to get file:" + e.getMessage());
			r = Response.status(Status.NOT_FOUND).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}
	
	/*
	 * Return the shared resource files
	 */
	public Response getSharedMedia(HttpServletRequest request, int sId, boolean getall, boolean forDevice) {
		Response response = null;
		String user = request.getRemoteUser();
		boolean superUser = false;

		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		Authorise auth = new Authorise(authorisations, null);
		
		String connectionString = "surveyKPI-UploadFiles-getMedia";
		/*
		 * Authorise
		 *  If survey ident is passed then check user access to survey
		 *  Else provide access to the media for the organisation
		 */
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
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
				mediaInfo.setFolder(basePath, user, oId, false);				 
			}

			log.info("Media query on: " + mediaInfo.getPath());

			MediaResponse mResponse = new MediaResponse();
			mResponse.files = mediaInfo.get(sId, null, forDevice);	
			
			if(sId > 0 && getall) {
				log.info("Media getting files for survey: " + sId);
				// Get a hashmap of the names to exclude
				HashMap<String, String> exclude = new HashMap<> ();
				for(MediaItem mi : mResponse.files) {
					exclude.put(mi.name, mi.name);
				}
				// Add the organisation level media
				mediaInfo.setFolder(basePath, user, oId, false);
				mResponse.files.addAll(mediaInfo.get(0, exclude, forDevice));
				
			}
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(mResponse);
			response = Response.ok(resp).build();		

		}  catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().build();
		} finally {

			if (pstmt != null) { try {pstmt.close();} catch (SQLException e) {}}

			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;	
	}
	
	/*
	 * Upload a shared resource media file
	 */
	public Response uploadSharedMedia(HttpServletRequest request) {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		Authorise auth = new Authorise(authorisations, null);
		
		Response response = null;
		String connectionString = "SurveyKPI-uploadSharedResourceFile";
		
		log.info("upload shared resource file -----------------------");
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();
		String resourceName = null;
		String action = "add";	// By default add
		int surveyId = 0;
		String surveyIdent = null;
		FileItem fileItem = null;
		String fileName = null;
		String user = request.getRemoteUser();
		String tz = "UTC";

		fileItemFactory.setSizeThreshold((int) SharedResourceManager.MAX_FILE_SIZE); 
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		Connection sd = SDDataSource.getConnection(connectionString); 
		Connection cResults = ResultsDataSource.getConnection(connectionString);

		PreparedStatement pstmtChangeLog = null;
		PreparedStatement pstmtUpdateChangeLog = null;
		
		try {
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);	
			
			boolean superUser = false;
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			
			// Authorise the user
			auth.isAuthorised(sd, request.getRemoteUser());
			
			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();
			while(itr.hasNext()) {
				
				FileItem item = (FileItem) itr.next();

				if(item.isFormField()) {
					if(item.getFieldName().equals("itemName")) {
						resourceName = item.getString("utf-8");
						if(resourceName != null) {
							resourceName = resourceName.trim();
						}
						log.info("Resource Name: " + resourceName);	
						
					} else if(item.getFieldName().equals("surveyId")) {
						try {
							// Authorise access to existing survey
							surveyId = Integer.parseInt(item.getString());
							if(surveyId > 0) {
								auth.isValidSurvey(sd, request.getRemoteUser(), surveyId, false, superUser);	// Check the user has access to the survey
							}
							surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, surveyId);
						} catch (Exception e) {
							
						}
						log.info("Upload to survey: " + surveyId);
						
					} else if(item.getFieldName().equals("action")) {						
						action = item.getString();
						log.info("Action: " + action);
						
					} else {
						log.info("Unknown field name = " + item.getFieldName() + ", Value = " + item.getString());
					}
				} else {					
					if(item.getName().trim().length() > 0) {
						fileName = item.getName().trim();
						fileItem = item;
					} else {
						log.info("No name specified for item in upload file");
					}
				}
			} 
			
			/*
			 * Validate the upload
			 */
			DocumentUploadManager dum = new DocumentUploadManager(localisation);
			dum.validateDocument(fileName, fileItem, DocumentUploadManager.SHARED_RESOURCE_TYPES);
			
			/*
			 * Default the resource name to the file name if it was not specified
			 */
			if(resourceName == null) {
				int idx = fileName.lastIndexOf('.');
				if (idx > 0) {
					resourceName = fileName.substring(0, idx);
				}
			}
			
			/*
			 * Load the resource
			 */
			SharedResourceManager srm = new SharedResourceManager(localisation, tz);			
			String basePath = GeneralUtilityMethods.getBasePath(request);			
			response = srm.add(sd, surveyIdent, surveyId, oId, basePath, user, resourceName, fileItem, action);
			
		} catch(AuthorisationException ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			throw ex;
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			if(pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (Exception e) {}
			if(pstmtUpdateChangeLog != null) try {pstmtUpdateChangeLog.close();} catch (Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}
		
		return response;
	}
}


