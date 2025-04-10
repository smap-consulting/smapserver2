package org.smap.sdal.managers;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.ServerData;

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
 * Manage the server table
 */
public class ServerManager {

	private static Logger log =
			Logger.getLogger(ServerManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	ResourceBundle localisation;

	public ServerData getServer(Connection sd, ResourceBundle l) {

		localisation = l;
		
		String sql = "select email_type,"
				+ "aws_region,"
				+ "smtp_host,"
				+ "email_domain,"
				+ "email_user,"
				+ "email_password,"
				+ "email_port,"
				+ "version,"
				+ "mapbox_default,"
				+ "google_key,"
				+ "maptiler_key,"
				+ "vonage_application_id,"
				+ "vonage_webhook_secret,"
				+ "sms_url,"
				+ "max_rate,"
				+ "password_strength,"
				+ "keep_erased_days,"
				+ "css,"
				+ "email_type "
				+ "from server;";
		PreparedStatement pstmt = null;
		ServerData data = new ServerData();

		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				data.email_type = rs.getString("email_type");
				data.aws_region = rs.getString("aws_region");
				data.smtp_host = rs.getString("smtp_host");
				data.email_domain = rs.getString("email_domain");
				data.email_user = rs.getString("email_user");
				data.email_password = rs.getString("email_password");
				data.email_port = rs.getInt("email_port");
				data.version = rs.getString("version");
				data.mapbox_default = rs.getString("mapbox_default");
				data.google_key = rs.getString("google_key");
				data.maptiler_key = rs.getString("maptiler_key");
				data.vonage_application_id = rs.getString("vonage_application_id");
				data.vonage_webhook_secret = rs.getString("vonage_webhook_secret");
				data.sms_url = rs.getString("sms_url");
				data.ratelimit = rs.getInt("max_rate");
				data.password_strength = rs.getDouble("password_strength");
				data.keep_erased_days = rs.getInt("keep_erased_days");
				data.css = rs.getString("css");
				data.email_type = rs.getString("email_type");
			}

		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}

		}

		return data;
	}

	/*
	 * Hard delete a survey
	 */
	public void deleteSurvey(
			Connection sd, 
			Connection rel, 
			String user,
			int projectId,
			int sId, 
			String surveyIdent,
			String surveyDisplayName,
			String basePath,
			boolean delData,
			String delTables) throws Exception {

		/*
		 * Get the tables associated with this survey
		 */
		String sql = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtCompound = null;
		ResultSet resultSet = null;
		
		SurveyManager surveyManager = new SurveyManager(localisation, "UTC");

		try {

			boolean nonEmptyDataTables = false;

			if(delTables != null && delTables.equals("yes")) {

				lm.writeLog(sd, sId, user, LogManager.ERASE, "Delete survey " + surveyDisplayName + " and its results", 0, null);
				log.info("userevent: " + user + " : hard delete survey : " + surveyDisplayName);

				// Get any tables that are shared with other surveys
				HashMap<String, ArrayList<String>> sharedTables = surveyManager.getSharedTables(sd, sId);
				
				sql = "select f.table_name from form f " +
						"where f.s_id = ? " +
						"order by f.table_name;";						

				pstmt = sd.prepareStatement(sql);	
				pstmt.setInt(1, sId);
				log.info("Get tables for deletion: " + pstmt.toString());
				resultSet = pstmt.executeQuery();

				while (resultSet.next() && (delData || !nonEmptyDataTables)) {		

					String tableName = resultSet.getString(1);
					
					ArrayList<String> surveys = sharedTables.get(tableName);
					if(surveys != null && surveys.size() > 0) {
						log.info("Table " + tableName + " not erased as it is used by " + surveys.toString());
						lm.writeLog(sd, sId, user, LogManager.ERASE, "Table " + tableName + " not erased as it is used by " + surveys.toString(), 0, null);
					} else {				
					
						int rowCount = 0;
	
						// Ensure the table is empty
						if(!delData) {
							try {
								sql = "select count(*) from " + tableName + ";";
								if(pstmtCompound != null) try {pstmtCompound.close();}catch(Exception e) {}
								pstmtCompound = rel.prepareStatement(sql);
								ResultSet resultSetCount = pstmtCompound.executeQuery();
								resultSetCount.next();							
								rowCount = resultSetCount.getInt(1);
							} catch (Exception e) {
								log.severe("failed to get count from table");
							}
						}
	
						Statement stmtRel = null;
						try {
							if(delData || (rowCount == 0)) {
								if(GeneralUtilityMethods.tableExists(sd, tableName)) {
									sql = "drop table " + tableName + ";";
									log.info(sql + " : " + tableName);
									stmtRel = rel.createStatement();
									stmtRel.executeUpdate(sql);	
								}
								
								/*
								 * Delete record event entries for this table
								 */
								RecordEventManager rem = new RecordEventManager();
								rem.deleteTableEvents(sd, tableName);
								
								/*
								 * Delete records of notifications for this table
								 */
								MessagingManager mm = new MessagingManager(localisation);
								mm.deleteTableEvents(rel, tableName);
								/*
								 * Drop any tables for compound data
								 */
								if(tableName.length() > 3) {
									sql = "select table_name from information_schema.tables where table_name like '" + tableName + "_%'";
									if(pstmtCompound != null) try {pstmtCompound.close();}catch(Exception e) {}
									pstmtCompound = rel.prepareStatement(sql);
									ResultSet rs = pstmtCompound.executeQuery();
									while (rs.next()) {
										
										String markerTable = rs.getString(1);
										// Check that this looks like a marker table
										if(GeneralUtilityMethods.hasColumn(rel, markerTable, "properties") 
												&& GeneralUtilityMethods.hasColumn(rel, markerTable, "locn")) {
											
											if(stmtRel != null) try{stmtRel.close();} catch(Exception e) {}
	
											sql = "drop table " + markerTable;
											stmtRel = rel.createStatement();
											stmtRel.executeUpdate(sql);	
										}
									}
								}
								
							} else {
								nonEmptyDataTables = true;
							}
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
						} finally {
							if(stmtRel != null) try{stmtRel.close();} catch(Exception e) {}
						}
					}

				}		
			} 

			if(delData || !nonEmptyDataTables) {

				/*
				 * Delete any attachments
				 */
				String fileFolder = basePath + "/attachments/" + surveyIdent;
				File folder = new File(fileFolder);
				try {
					log.info("Deleting attachments folder: " + fileFolder);
					FileUtils.deleteDirectory(folder);
				} catch (IOException e) {
					log.info("Error deleting attachments directory:" + fileFolder + " : " + e.getMessage());
				}

				/*
				 * Delete any raw upload data
				 */
				fileFolder = basePath + "/uploadedSurveys/" + surveyIdent;
				folder = new File(fileFolder);
				try {
					log.info("Deleting uploaded files for survey: " + surveyDisplayName + " in folder: " + fileFolder);
					FileUtils.deleteDirectory(folder);
				} catch (IOException e) {
					log.info("Error deleting uploaded instances: " + fileFolder + " : " + e.getMessage());
				}

				/*
				 * Delete any media files
				 */
				fileFolder = basePath + "/media/" + surveyIdent;
				folder = new File(fileFolder);
				try {
					log.info("Deleting media files for survey: " + surveyDisplayName + " in folder: " + fileFolder);
					FileUtils.deleteDirectory(folder);
				} catch (IOException e) {
					log.info("Error deleting media files: " + fileFolder + " : " + e.getMessage());
				}


				// Delete the survey  resources 
				int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);
			    CsvTableManager tm = new CsvTableManager(sd, localisation);
			    tm.delete(oId, sId, null);		

				// Delete survey definition
				sql = "delete from survey where s_id = ?;";	
				if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, sId);
				log.info("Delete survey definition: " + pstmt.toString());
				pstmt.execute();

				// Delete changeset data, this is an audit trail of modifications to the data
				sql = "delete from changeset where s_id = ?;";	
				if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
				pstmt = rel.prepareStatement(sql);
				pstmt.setInt(1, sId);
				log.info("Delete changeset data: " + pstmt.toString());
				pstmt.execute();
				
				
				/*
				 * Delete survey templates
				 */
				sql = "delete from survey_template where ident = ?";	
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, surveyIdent);
				log.info("Delete survey templates: " + pstmt.toString());
				pstmt.executeUpdate();

				// Delete the template files
				// No longer required but keep this to delete legacy
				try {
					GeneralUtilityMethods.deleteTemplateFiles(surveyDisplayName, basePath, projectId );
				} catch (Exception e) {
					log.info("Error deleting templates: " + surveyDisplayName + " : " + e.getMessage());
				}
				try {
					GeneralUtilityMethods.deleteDirectory(basePath + "/templates/survey/" + surveyIdent);
				} catch (Exception e) {
					log.info("Error deleting pdf templates: " + surveyDisplayName + " : " + e.getMessage());
				}
				
			}


		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtCompound != null) {pstmtCompound.close();}} catch (SQLException e) {}
		}
	}

}