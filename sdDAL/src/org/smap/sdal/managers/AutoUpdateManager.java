package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import org.smap.notifications.interfaces.AudioProcessing;
import org.smap.notifications.interfaces.ImageProcessing;
import org.smap.notifications.interfaces.TextProcessing;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.AutoUpdate;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.TranscribeResultSmap;

import com.google.gson.Gson;

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
 * Manage the log table
 * Assume emails are case insensitive
 */
public class AutoUpdateManager {
	
	private static Logger log =
			 Logger.getLogger(AutoUpdateManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	public static String AUTO_UPDATE_IMAGE = "imagelabel";
	public static String AUTO_UPDATE_AUDIO = "audiotranscript";
	public static String AUTO_UPDATE_TEXT = "texttranslate";
	
	public static String AU_STATUS_PENDING = "pending";
	public static String AU_STATUS_COMPLETE = "complete";
	public static String AU_STATUS_ERROR = "error";
	
	public AutoUpdateManager() {

	}
	
	/*
	 * Identify any auto updates required for this submission and write to a table
	 */
	public ArrayList<AutoUpdate> identifyAutoUpdates(Connection sd, 
			Connection cResults, 
			Gson gson) throws SQLException {
		ArrayList<AutoUpdate> autoUpdates = new ArrayList<AutoUpdate> ();	
		
		HashMap<String, QuestionForm> groupQuestions = getAutoUpdateQuestions(sd);
		HashMap<Integer, String> localeHashMap = new HashMap<> ();		// reduce database access
		
		for(String q : groupQuestions.keySet()) {
			QuestionForm qf = groupQuestions.get(q);
			if(qf.parameters != null) {
				HashMap<String, String> params = GeneralUtilityMethods.convertParametersToHashMap(qf.parameters);
				String auto = params.get("auto");
				if(auto != null && auto.equals("yes")) {
					
					ResourceBundle localisation = null;
					String itemLocaleString = null;
					
					if(params.get("source") != null) {
						
						String refColumn = params.get("source").trim();					
						if(refColumn.startsWith("$") && refColumn.length() > 3) {	// Remove ${} syntax if the source has that
							refColumn = refColumn.substring(2, refColumn.length() -1);
						}		
						
						if(GeneralUtilityMethods.hasColumn(cResults, qf.tableName, refColumn)) {
					
							int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, qf.s_id);
							
							itemLocaleString = localeHashMap.get(oId);
							if(itemLocaleString == null) {
								Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
								itemLocaleString = organisation.locale;
								localeHashMap.put(oId, itemLocaleString);
							}
							Locale orgLocale = new Locale(itemLocaleString);								
							try {
								localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
							} catch(Exception e) {
								localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
							}
							SurveyManager sm = new SurveyManager(localisation, "UTC");			
							
							int groupSurveyId = getGroupSurveyId(sd, qf.s_id);
							HashMap<String, QuestionForm> refQuestionMap = sm.getGroupQuestions(sd, 
									groupSurveyId, 
									" q.column_name = '" + refColumn + "'");
							
							QuestionForm refQf = refQuestionMap.get(refColumn);
							
							if(refQf.qType != null 
									&& (refQf.qType.equals("image")
											|| refQf.qType.equals("audio")
											|| refQf.qType.equals("string"))) {
								
								String updateType = null;
								String fromLang = params.get("from_lang");
								String toLang = params.get("to_lang");
								
								if(refQf.qType.equals("image")) {
									updateType = AUTO_UPDATE_IMAGE;
								} else if(refQf.qType.equals("audio")) {
									updateType = AUTO_UPDATE_AUDIO;
								} else if(refQf.qType.equals("string")) {
									updateType = AUTO_UPDATE_TEXT;
								}
								
								// Validate
								if(updateType == null) {
									log.info("------------------ AutoUpdate: Error: invalid reference question type" + refQf.qType);
								} else if(updateType.equals(AUTO_UPDATE_TEXT) &&
										(fromLang == null || toLang == null)) {
									log.info("------------------ AutoUpdate: Error: From Language / To Language not specified for text translation");
								} else {
									AutoUpdate au = new AutoUpdate(updateType);
									au.oId = oId;
									au.locale = itemLocaleString;
									au.labelColType = "text";
									au.sourceColName = refColumn;
									au.targetColName = qf.columnName;
									au.tableName = qf.tableName;
									au.fromLang = fromLang;
									au.toLang = toLang;
									autoUpdates.add(au);
								}
							} 
						} else {
							log.info("------------------ AutoUpdate: Error: " + refColumn + " not found in " + qf.tableName);
						}
					} 
				} else {
					log.info("------------------ AutoUpdate: auto not set for " + qf.qName);
				}
			}
		}
		
		return autoUpdates;
	}
	
	/*
	 * Check pending async jobs to see if they have finished
	 */
	public void checkPendingJobs(
		Connection sd,
		Connection cResults,
		Gson gson,
		String region) {
		
		ImageProcessing ip = new ImageProcessing(region);
		AudioProcessing ap = new AudioProcessing(region);		
		
		String sql = "select "
				+ "id,"
				+ "type, "
				+ "job,"
				+ "table_name,"
				+ "col_name,"
				+ "instanceid "
				+ " from aws_async_jobs where status = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, AU_STATUS_PENDING);
			
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				int id = rs.getInt(1);
				String type = rs.getString(2);
				String job = rs.getString(3);
				String tableName = rs.getString(4);
				String colName = rs.getString(5);
				String instanceId = rs.getString(6);
				
				if(type.equals(AUTO_UPDATE_AUDIO)) {
					String urlString = ap.getTranscriptUri(job);
					
					if(urlString != null && urlString.startsWith("https")) {
						
						String output = null;
						String status = null;
						
						try {					
							TranscribeResultSmap trs = gson.fromJson(
									GeneralUtilityMethods.readTextUrl(urlString), 
									TranscribeResultSmap.class);
							
							output = trs.results.transcripts.get(0).transcript;
							status = AU_STATUS_COMPLETE;
							
							
						} catch (Exception e) {
							output = "[" + e.getMessage()+ "]";
							status = AU_STATUS_ERROR;
							urlString = null;
						}
						// Write result to database and update the job status
						writeResult(cResults, tableName, colName, instanceId, output);
						updateSyncStatus(sd, id, status, urlString);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			
		}
	}
	
	/*
	 * Apply auto update changes
	 */
	public void applyAutoUpdates(
			Connection sd,
			Connection cResults,
			Gson gson,
			String server, 
			ArrayList<AutoUpdate> updates,
			String mediaBucket,
			String region,
			String basePath) {
		
		PreparedStatement pstmt = null;

		String sqlAsync = "insert into aws_async_jobs"
				+ "(o_id, table_name, col_name, instanceid, type, "
				+ "update_details, job, status, request_initiated) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, now())";
		PreparedStatement pstmtAsync = null;
		
		try {

			pstmtAsync = sd.prepareStatement(sqlAsync);
			
			ImageProcessing ip = new ImageProcessing(region);
			AudioProcessing ap = new AudioProcessing(region);	
			TextProcessing tp = new TextProcessing(region);	
			ResourceManager rm = new ResourceManager();
			
			// For each update item get the records that are null and need updating
			for(AutoUpdate item : updates) {
					
				if(GeneralUtilityMethods.hasColumn(cResults, item.tableName, item.sourceColName)) {
					
					if(GeneralUtilityMethods.hasColumn(cResults, item.tableName, item.targetColName)) {
						
						Locale orgLocale = new Locale(item.locale);
						ResourceBundle localisation = null;
						try {
							localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
						} catch(Exception e) {
							localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
						}
						
						String sql = "select instanceid," + item.sourceColName 
								+ " from " + item.tableName 
								+ " where " + item.targetColName + " is null "
								+ "and " + item.sourceColName + " is not null "
								+ "and not _bad";
						if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
						pstmt = cResults.prepareStatement(sql);
							
						// Diagnostic log.info("Get auto updates: " + pstmt.toString());						
						ResultSet rs = pstmt.executeQuery();
						while (rs.next()) {
							String instanceId = rs.getString(1);
							String source = rs.getString(2);
							String output = "";
							
							if(item.type.equals(AUTO_UPDATE_IMAGE)) {
									
								if(source.trim().startsWith("attachments")) {
									
									output = ip.getLabels(
											basePath + "/",
											source, 
											item.labelColType,
											mediaBucket);
									
									lm.writeLog(sd, item.oId, "auto_update", LogManager.REKOGNITION, "Batch: " + "/smap/" + source, 0);
									
								} else {
									output = "[Error: invalid source data " + source + "]";
								}
							} else if(item.type.equals(AUTO_UPDATE_AUDIO)) {
									
								if(source.trim().startsWith("attachments")) {
									
									// Unique job within the account
									StringBuffer job = new StringBuffer(item.tableName)
											.append("_")
											.append(String.valueOf(UUID.randomUUID()));
									
									String  status = ap.submitJob(
											localisation, 
											basePath + "/",
											source, 
											item.labelColType, 
											job.toString(),
											mediaBucket);
									
									if(status.equals("IN_PROGRESS")) {
										lm.writeLogOrganisation(sd, item.oId, "auto_update", LogManager.TRANSCRIBE, "Batch: " + "/smap/" + source, 0);
										
										// Write result to async table, the labels will be retrieved later
										pstmtAsync.setInt(1, item.oId);
										pstmtAsync.setString(2, item.tableName);
										pstmtAsync.setString(3, item.targetColName);
										pstmtAsync.setString(4, instanceId);
										pstmtAsync.setString(5, item.type);
										pstmtAsync.setString(6, gson.toJson(item));
										pstmtAsync.setString(7, job.toString());
										pstmtAsync.setString(8, AU_STATUS_PENDING);
										log.info("Save to Async queue: " + pstmtAsync.toString());
										pstmtAsync.executeUpdate();
										
										// Update tables to record that update is pending
										output = "[" + localisation.getString("c_pending") + "]";
									} else {
										output = "[" + status + "]";
									}
								} else {
									output = "[Error: invalid source data " + source + "]";
								}
							} else if(item.type.equals(AUTO_UPDATE_TEXT)) {
									
								if(rm.canUse(sd, item.oId, LogManager.TRANSLATE)) {
									
									output = tp.getTranslatian(source, item.fromLang, item.toLang);
									String msg = localisation.getString("aws_t_au")
											.replace("%s1", item.fromLang)
											.replace("%s2", item.toLang)
											.replace("%s3", item.tableName)
											.replace("%s4", item.targetColName);
									lm.writeLogOrganisation(sd, item.oId, "auto_update", LogManager.TRANSLATE, msg, 
											source.length());
									rm.recordUsage(sd, item.oId, 0, LogManager.TRANSLATE, msg, 
											"auto_update", source.length());
								} else {
									String msg = localisation.getString("re_error")
											.replace("%s1", LogManager.TRANSLATE);
									output = "[" + msg + "]";
									lm.writeLogOrganisation(sd, item.oId, "auto_update", LogManager.LIMIT, msg, 0);
								}
									
							} else {
								String msg = "cannot perform auto update for update type: \" + item.type";
								log.info("Error: " + msg);
								output = "[" + localisation.getString("c_error") + " " + msg + "]";
							}
								
							// Write result to database
							writeResult(cResults, item.tableName, item.targetColName, instanceId, output);					
								
							
						} 
					} else {
						log.info("------------ AutoUpdate: Target Columns not found: " + item.targetColName + " in " + item.tableName);
					}				
				} else {
					log.info("------------ AutoUpdate: Source Columns not found: " + item.sourceColName + " in " + item.tableName);
				}
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmtAsync != null) {try {pstmtAsync.close();} catch(Exception e) {}}
		}
	}
	
	private HashMap<String, QuestionForm> getAutoUpdateQuestions(Connection sd) throws SQLException {
		
		HashMap<String, QuestionForm> auQuestions = new HashMap<> ();
		
		String sql = "select q.qname, q.column_name, f.name, f.table_name, q.parameters, q.qtype, f.s_id "
				+ "from question q, form f, survey s "
				+ "where q.f_id = f.f_id "
				+ "and f.s_id = s.s_id "
				+ "and not s.deleted "
				+ "and not s.blocked "
				+ "and q.parameters like '%source=%'"
				+ "and q.parameters like '%auto=yes%'";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();

			while (rs.next()) {
				QuestionForm qt = new QuestionForm(
						rs.getString("qname"), 
						rs.getString("column_name"),
						rs.getString("name"),
						rs.getString("table_name"),
						rs.getString("parameters"),
						rs.getString("qtype"),
						rs.getInt("s_id"));
				auQuestions.put(rs.getString("column_name"), qt);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return auQuestions;
	}
	
	private int getGroupSurveyId(Connection sd, int sId) throws SQLException {
		int groupSurveyId = sId;
		
		String sql = "select group_survey_id from survey where s_id = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				int id = rs.getInt(1);
				if(id > 0) {
					groupSurveyId = id;
				}
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
		return groupSurveyId;
	}
	
	/*
	 * Write a result to the latest record in the results table
	 */
	private void writeResult(Connection cResults, 
			String tableName,
			String colName,
			String instanceId, 
			String output) throws SQLException {
		
		PreparedStatement pstmt = null;
		
		String sql = "update " 
				+ tableName 
				+ " set " + colName + " = ? "
				+ "where instanceid = ?";
		
		try {			
			pstmt = cResults.prepareStatement(sql);
			
			instanceId = GeneralUtilityMethods.getLatestInstanceId(cResults, tableName, instanceId);
			pstmt.setString(1, output);
			pstmt.setString(2, instanceId);
			pstmt.executeUpdate();
		
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
	

	/*
	 * Write a result to the latest record in the results table
	 */
	private void updateSyncStatus(Connection sd, 		
			int id,
			String status,
			String urlString
		) throws SQLException {
		
		PreparedStatement pstmt = null;
		
		String sqlUpdate = "update aws_async_jobs " 
				+ "set status = ?,"
				+ "results_link = ?,"
				+ "request_completed = now() "
				+ "where id = ?";
		
		pstmt = sd.prepareStatement(sqlUpdate);
		
		pstmt.setString(1, status);
		pstmt.setString(2, urlString);
		pstmt.setInt(3, id);
		pstmt.executeUpdate();
		
		try {
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
}


