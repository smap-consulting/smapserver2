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
import java.util.logging.Level;
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
	public static String AU_STATUS_TIMEOUT = "timeout";
	
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
				String auto = params.get("auto");	// legacy
				String auto_annotate = params.get("auto_annotate");
				if((auto_annotate != null && auto_annotate.equals("yes")) 
						|| (auto != null && auto.equals("yes"))) {
					
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
		
		AudioProcessing ap = new AudioProcessing(region);	
		ResourceManager rm = new ResourceManager();
		
		String sql = "select "
				+ "id,"
				+ "o_id,"
				+ "type, "
				+ "job,"
				+ "table_name,"
				+ "col_name,"
				+ "instanceid,"
				+ "locale, "
				+ "now() > (request_initiated + interval '24 hours') as timed_out "
				+ "from aws_async_jobs where status = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, AU_STATUS_PENDING);
			
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				int id = rs.getInt(1);
				int oId = rs.getInt(2);
				String type = rs.getString(3);
				String job = rs.getString(4);
				String tableName = rs.getString(5);
				String colName = rs.getString(6);
				String instanceId = rs.getString(7);
				String locale = rs.getString(8);
				boolean timedOut = rs.getBoolean(9);
				
				if(locale == null) {
					locale = "en";
				}
				Locale orgLocale = new Locale(locale);
				ResourceBundle localisation = null;
				try {
					localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
				} catch(Exception e) {
					localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
				}
				
				boolean success = false;
				
				if(type != null && type.equals(AUTO_UPDATE_AUDIO)) {
					String urlString = ap.getTranscriptUri(job);
					
					if(urlString != null && urlString.startsWith("https")) {
						
						String output = null;
						String status = null;
						int durn = 0;
						
						try {					
							TranscribeResultSmap trs = gson.fromJson(
									GeneralUtilityMethods.readTextUrl(urlString), 
									TranscribeResultSmap.class);
							
							output = trs.results.transcripts.get(0).transcript;
							
							// Get the end time
							for(int i = trs.results.items.size() - 1; i > 0; i--) {
								String durnString = trs.results.items.get(i).end_time;
								if(durnString != null) {
									try {
										Double durnDouble = Double.valueOf(durnString);
										durn = (int) Math.round(durnDouble);
									} catch (Exception e) {
										log.log(Level.SEVERE, e.getMessage(), e);
									}
									break;
								}
							}
												
							status = AU_STATUS_COMPLETE;
							
						} catch (Exception e) {
							output = "[" + e.getMessage()+ "]";
							status = AU_STATUS_ERROR;
							urlString = null;
						}
						// Write result to database and update the job status
						success = true;
						writeResult(cResults, tableName, colName, instanceId, output);
						updateSyncStatus(sd, id, status, urlString, durn);
						
						if(durn > 0) {
							int billableDuration = (durn > 15) ? durn : 15;		// Minimum billable time is 15 seconds
							String msg = localisation.getString("aws_t_au_trans")
									.replace("%s3", tableName)
									.replace("%s4", colName);
							rm.recordUsage(sd, oId, 0, LogManager.TRANSCRIBE, msg, 
									"auto_update", billableDuration);
						} else {
							log.info("Error:xxxxxxxxxx duration of audio recorded as 0");
						}
					} 
				}
				
				if(!success && timedOut) {
					writeResult(cResults, tableName, colName, instanceId, 
							"[" + localisation.getString("aws_t_timeout") + "]");
					updateSyncStatus(sd, id, AU_STATUS_TIMEOUT, null, 0);
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

		LanguageCodeManager lcm = new LanguageCodeManager();
		
		String sqlAsync = "insert into aws_async_jobs"
				+ "(o_id, table_name, col_name, instanceid, type, "
				+ "update_details, job, status, locale, request_initiated) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, now())";
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
									
								if(rm.canUse(sd, item.oId, LogManager.REKOGNITION)) {
									if(source.trim().startsWith("attachments")) {
										
										try {
											output = ip.getLabels(
													basePath + "/",
													source, 
													item.labelColType,
													mediaBucket);
										} catch (Exception e) {
											output = "[Error: " + e.getMessage() + "]";
										}
										
										rm.recordUsage(sd, item.oId, 0, LogManager.REKOGNITION, "Batch: " + "/smap/" + source, "auto_update", 1);
										
									} else {
										output = "[Error: invalid source data " + source + "]";
									}
								} else {
									String msg = localisation.getString("re_error")
											.replace("%s1", LogManager.REKOGNITION);
									output = "[" + msg + "]";
									lm.writeLogOrganisation(sd, item.oId, "auto_update", LogManager.LIMIT, msg, 0);
								}
									
							} else if(item.type.equals(AUTO_UPDATE_AUDIO)) {
								
								if(rm.canUse(sd, item.oId, LogManager.TRANSCRIBE)) {
									if(source.trim().startsWith("attachments")) {
										
										// Unique job within the account
										StringBuffer job = new StringBuffer(item.tableName)
												.append("_")
												.append(String.valueOf(UUID.randomUUID()));
										
										if(lcm.isSupported(sd, item.fromLang, LanguageCodeManager.LT_TRANSCRIBE)) {
											try {
												String  status = ap.submitJob(
														localisation, 
														basePath + "/",
														source, 
														item.fromLang,
														job.toString(),
														mediaBucket);
	
												if(status.equals("IN_PROGRESS")) {
													lm.writeLogOrganisation(sd, item.oId, "auto_update", LogManager.TRANSCRIBE, "Batch: " + "/smap/" + source, 0);
	
													// Write result to async table, the transcript will be retrieved later
													pstmtAsync.setInt(1, item.oId);
													pstmtAsync.setString(2, item.tableName);
													pstmtAsync.setString(3, item.targetColName);
													pstmtAsync.setString(4, instanceId);
													pstmtAsync.setString(5, item.type);
													pstmtAsync.setString(6, gson.toJson(item));
													pstmtAsync.setString(7, job.toString());
													pstmtAsync.setString(8, AU_STATUS_PENDING);
													pstmtAsync.setString(9, item.locale);
													log.info("Save to Async queue: " + pstmtAsync.toString());
													pstmtAsync.executeUpdate();
	
													// Update tables to record that update is pending
													output = "[" + localisation.getString("c_pending") + "]";
												} else {
													output = "[" + status + "]";
												}
											} catch (Exception e) {
												output = "[Error: " + e.getMessage() + "]";
											}
										} else {
											if(item.fromLang == null) {
												output = "[Error: " + localisation.getString("aws_t_np").replace("%s1", "from_lang") + "]";
											} else {
												output = "[Error: " + localisation.getString("aws_t_ilc").replace("%s1", item.fromLang) + "]";
											}
										}
									} else {
										output = "[Error: invalid source data " + source + "]";
									}
								} else {
									String msg = localisation.getString("re_error")
											.replace("%s1", LogManager.TRANSCRIBE);
									output = "[" + msg + "]";
									lm.writeLogOrganisation(sd, item.oId, "auto_update", LogManager.LIMIT, msg, 0);
								}
							} else if(item.type.equals(AUTO_UPDATE_TEXT)) {
									
								if(rm.canUse(sd, item.oId, LogManager.TRANSLATE)) {
									
									if(lcm.isSupported(sd, item.fromLang, LanguageCodeManager.LT_TRANSLATE)) {
										if(lcm.isSupported(sd, item.toLang, LanguageCodeManager.LT_TRANSLATE)) {
											output = tp.getTranslatian(source, item.fromLang, item.toLang);
											String msg = localisation.getString("aws_t_au")
													.replace("%s1", item.fromLang)
													.replace("%s2", item.toLang)
													.replace("%s3", item.tableName)
													.replace("%s4", item.targetColName);
											rm.recordUsage(sd, item.oId, 0, LogManager.TRANSLATE, msg, 
													"auto_update", source.length());					
										} else {
											if(item.toLang == null) {
												output = "[" + localisation.getString("aws_t_np").replace("%s1", "to_lang") + "]";
											} else {
												output = "[" + localisation.getString("aws_t_ilc").replace("%s1", item.toLang) + "]";
											}
										}
									} else {
										if(item.fromLang == null) {
											output = "[" + localisation.getString("aws_t_np").replace("%s1", "from_lang") + "]";
										} else {
											output = "[" + localisation.getString("aws_t_ilc").replace("%s1", item.fromLang) + "]";
										}
									}
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
		
		String sql = "select q.qname, q.column_name, f.name, f.table_name, q.parameters, q.qtype, f.s_id, f.reference "
				+ "from question q, form f, survey s "
				+ "where q.f_id = f.f_id "
				+ "and f.s_id = s.s_id "
				+ "and not s.deleted "
				+ "and not s.blocked "
				+ "and q.parameters is not null "
				+ "and q.parameters like '%source=%'"
				+ "and (q.parameters like '%auto=yes%' || q.parameters like '%auto_annotate=yes%')";

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
						rs.getInt("s_id"),
						rs.getBoolean("reference"));
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
		
		if(colName != null && tableName != null && instanceId != null) {
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
	}
	

	/*
	 * Write a result to the latest record in the results table
	 */
	private void updateSyncStatus(Connection sd, 		
			int id,
			String status,
			String urlString,
			long durn
		) throws SQLException {
		
		PreparedStatement pstmt = null;
		
		String sqlUpdate = "update aws_async_jobs " 
				+ "set status = ?,"
				+ "results_link = ?,"
				+ "duration = ?, "
				+ "request_completed = now() "
				+ "where id = ?";
		
		pstmt = sd.prepareStatement(sqlUpdate);
		
		pstmt.setString(1, status);
		pstmt.setString(2, urlString);
		pstmt.setLong(3, durn);
		pstmt.setInt(4, id);
		
		log.info("Update sync status: " + pstmt.toString());
		pstmt.executeUpdate();
		
		try {
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
}


