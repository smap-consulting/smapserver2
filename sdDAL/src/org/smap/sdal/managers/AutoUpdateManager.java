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
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.AutoUpdate;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.Survey;

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
											|| refQf.qType.equals("audio"))) {
								
								String updateType = null;
								if(refQf.qType.equals("image")) {
									updateType = AUTO_UPDATE_IMAGE;
								} else if(refQf.qType.equals("audio")) {
									updateType = AUTO_UPDATE_AUDIO;
								}
								
								AutoUpdate au = new AutoUpdate(updateType);
								au.oId = oId;
								au.locale = itemLocaleString;
								au.labelColType = "text";
								au.sourceColName = refColumn;
								au.targetColName = qf.columnName;
								au.tableName = qf.tableName;
								autoUpdates.add(au);
								
								log.info("--------------- AutoUpdate: " + refQf.qType);
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
		Gson gson) {
		
		ImageProcessing ip = new ImageProcessing();
		AudioProcessing ap = new AudioProcessing();		
		
		String sql = "select type, job from au_aws_async where status = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, AU_STATUS_PENDING);
			
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String type = rs.getString(1);
				String job = rs.getString(2);
				if(type.equals(AUTO_UPDATE_AUDIO)) {
					String result = ap.getTranscript(job);
				}
				System.out.println("Pending job: " + rs.getString(2));
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
			int oId,
			ArrayList<AutoUpdate> updates) {
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtUpdate = null;

		String sqlAsync = "insert into au_aws_async(o_id, type, au_details, status, job, results, started) "
				+ "values(?, ?, ?, ?, ?, ?, now())";
		PreparedStatement pstmtAsync = null;
		try {

			pstmtAsync = sd.prepareStatement(sqlAsync);
			
			ImageProcessing ip = new ImageProcessing();
			AudioProcessing ap = new AudioProcessing();		
			
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
						
						String sql = "select prikey," + item.sourceColName 
								+ " from " + item.tableName 
								+ " where " + item.targetColName + " is null "
								+ "and " + item.sourceColName + " is not null";
						if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
						pstmt = cResults.prepareStatement(sql);
							
						String sqlUpdate = "update " 
								+ item.tableName 
								+ " set " + item.targetColName + " = ? where prikey = ?";
						if(pstmtUpdate != null) {try {pstmtUpdate.close();} catch(Exception e) {}}
						pstmtUpdate = cResults.prepareStatement(sqlUpdate);
							
						log.info("Get auto updates: " + pstmt.toString());						
						ResultSet rs = pstmt.executeQuery();
						while (rs.next()) {
							int prikey = rs.getInt(1);
							String source = rs.getString(2);
							String output = "";
							if(source.trim().startsWith("attachments")) {
								if(item.type.equals(AUTO_UPDATE_IMAGE)) {
									output = ip.getLabels(server, "auto_update", "/smap/" + source, item.labelColType);
									lm.writeLog(sd, oId, "auto_update", LogManager.REKOGNITION, "Batch: " + "/smap/" + source);
									
								} else if(item.type.equals(AUTO_UPDATE_AUDIO)) {
									
									// Unique job within the account
									StringBuffer job = new StringBuffer(item.tableName)
											.append("_")
											.append(prikey)
											.append("_")
											.append(String.valueOf(UUID.randomUUID()));
									
									String  result = ap.submitJob(server, "auto_update", "/smap/" + source, 
											item.labelColType, job.toString());
						
									lm.writeLogOrganisation(sd, oId, "auto_update", LogManager.TRANSCRIBE, "Batch: " + "/smap/" + source);
									
									// Write result to async table, the labels will be retrieved later
									pstmtAsync.setInt(1, item.oId);
									pstmtAsync.setString(2, item.type);
									pstmtAsync.setString(3, gson.toJson(item));
									pstmtAsync.setString(4, AU_STATUS_PENDING);
									pstmtAsync.setString(5, job.toString());
									pstmtAsync.setString(6, result);
									log.info("Save to Async queue: " + pstmtAsync.toString());
									pstmtAsync.executeUpdate();
									
									// Update tables to record that update is pending
									output = "[" + localisation.getString("c_pending") + "]";
								} else {
									String msg = "cannot perform auto update for update type: \" + item.type";
									log.info("Error: " + msg);
									output = "[" + localisation.getString("c_error") + " " + msg + "]";
								}
								
								// Write result to database
								pstmtUpdate.setString(1, output);
								pstmtUpdate.setInt(2, prikey);
								log.info("Autoupdate results: " + pstmtUpdate.toString());
								pstmtUpdate.executeUpdate();
								
							}
							
						} 
					} else {
						log.info("------------ AutoUpdate: Target Columns not found: " + item.targetColName + " in " + item.tableName);
					}				
				} else {
					log.info("------------ AutoUpdate: Target Columns not found: " + item.sourceColName + " in " + item.tableName);
				}
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmtUpdate != null) {try {pstmtUpdate.close();} catch(Exception e) {}}
			if(pstmtAsync != null) {try {pstmtAsync.close();} catch(Exception e) {}}


			try {
				if (sd != null) {
					sd.close();
					sd = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				if (cResults != null) {
					cResults.close();
					cResults = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
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
}


