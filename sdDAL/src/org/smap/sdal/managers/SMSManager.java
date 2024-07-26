package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.AdvisoryLock;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.legacy.TableManager;
import org.smap.sdal.legacy.UtilityMethods;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.SMSDetails;
import org.smap.sdal.model.SMSNumber;
import org.smap.sdal.model.SubscriberEvent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
 * Manage messages
 */
public class SMSManager {
	
	private static Logger log =
			 Logger.getLogger(SMSManager.class.getName());
	
	private static LogManager lm = new LogManager();		// Application log
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create(); 
	
	private ResourceBundle localisation;
	private String tz;
	
	public SMSManager(ResourceBundle l, String tz) {
		this.localisation = l;
		this.tz = tz;
	}
	/*
	 * Write a log entry that includes the survey id
	 */
	public void saveMessage(
			Connection sd,
			SMSDetails sms,
			String serverName)  {
		
		String sql = "insert into upload_event ("
				+ "upload_time,"
				+ "user_name, "
				+ "submission_type, "
				+ "payload, "
				+ "server_name,"
				+ "status,"
				+ "s_id,"
				+ "instanceid) "
				+ "values (now(), ?, 'SMS', ?, ?, 'success', 0, gen_random_uuid());";

		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sms.ourNumber);  	// User
			pstmt.setString(2, gson.toJson(sms));	// Payload
			pstmt.setString(3, serverName);			// Server Name
			
			log.info("----- new sms " + pstmt.toString());
			pstmt.executeUpdate();
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	public SMSNumber getDetailsForOurNumber(Connection sd, String ourNumber) throws SQLException {
		
		String sql = "select "
				+ "element_identifier,"
				+ "survey_ident,"
				+ "their_number_question,"
				+ "message_question "
				+ "from sms_number "
				+ "where our_number = ?";
		PreparedStatement pstmt = null;
		
		SMSNumber smsNumber = null;
		
		try {
			
			/*
			 * Get destination for the SMS
			 */		
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ourNumber);
			log.info("Get SMS number details: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				smsNumber = new SMSNumber(rs.getString("element_identifier"),
						ourNumber,
						rs.getString("survey_ident"),
						rs.getString("their_number_question"),
						rs.getString("message_question"));
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		return smsNumber;
	}

	public SMSNumber getDetailsForSurvey(Connection sd, String surveyIdent) throws SQLException {
		
		String sql = "select "
				+ "element_identifier,"
				+ "our_number,"
				+ "their_number_question,"
				+ "message_question "
				+ "from sms_number "
				+ "where survey_ident = ?";
		PreparedStatement pstmt = null;
		
		SMSNumber smsNumber = null;
		
		try {
			
			/*
			 * Get destination for the SMS
			 */		
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, surveyIdent);
			log.info("Get SMS destination: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				smsNumber = new SMSNumber(
						rs.getString("element_identifier"),
						rs.getString("our_number"),
						surveyIdent,
						rs.getString("their_number_question"),
						rs.getString("message_question"));
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		return smsNumber;
	}
	
	/*
	 * Write a text conversation to the results table
	 */
	public void writeMessageToResults(Connection sd, 
			Connection cResults,
			SubscriberEvent se,
			String instanceid,
			SMSDetails sms) throws Exception {
		
		PreparedStatement pstmtExists = null;
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmt = null;
		
		AdvisoryLock lockTableChange = null;
		
		try {
			SMSNumber smsNumber = getDetailsForOurNumber(sd, sms.ourNumber);
			
			if(smsNumber != null) {
				
				int sId = GeneralUtilityMethods.getSurveyId(sd, smsNumber.surveyIdent);
				String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, smsNumber.surveyIdent);
				String theirNumberColumn = GeneralUtilityMethods.getColumnName(sd, sId, smsNumber.theirNumberQuestion);
				String messageColumn = GeneralUtilityMethods.getColumnName(sd, sId, smsNumber.messageQuestion);				
				
				lockTableChange = new AdvisoryLock(sd, 1, sId);	// If necessary lock at the survey level
				
				/*
				 * Ensure tables are fully published
				 */
				lockTableChange.lock("table sms mod start");	// Start lock while modifying tables
				
				UtilityMethods.createSurveyTables(sd, cResults, localisation, 
						sId, smsNumber.surveyIdent, tz);
				lockTableChange.release("table sms mod done");
				
				/*
				 * Get the case details
				 */
				String statusQuestion = null;
				String finalStatus = null;
				CaseManager cm = new CaseManager(localisation);
				String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
				CMS caseSettings = cm.getCaseManagementSettings(sd, groupSurveyIdent);
				if(caseSettings != null) {
					statusQuestion = caseSettings.settings.statusQuestion;
					finalStatus = caseSettings.settings.finalStatus;
				}
				
				/*
				 * Check to see if there is an existing case for this number
				 */
				int existingPrikey = 0;
				boolean checkStatus = false;
	
				StringBuilder sqlExists = new StringBuilder("select prikey from ")
						.append(tableName)
						.append(" where ")
						.append(theirNumberColumn)
						.append(" = ? ");
				
				if(statusQuestion != null && finalStatus != null) {
					checkStatus  = true;
					String statusColumn = GeneralUtilityMethods.getColumnName(sd, sId, statusQuestion);
					sqlExists.append(" and ")
						.append(statusColumn)
						.append(" != ?");
				}
				
				pstmtExists = cResults.prepareStatement(sqlExists.toString());
				pstmtExists.setString(1, sms.theirNumber);
				if(checkStatus) {
					pstmtExists.setString(2, finalStatus);
				}
				log.info("Check for existing cases: " + pstmtExists.toString());
				ResultSet rs = pstmtExists.executeQuery();
				if(rs.next()) {
					existingPrikey = rs.getInt("prikey");
				}
				rs.close();
				
				if(existingPrikey == 0) {
					/*
					 * Create new entry
					 */
					log.info("Create new entry ");
					StringBuilder sql = new StringBuilder("insert into ")
							.append(tableName)
							.append(" (_user, instanceid, _thread")
							.append(",").append(theirNumberColumn)
							.append(",").append(messageColumn)
							.append(") values(?, ?, ?, ?, ?)");
					pstmt = cResults.prepareStatement(sql.toString());
					pstmt.setString(1, sms.ourNumber);
					pstmt.setString(2,  instanceid);
					pstmt.setString(3,  instanceid);	// thread
					pstmt.setString(4, sms.theirNumber);
					pstmt.setString(5, gson.toJson(getMessageText(sms, null)));
					
				} else {
					/*
					 * Update existing entry
					 */
					log.info("Update existing entry with prikey: " + existingPrikey);
					ArrayList<SMSDetails> currentConv = null;
					Type type = new TypeToken<ArrayList<SMSDetails>>() {}.getType();
					StringBuilder sqlGet = new StringBuilder("select ")
							.append(messageColumn)
							.append(" from ")
							.append(tableName)
							.append(" where prikey = ?");
					pstmtGet = cResults.prepareStatement(sqlGet.toString());
					pstmtGet.setInt(1, existingPrikey);
					log.info("Get existing: " + pstmtGet.toString());
					ResultSet rsGet = pstmtGet.executeQuery();
					if(rsGet.next()) {
						String currentConvString = rsGet.getString(1);
						if(currentConvString != null) {
							currentConv = gson.fromJson(currentConvString, type);
						}
					}
					
					// Create update statement
					StringBuilder sql = new StringBuilder("update ")
							.append(tableName)
							.append(" set ")
							.append(messageColumn)
							.append(" = ? where prikey = ?");
					pstmt = cResults.prepareStatement(sql.toString());
					pstmt.setString(1, gson.toJson(getMessageText(sms, currentConv)));
					pstmt.setInt(2, existingPrikey);			
				}
				
				log.info("Process sms: " + pstmt.toString());
				pstmt.executeUpdate();
				
				/*
				 * Write log entry
				 */		
				lm.writeLog(sd, sId, sms.theirNumber, LogManager.SMS, se.getStatus() + " : " 
						+ (se.getReason() == null ? "" : se.getReason()) + " : ", 0, null);
				
			} else {
				log.info("Error:  Inbound number " + sms.ourNumber + " not found");
				se.setStatus("error");
				se.setReason("SMS Inbound Number not found.  This number will need to be added to the numbers supported by the system before SMS messages to it can be processed.");
			}
			
		} finally {
			
			if(lockTableChange != null) {
				lockTableChange.release("top level");    // Ensure lock is released before closing
				lockTableChange.close("top level");
			}
			
			if(pstmtExists != null) {try {pstmtExists.close();} catch (Exception e) {}}
			if(pstmtGet != null) {try {pstmtGet.close();} catch (Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
	}
	
	/*
	 * Append new message details to existing
	 */
	private ArrayList<SMSDetails> getMessageText(SMSDetails sms, ArrayList<SMSDetails> current) {
		ArrayList<SMSDetails> conversation = null;
		
		if(current != null) {
			conversation = current;
		} else {
			conversation = new ArrayList<SMSDetails> ();
		}
		conversation.add(sms);
		
		return conversation;
	}
}


