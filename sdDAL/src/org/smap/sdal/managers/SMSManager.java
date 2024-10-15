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
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.legacy.UtilityMethods;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.ConversationItemDetails;
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
	
	public static String SMS_TYPE = "SMS";
	public static String FORM_TYPE = "Form";
	
	private static Logger log =
			 Logger.getLogger(SMSManager.class.getName());
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create(); 
	
	private String sqlGetNumber = "select "
			+ "element_identifier,"
			+ "our_number,"
			+ "channel,"
			+ "survey_ident,"
			+ "their_number_question,"
			+ "message_question,"
			+ "o_id "
			+ "from sms_number ";
	
	private ResourceBundle localisation;
	private String tz;
	
	public SMSManager(ResourceBundle l, String tz) {
		this.localisation = l;
		this.tz = tz;
	}
	
	/*
	 * Get a list of available numbers
	 */
	public ArrayList<SMSNumber> getOurNumbers(Connection sd, String user, boolean orgOnly) throws SQLException {
		
		ArrayList<SMSNumber> numbers = new ArrayList<>();
		
		StringBuilder sqlSelect = new StringBuilder(sqlGetNumber);
		PreparedStatement pstmt = null;
		
		try {
			
			/*
			 * If the request is not explicitely for the current organisation then
			 * force it to be so, if the user is not an organisation administrator
			 */
			if(orgOnly || !GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.ORG_ID)) {
				sqlSelect.append("where o_id = ? ");
				orgOnly = true;
			}
			
			sqlSelect.append("order by time_modified asc ");
			pstmt = sd.prepareStatement(sqlSelect.toString());
			if(orgOnly) {
				pstmt.setInt(1, GeneralUtilityMethods.getOrganisationId(sd, user));
			}
			
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				SMSNumber n = getNumber(rs);
				getSecondaryNumberAttributes(sd, n);
				numbers.add(n);
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch (Exception e) {}}
		}
		return numbers;
	}
	
	/*
	 * Write an upload entry for the message
	 */
	public void saveMessage(
			Connection sd,
			ConversationItemDetails sms,
			String serverName,
			String instanceId)  {
		
		String sql = "insert into upload_event ("
				+ "upload_time,"
				+ "user_name, "
				+ "submission_type, "
				+ "payload, "
				+ "server_name,"
				+ "status,"
				+ "s_id,"
				+ "instanceid) "
				+ "values (now(), ?, ?, ?, ?, 'success', 0, ?);";

		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sms.ourNumber);  	// User
			pstmt.setString(2, SMS_TYPE);			// Submission type
			pstmt.setString(3, gson.toJson(sms));	// Payload
			pstmt.setString(4, serverName);			// Server Name
			pstmt.setString(5, instanceId);			// Instance Id
			
			log.info("----- new sms " + pstmt.toString());
			pstmt.executeUpdate();
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	public SMSNumber getDetailsForOurNumber(Connection sd, String ourNumber) throws SQLException {
		
		StringBuilder sqlSelect = new StringBuilder(sqlGetNumber);
		sqlSelect.append("where our_number = ?");
		PreparedStatement pstmt = null;
		
		SMSNumber smsNumber = null;
		
		try {
			
			// Ignore "+"
			if(ourNumber.startsWith("+")) {
				ourNumber = ourNumber.substring(1);
			} 
			/*
			 * Get destination for the SMS
			 */		
			pstmt = sd.prepareStatement(sqlSelect.toString());
			pstmt.setString(1, ourNumber);
			log.info("Get SMS number details: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				smsNumber = getNumber(rs);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		return smsNumber;
	}

	public SMSNumber getDetailsForSurvey(Connection sd, String surveyIdent) throws SQLException {
		
		StringBuilder sqlSelect = new StringBuilder(sqlGetNumber);
		PreparedStatement pstmt = null;		
		SMSNumber smsNumber = null;
		sqlSelect.append("where survey_ident = ?");
		try {
			
			/*
			 * Get destination for the SMS
			 */		
			pstmt = sd.prepareStatement(sqlSelect.toString());
			pstmt.setString(1, surveyIdent);
			log.info("Get SMS destination: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				smsNumber = getNumber(rs);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		return smsNumber;
	}
	
	/*
	 * Write a text conversation to the results table
	 */
	public void writeInboundMessageToResults(Connection sd, 
			Connection cResults,
			SubscriberEvent se,
			String instanceid,
			ConversationItemDetails sms,
			int ueId,
			int ueSurveyId) throws Exception {
		
		PreparedStatement pstmtUploadEvent = null;
		PreparedStatement pstmtExists = null;
		PreparedStatement pstmt = null;
		
		AdvisoryLock lockTableChange = null;
		
		try {
			SMSNumber smsNumber = getDetailsForOurNumber(sd, sms.ourNumber);
			
			if(smsNumber != null) {
				
				getSecondaryNumberAttributes(sd, smsNumber);
				
				/*
				 * Ensure tables are fully published
				 */	
				int sId = GeneralUtilityMethods.getSurveyId(sd, smsNumber.surveyIdent);
				lockTableChange = new AdvisoryLock(sd, 1, sId);	// If necessary lock at the survey level
				UtilityMethods.createSurveyTables(sd, cResults, localisation, 
						sId, smsNumber.surveyIdent, tz, lockTableChange);
				
				String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, smsNumber.surveyIdent);
				String theirNumberColumn = GeneralUtilityMethods.getColumnName(sd, sId, smsNumber.theirNumberQuestion);
				String messageColumn = GeneralUtilityMethods.getColumnName(sd, sId, smsNumber.messageQuestion);	
				String existingInstanceId = null;
				
				/*
				 * Success message
				 */
				String msg = localisation.getString("msg_sms_received");
				msg = msg.replaceAll("%s1",  sms.msg);
				msg = msg.replaceAll("%s2", sms.theirNumber);
				msg = msg.replace("%s3", sms.channel);			
				
				int count = 0;
				
				/*
				 * Check to see if there is a reference to a case in the message
				 * Update the referenced case
				 */
				int existingPrikey = getReference(sms.msg);
				if(existingPrikey > 0) {
					count += updateExistingEntry(sd, cResults, sms, existingPrikey, messageColumn, tableName);
					updateHistory(sd, cResults, sms, tableName, existingInstanceId, smsNumber, msg);
				}
				
				/*
				 * If no record was updated then look for an existing case for this number
				 */
				if(count == 0) {
						
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
					
					boolean checkStatus = false;
					if(tableName != null) {
						StringBuilder sqlExists = new StringBuilder("select prikey, instanceid from ")
								.append(tableName)
								.append(" where not _bad and ")
								.append(theirNumberColumn)
								.append(" = ? ");
						
						if(statusQuestion != null && finalStatus != null) {
							checkStatus  = true;
							String statusColumn = GeneralUtilityMethods.getColumnName(sd, sId, statusQuestion);
							sqlExists.append(" and (")
								.append(statusColumn)
								.append(" is null or ")
								.append(statusColumn)
								.append(" != ?)");
						}
						
						pstmtExists = cResults.prepareStatement(sqlExists.toString());
						pstmtExists.setString(1, sms.theirNumber);
						if(checkStatus) {
							pstmtExists.setString(2, finalStatus);
						}
						log.info("Check for existing cases: " + pstmtExists.toString());
						ResultSet rs = pstmtExists.executeQuery();
						while(rs.next()) {
							existingPrikey = rs.getInt("prikey");
							existingInstanceId = rs.getString("instanceid");
							
							count += updateExistingEntry(sd, cResults, sms, existingPrikey, messageColumn, tableName);
							updateHistory(sd, cResults, sms, tableName, existingInstanceId, smsNumber, msg);

						}
						rs.close();
					}
				}	
				
				/*
				 * Create a new entry if an existing entry was not updated
				 */
				if(count == 0) {

					/*
					 * Create new entry
					 */
					log.info("Create new entry ");
					existingInstanceId = instanceid;
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
					log.info("Create new sms case: " + pstmt.toString());
					count = pstmt.executeUpdate();
					
					updateHistory(sd, cResults, sms, tableName, existingInstanceId, smsNumber, msg);
				} 
					
				/*
				 * Update Survey Details base on the settings for this number
				 * Only do this if the survey details in the upload event table are different from those in the number details table
				 */
				if(ueId > 0 && ueSurveyId != sId) {
					String sqlUploadEvent = "update upload_event "
							+ "set imei = 'SMS',"
							+ "o_id = ?,"
							+ "p_id = ?,"
							+ "s_id = ?, "
							+ "ident = ?,"
							+ "survey_name = ?,"
							+ "file_name = ? "
							+ "where ue_id = ?";
					pstmtUploadEvent = sd.prepareStatement(sqlUploadEvent);
					pstmtUploadEvent.setInt(1,  smsNumber.oId);
					pstmtUploadEvent.setInt(2,  smsNumber.pId);
					pstmtUploadEvent.setInt(3,  smsNumber.sId);
					pstmtUploadEvent.setString(4,  smsNumber.surveyIdent);
					pstmtUploadEvent.setString(5,  smsNumber.surveyName);
					pstmtUploadEvent.setString(6,  sms.msg);
					pstmtUploadEvent.setInt(7,  ueId);
					pstmtUploadEvent.executeUpdate();
				}
				
				se.setStatus("success");
				se.setReason(msg);
				
			} else {
				String msg = localisation.getString("msg_nf");
				msg = msg.replace("%s1", sms.ourNumber);
				msg = msg.replace("%s2", sms.msg);
				log.info("Error: " + msg);
				se.setStatus("error");
				se.setReason(msg);
			}
			
		} finally {
			
			if(lockTableChange != null) {
				lockTableChange.release("top level");    // Ensure lock is released before closing
				lockTableChange.close("top level");
			}
			
			if(pstmtUploadEvent != null) {try {pstmtUploadEvent.close();} catch (Exception e) {}}
			if(pstmtExists != null) {try {pstmtExists.close();} catch (Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
	}
	
	/*
	 * Append new message details to existing
	 */
	public ArrayList<ConversationItemDetails> getMessageText(ConversationItemDetails sms, ArrayList<ConversationItemDetails> current) {
		ArrayList<ConversationItemDetails> conversation = null;
		
		if(current != null) {
			conversation = current;
		} else {
			conversation = new ArrayList<ConversationItemDetails> ();
		}
		conversation.add(sms);
		
		return conversation;
	}
	
	/*
	 * Update existing entry
	 */
	private int updateExistingEntry(Connection sd,
			Connection cResults, 
			ConversationItemDetails sms,
			int existingPrikey,
			String messageColumn,
			String tableName) throws SQLException {
		
		int count = 0;
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmt = null;
		
		try {
			log.info("Update existing entry with prikey: " + existingPrikey);
			ArrayList<ConversationItemDetails> currentConv = null;
			Type type = new TypeToken<ArrayList<ConversationItemDetails>>() {}.getType();
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
			log.info("Update existing sms case: " + pstmt.toString());
			int c = pstmt.executeUpdate();
			if(c == 0) {
				log.info("Tried to update an existing case but nothing was updated");
			} else {
				count += c;
			}
			
		} finally {
			if(pstmtGet != null) {try {pstmtGet.close();} catch (Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
		
		return count;
	}
	
	private SMSNumber getNumber(ResultSet rs) throws SQLException {
		return new SMSNumber(rs.getString("element_identifier"),
				rs.getString("our_number"),
				rs.getString("survey_ident"),
				rs.getString("their_number_question"),
				rs.getString("message_question"),
				rs.getInt("o_id"),
				rs.getString("channel"));
	}
	
	/*
	 * Update the history for the record
	 */
	private void updateHistory(Connection sd, Connection cResults, ConversationItemDetails sms,
			String tableName,
			String existingInstanceId,
			SMSNumber smsNumber,
			String msg) throws SQLException {
		
		RecordEventManager rem = new RecordEventManager();
		rem.writeEvent(sd, cResults, 
				RecordEventManager.INBOUND_MESSAGE, 
				RecordEventManager.STATUS_SUCCESS,
				sms.theirNumber, 
				tableName, 
				existingInstanceId, 
				null, 					// Change object
				null, 					// Task object
				null,					// Notification object
				msg, 					// Description
				0, 						// sID legacy
				smsNumber.surveyIdent,	// Survey Ident
				0,
				0);	
		
	}
	
	private void getSecondaryNumberAttributes(Connection sd, SMSNumber n) throws SQLException {
		
		String sqlOrg = "select name from organisation where id = ?";
		PreparedStatement pstmtOrg = sd.prepareStatement(sqlOrg);
		
		String sqlSurvey = "select display_name, p_id, s_id from survey where ident = ?";
		PreparedStatement pstmtSurvey = sd.prepareStatement(sqlSurvey);
		
		try {
			/*
			 * Get org name
			 */
			if(n.oId > 0) {
				pstmtOrg.setInt(1, n.oId);
				ResultSet rsOrg = pstmtOrg.executeQuery();
				if(rsOrg.next()) {
					n.orgName = rsOrg.getString(1);
				}
			}
			
			/*
			 * Get survey name and project id
			 */
			if(n.surveyIdent != null) {
				pstmtSurvey.setString(1, n.surveyIdent);
				ResultSet rsSurvey = pstmtSurvey.executeQuery();
				if(rsSurvey.next()) {
					n.surveyName = rsSurvey.getString("display_name");
					n.pId = rsSurvey.getInt("p_id");
					n.sId = rsSurvey.getInt("s_id");
				}
			}
		} finally {
			if(pstmtOrg != null) {try{pstmtOrg.close();}catch (Exception e) {}}
			if(pstmtSurvey != null) {try{pstmtSurvey.close();}catch (Exception e) {}}
		}
	}
	
	/*
	 * Get the primary key which acts as the reference for a case from the message
	 */
	private int getReference(String msg) {
		int caseReference = 0;
		if(msg != null) {
			int idx = msg.lastIndexOf('#');
			if(idx >= 0) {
				String ref = msg.substring(idx + 1);
				int idx2 = ref.indexOf(' ');
				if(idx2 > 0) {
					ref = ref.substring(0, idx2);
				}
				try {
					caseReference = Integer.valueOf(ref);
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		}
		return caseReference;
	}
}


