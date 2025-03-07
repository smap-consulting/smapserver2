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
import org.smap.sdal.Utilities.ApplicationException;
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
import com.vonage.client.VonageClient;
import com.vonage.client.messages.MessageResponse;

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
	
	public static String SMS_TYPE = "SMS";		// More accurately would be "message", however there is now legacy entries where this is represented as SMS
	public static String FORM_TYPE = "Form";
	public static String NEW_CASE = "NewCase";
	
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
			+ "mc_msg,"
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
			
			sqlSelect.append("order by our_number asc ");
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
			String instanceId,
			String submissionType,
			String comment)  {
		
		String sql = "insert into upload_event ("
				+ "upload_time,"
				+ "user_name, "
				+ "submission_type, "
				+ "payload, "
				+ "server_name,"
				+ "status,"
				+ "s_id,"
				+ "instanceid,"
				+ "survey_notes) "
				+ "values (now(), ?, ?, ?, ?, 'success', 0, ?, ?);";

		PreparedStatement pstmt = null;
		
		try {
			
			if(comment != null && comment.trim().length() == 0) {
				comment = null;
			}
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sms.ourNumber);  	// User
			pstmt.setString(2, submissionType);		// Submission type
			pstmt.setString(3, gson.toJson(sms));	// Payload
			pstmt.setString(4, serverName);			// Server Name
			pstmt.setString(5, instanceId);			// Instance Id
			pstmt.setString(6, comment);			// Instance Id
			
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
			VonageClient vonageClient,
			SubscriberEvent se,
			String instanceid,
			ConversationItemDetails sms,
			int ueId,
			int ueSurveyId,
			String submissionType,
			String comment) throws Exception {
		
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
				
				boolean messageWritten = false;
				
				/*
				 * Process
				 * 1. Look for a reference, greater than 0, in the message (#{prikey}) and attempt to update that record
				 *    1.1. If the reference is 0 create a new entry
				 * 2. If not found, look for open cases initiated by "their" number
				 *    2.1.  If one case is found update that
				 *    2.2.  If more than one case is found send a response message requesting the user to include the case number
				 * 3. If still not found, create a new entry
				 * 
				 * The first two steps are only done if the message is a new inbound SMS type, if the
				 *  message is being re-applied by a user as a new case then a new entry is always created
				 */
				int existingPrikey = getReference(cResults, sms.msg, tableName);
				if(SMSManager.SMS_TYPE.equals(submissionType) && existingPrikey != 0) {
					/*
					 * 1. Check to see if there is a reference to a case in the message
					 * Update the referenced case
					 */
					
					if(existingPrikey > 0) {
						updateExistingEntry(sd, cResults, true, sms, existingPrikey, messageColumn, tableName, 0);
						updateHistory(sd, cResults, sms.theirNumber, tableName, existingInstanceId, smsNumber.surveyIdent, msg,
								RecordEventManager.INBOUND_MESSAGE);
						messageWritten = true;
					}
					
					/*
					 * 2. If no record was updated then look for an existing case for this number
					 */
					if(!messageWritten) {
							
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
							
							pstmtExists = cResults.prepareStatement(sqlExists.toString(), 
									ResultSet.TYPE_SCROLL_SENSITIVE, 
			                        ResultSet.CONCUR_UPDATABLE);
							pstmtExists.setString(1, sms.theirNumber);
							if(checkStatus) {
								pstmtExists.setString(2, finalStatus);
							}
							log.info("Check for existing cases: " + pstmtExists.toString());
							ResultSet rs = pstmtExists.executeQuery();
							
							/*
							 * Get the number of records
							 * If there is only one then that record will be updated
							 * If there is more than one record then a message requesting the user to specify the
							 *  exact record will be returned
							 */
							StringBuilder caseList = new StringBuilder("");
							int count = 0;
							if(rs.last()) {
								count = rs.getRow();
								rs.beforeFirst();
							}
							while(rs.next()) {
								existingPrikey = rs.getInt("prikey");
								existingInstanceId = rs.getString("instanceid");
								
								if(count == 1) {
									updateExistingEntry(sd, cResults, true, sms, existingPrikey, messageColumn, tableName, 0);
									updateHistory(sd, cResults, sms.theirNumber, tableName, existingInstanceId, smsNumber.surveyIdent, msg,
											RecordEventManager.INBOUND_MESSAGE);
								} else {
									// Build response message
									caseList.append("#")
										.append(existingPrikey)
										.append(": ")
										.append(getFirstMessage(cResults, messageColumn, tableName, existingPrikey))
										.append(", ");
								}
								messageWritten = true;
							}
							rs.close();
							
							if(count > 1) {
								/*
								 * Send response to the user who sent the message
								 * Ask them to clarify which case they want to update
								 */
								String response_msg = smsNumber.mcMsg;
								if(response_msg != null && response_msg.trim().length() > 0) {
									response_msg = response_msg.replace("%s1", String.valueOf(count));
									response_msg = response_msg.replace("%s2", caseList.toString());
									
									/*
									 * Send message
									 */
									ConversationManager conversationMgr = new ConversationManager(localisation, tz);
									MessageResponse response = conversationMgr.sendMessage(vonageClient,
											sms.channel,
											sms.ourNumber,
											sms.theirNumber,
											response_msg.toString());
									if(response.getMessageUuid() == null) {
										throw new ApplicationException("Failed to send response message \"" +
												response_msg.toString() + "\" to " + sms.theirNumber);
									}
								}
							}
						}
					}	
				}
				
				/*
				 * 3. Create a new entry if an existing entry was not updated
				 */
				if(!messageWritten) {

					/*
					 * Create new entry
					 */
					log.info("Create new entry ");
					existingInstanceId = instanceid;
					StringBuilder sql = new StringBuilder("insert into ")
							.append(tableName)
							.append(" (_user, instanceid, _thread, _survey_notes")
							.append(",").append(theirNumberColumn)
							.append(",").append(messageColumn)
							.append(") values(?, ?, ?, ?, ?, ?)");
					pstmt = cResults.prepareStatement(sql.toString());
					pstmt.setString(1, sms.ourNumber);
					pstmt.setString(2,  instanceid);
					pstmt.setString(3,  instanceid);	// thread
					pstmt.setString(4, comment);
					pstmt.setString(5, sms.theirNumber);
					pstmt.setString(6, gson.toJson(getMessageText(sms, null)));
					log.info("Create new sms case: " + pstmt.toString());
					pstmt.executeUpdate();
					
					updateHistory(sd, cResults, sms.theirNumber, tableName, existingInstanceId, smsNumber.surveyIdent, msg,
							RecordEventManager.INBOUND_MESSAGE);
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
	 * Remove message details from existing
	 */
	public ArrayList<ConversationItemDetails> removeMessageText(int idx, ArrayList<ConversationItemDetails> current) {
		ArrayList<ConversationItemDetails> conversation = null;
		
		if(current != null) {
			conversation = current;
			conversation.remove(idx);
		} 
		
		return conversation;
	}
	
	/*
	 * Update existing entry
	 *  Items can be added or removed
	 *  If removed the removed item details are returned
	 */
	private ConversationItemDetails updateExistingEntry(Connection sd,
			Connection cResults, 
			boolean add,
			ConversationItemDetails sms,
			int existingPrikey,
			String messageColumn,
			String tableName,
			int idx						// Index of conversation item if it is to be removed
			) throws SQLException {
		
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmt = null;
		ConversationItemDetails removedItem = null;
		
		try {
			log.info("Update existing entry with prikey: " + existingPrikey);
			ArrayList<ConversationItemDetails> currentConv = getConversation(cResults, messageColumn,
					tableName, existingPrikey);
			
			// Create update statement
			StringBuilder sql = new StringBuilder("update ")
					.append(tableName)
					.append(" set ")
					.append(messageColumn)
					.append(" = ? where prikey = ?");
			pstmt = cResults.prepareStatement(sql.toString());
			
			if(add) {
				pstmt.setString(1, gson.toJson(getMessageText(sms, currentConv)));
			} else {
				if(idx < currentConv.size()) {
					removedItem = currentConv.get(idx);
					pstmt.setString(1, gson.toJson(removeMessageText(idx, currentConv)));
				} else {
					pstmt.setString(1, gson.toJson(currentConv));
				}
			}
			pstmt.setInt(2, existingPrikey);	
			log.info("Update existing sms case: " + pstmt.toString());
			int c = pstmt.executeUpdate();
			if(c == 0) {
				log.info("Tried to update an existing case but nothing was updated");
			} 
			
		} finally {
			if(pstmtGet != null) {try {pstmtGet.close();} catch (Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
		
		return removedItem;
	}
	
	/*
	 * Get the text of the first message sent for this case
	 * Remove hash references from the message
	 */
	private String getFirstMessage(Connection cResults, 		
			String messageColumn,
			String tableName,
			int existingPrikey) throws SQLException {
		
		String firstMessage = null;
		
		ArrayList<ConversationItemDetails> currentConv = getConversation(cResults, messageColumn,
					tableName, existingPrikey);
	
		if(currentConv != null && currentConv.size() > 0) {
			firstMessage = currentConv.get(0).msg;
			firstMessage = firstMessage.replaceAll("#[0-9]*", "");
 		}
		
		return firstMessage;
	}
	
	private ArrayList<ConversationItemDetails> getConversation(Connection cResults, 
			String messageColumn,
			String tableName,
			int existingPrikey) throws SQLException {
		
		ArrayList<ConversationItemDetails> conv = null;
		PreparedStatement pstmtGet = null;
		try {

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
					conv = gson.fromJson(currentConvString, type);
				}
			}
		} finally {
			if(pstmtGet != null) {try {pstmtGet.close();} catch (Exception e) {}}
		}
		return conv;
	}
	
	private SMSNumber getNumber(ResultSet rs) throws SQLException {
		return new SMSNumber(rs.getString("element_identifier"),
				rs.getString("our_number"),
				rs.getString("survey_ident"),
				rs.getString("their_number_question"),
				rs.getString("message_question"),
				rs.getInt("o_id"),
				rs.getString("channel"),
				rs.getString("mc_msg"));
	}
	
	/*
	 * Update the history for the record
	 */
	private void updateHistory(Connection sd, Connection cResults, String user,
			String tableName,
			String existingInstanceId,
			String surveyIdent,
			String msg,
			String event) throws SQLException {
		
		RecordEventManager rem = new RecordEventManager();
		rem.writeEvent(sd, cResults, 
				event, 
				RecordEventManager.STATUS_SUCCESS,
				user, 
				tableName, 
				existingInstanceId, 
				null, 					// Change object
				null, 					// Task object
				null,					// Message object
				null,					// Notification object
				msg, 					// Description
				0, 						// sID legacy
				surveyIdent,			// Survey Ident
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
	 * return minus 1 for not found
	 */
	private int getReference(Connection cResults, String msg, String tableName) throws SQLException {
		int caseReference = -1;
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
		if(caseReference > 0) {
			caseReference = GeneralUtilityMethods.getLatestPrikey(cResults, tableName, caseReference);
		}
		return caseReference;
	}
	
	public ConversationItemDetails removeConversationItemFromRecord(Connection sd, Connection cResults, int sId, int idx, 
			String instanceid,
			String user) throws SQLException {
		
		ConversationItemDetails item = null;
		PreparedStatement pstmt = null;
		
		try {
			String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
			String messageColumn = GeneralUtilityMethods.getConversationColumn(sd, sId);
			int prikey = GeneralUtilityMethods.getPrikey(cResults, tableName, instanceid);
			String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			
			/*
			 * Delete the existing conversation item
			 * Returns a copy of the item
			 */
			item = updateExistingEntry(sd, cResults, false, null, prikey, messageColumn, tableName, idx);
			
			/*
			 * Update the history
			 */
			String msg = localisation.getString("msg_new_case");
			msg = msg.replaceAll("%s1",  item.msg);
			updateHistory(sd, cResults, user, tableName, instanceid, surveyIdent, msg, RecordEventManager.NEW_CASE);
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		return item;

	}
}


