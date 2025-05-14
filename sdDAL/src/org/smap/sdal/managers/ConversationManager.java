package org.smap.sdal.managers;

import java.io.File;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.model.ConversationItemDetails;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.vonage.client.VonageClient;
import com.vonage.client.messages.MessageRequest;
import com.vonage.client.messages.MessageResponse;
import com.vonage.client.messages.MessagesClient;
import com.vonage.client.messages.sms.SmsTextRequest;
import com.vonage.client.messages.whatsapp.WhatsappTextRequest;

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
public class ConversationManager {
	
	public static String SMS_TYPE = "SMS";
	public static String FORM_TYPE = "Form";
	
	private static Logger log =
			 Logger.getLogger(ConversationManager.class.getName());
	
	LogManager lm = new LogManager(); // Application log
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create(); 
	
	private ResourceBundle localisation;
	private String tz;
	
	public ConversationManager(ResourceBundle l, String tz) {
		this.localisation = l;
		this.tz = tz;
	}
	

	/*
	 * Write a conversation update to the results table
	 */
	public int writeConversationToResults(Connection sd, 
			Connection cResults,
			String instanceid,
			String surveyIdent,
			String ourAddress,		// number or email
			String theirAddress,	// number or email
			boolean inbound,
			String channel,
			String content) throws Exception {
		
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmt = null;
		
		int prikey = 0;
		
		try {
			
			/*
			 * Update entry
			 */
			SMSManager smsMgr = new SMSManager(localisation, tz);
			ConversationItemDetails msg = new ConversationItemDetails(theirAddress, ourAddress, content, false, channel, new Timestamp(System.currentTimeMillis()));
			
			String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, surveyIdent);
			int sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
			String messageColumn = GeneralUtilityMethods.getConversationColumn(sd, sId);	
			
			if(tableName != null && messageColumn != null && sId > 0) {
				log.info("Update existing entry with instanceId: " + instanceid);
				ArrayList<ConversationItemDetails> currentConv = null;
				Type type = new TypeToken<ArrayList<ConversationItemDetails>>() {}.getType();
				StringBuilder sqlGet = new StringBuilder("select prikey, ")
						.append(messageColumn)
						.append(" from ")
						.append(tableName)
						.append(" where instanceid = ?");
				pstmtGet = cResults.prepareStatement(sqlGet.toString());
				pstmtGet.setString(1, instanceid);
				log.info("Get existing: " + pstmtGet.toString());
				ResultSet rsGet = pstmtGet.executeQuery();
				if(rsGet.next()) {
					prikey = rsGet.getInt("prikey");
					String currentConvString = rsGet.getString(2);
					if(currentConvString != null) {
						currentConv = gson.fromJson(currentConvString, type);
					}
				}
						
				// Create update statement
				StringBuilder sql = new StringBuilder("update ")
						.append(tableName)
						.append(" set ")
						.append(messageColumn)
						.append(" = ? where instanceid = ?");
				pstmt = cResults.prepareStatement(sql.toString());
				pstmt.setString(1, gson.toJson(smsMgr.getMessageText(msg, currentConv)));
				pstmt.setString(2, instanceid);			
					
				log.info("Process sms: " + pstmt.toString());
				pstmt.executeUpdate();
					
				/*
				 * Update the history for the record
				 */
				String hEntry = localisation.getString("msg_sms_received");
				hEntry = hEntry.replace("%s1",  msg.msg);
				hEntry = hEntry.replace("%s2", theirAddress);
				hEntry = hEntry.replace("%s3", msg.channel);
				RecordEventManager rem = new RecordEventManager();
				rem.writeEvent(sd, cResults, 
							inbound ? RecordEventManager.INBOUND_MESSAGE : RecordEventManager.OUTBOUND_MESSAGE, 
							RecordEventManager.STATUS_SUCCESS,
							theirAddress, 
							tableName, 
							instanceid, 
							null, 					// Change object
							null, 					// Task object
							null,					// Message object
							null,					// Notification object
							hEntry, 				// Description
							0, 						// sID legacy
							surveyIdent,			// Survey Ident
							0,
							0);	
			} else {
				log.info("Error: Conversation question not found");
			}
			
		} finally {
			
			if(pstmtGet != null) {try {pstmtGet.close();} catch (Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
		
		return prikey;
	}
	
	/*
	 * Send a message
	 */
	MessageResponse sendMessage(VonageClient vonageClient,
			String channel,
			String ourNumber,
			String toNumber,
			String msgText) {
		
		MessagesClient messagesClient = vonageClient.getMessagesClient(); // TODO check for null	
		
		if(channel != null && channel.equals("whatsapp")) {
			
			WhatsappTextRequest message = WhatsappTextRequest.builder()
					.from(ourNumber)
					.to(toNumber)		// TODO from number
					.text(msgText)
					.build();
			
			return messagesClient.sendMessage(message);
		} else {
		
			MessageRequest message = SmsTextRequest.builder()
					.from(ourNumber)
					.to(toNumber)		// TODO from number
					.text(msgText)
					.build();
		
			return messagesClient.sendMessage(message);
		}
	}
	
	/*
	 * Create a Vonage client object if the private key exists and application id is specified
	 */
	public VonageClient getVonageClient(Connection sd) throws ApplicationException, SQLException {
		
		VonageClient vonageClient = null;
		String privateKeyFile = ServerSettings.getBasePath() + "_bin/resources/properties/vonage_private.key";
		File vonagePrivateKey = new File(privateKeyFile);
		String vonageApplicationId = getVonageApplicationId(sd);
			
		if(vonagePrivateKey.exists() && vonageApplicationId != null && vonageApplicationId.trim().length() > 0) {
			log.info("Getting vonage client with application Id: " + vonageApplicationId + " file at: " + privateKeyFile);
			try {
				vonageClient = VonageClient.builder()
						.applicationId(vonageApplicationId)
						.privateKeyPath(vonagePrivateKey.getAbsolutePath())
						.build();
				log.info("Got vonage client");
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(),e);
				lm.writeLogOrganisation(sd, -1, null, LogManager.SMS, 
							"Cannot create vonage client" + " " + e.getMessage(), 0);
			}
		} else {
			String msg = "Cannot create vonage client. " 
					+ (!vonagePrivateKey.exists() ? " vonage_private.key was not found." : "")
					+ (vonageApplicationId == null ? " The vonage application Id was not found in settings." : "");
			
			if(vonageApplicationId != null && vonageApplicationId.trim().length() > 0) {
				// Set organisation id to -1 as this is an issue not related to an organisation			
				lm.writeLogOrganisation(sd, -1, null, LogManager.SMS, msg, 0);	
				log.info("Error setting up vonage client: " + msg);
			}
			
		}
		
		return vonageClient;
	}
	
	private String getVonageApplicationId(Connection sd) throws SQLException {
		String id = null;
		String sql = "select vonage_application_id from server";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				id = rs.getString(1);
			}
		} finally {
			if (pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return id;
	}
	
}


