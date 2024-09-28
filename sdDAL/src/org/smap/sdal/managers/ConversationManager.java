package org.smap.sdal.managers;

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

import org.smap.sdal.Utilities.AdvisoryLock;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
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
public class ConversationManager {
	
	public static String SMS_TYPE = "SMS";
	public static String FORM_TYPE = "Form";
	
	private static Logger log =
			 Logger.getLogger(ConversationManager.class.getName());
	
	private static LogManager lm = new LogManager();		// Application log
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create(); 
	
	private String sqlGetNumber = "select "
			+ "element_identifier,"
			+ "our_number,"
			+ "survey_ident,"
			+ "their_number_question,"
			+ "message_question,"
			+ "o_id "
			+ "from sms_number ";
	
	private ResourceBundle localisation;
	private String tz;
	
	public ConversationManager(ResourceBundle l, String tz) {
		this.localisation = l;
		this.tz = tz;
	}
	

	/*
	 * Write a conversation update to the results table
	 */
	public void writeConversationToResults(Connection sd, 
			Connection cResults,
			String instanceid,
			String surveyIdent,
			String ourAddress,		// number or email
			String theirAddress,	// number or email
			boolean inbound,
			String content) throws Exception {
		
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmt = null;
		
		try {
			
			/*
			 * Update entry
			 */
			SMSManager smsMgr = new SMSManager(localisation, tz);
			SMSDetails msg = new SMSDetails(theirAddress, ourAddress, content, false, new Timestamp(System.currentTimeMillis()));
			
			String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, surveyIdent);
			int sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
			String messageColumn = GeneralUtilityMethods.getConversationColumn(sd, sId);	
			
			log.info("Update existing entry with instanceId: " + instanceid);
			ArrayList<SMSDetails> currentConv = null;
			Type type = new TypeToken<ArrayList<SMSDetails>>() {}.getType();
			StringBuilder sqlGet = new StringBuilder("select ")
					.append(messageColumn)
					.append(" from ")
					.append(tableName)
					.append(" where instanceid = ?");
			pstmtGet = cResults.prepareStatement(sqlGet.toString());
			pstmtGet.setString(1, instanceid);
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
			hEntry = hEntry.replaceAll("%s1",  msg.msg);
			hEntry = hEntry.replaceAll("%s2", theirAddress);
			RecordEventManager rem = new RecordEventManager();
			rem.writeEvent(sd, cResults, 
						inbound ? RecordEventManager.INBOUND_MESSAGE : RecordEventManager.OUTBOUND_MESSAGE, 
						RecordEventManager.STATUS_SUCCESS,
						theirAddress, 
						tableName, 
						instanceid, 
						null, 					// Change object
						null, 					// Task object
						null,					// Notification object
						hEntry, 					// Description
						0, 						// sID legacy
						surveyIdent,			// Survey Ident
						0,
						0);	
			
		} finally {
			
			if(pstmtGet != null) {try {pstmtGet.close();} catch (Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
	}
	
	
	
	
}


