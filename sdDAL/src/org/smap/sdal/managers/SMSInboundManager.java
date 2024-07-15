package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.SMSDetails;
import org.smap.sdal.model.SubscriberEvent;

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
 * Manage inbound SMS message
 */
public class SMSInboundManager {
	
	private static Logger log =
			 Logger.getLogger(SMSInboundManager.class.getName());
	
	private static LogManager lm = new LogManager();		// Application log
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create(); 
	
	/*
	 * Write a log entry that includes the survey id
	 */
	public void saveMessage(
			Connection sd,
			SMSDetails sms,
			String serverName)  {
		
		String sql = "insert into upload_event ("
				+ "upload_time,"
				+ "submission_type, "
				+ "payload, "
				+ "server_name,"
				+ "status,"
				+ "s_id) "
				+ "values (now(), 'SMS', ?, ?, 'success', 0);";

		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, gson.toJson(sms));
			pstmt.setString(2, serverName);
			
			log.info("----- new sms " + pstmt.toString());
			pstmt.executeUpdate();
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	

	public void processMessage(Connection sd, SMSDetails sms, SubscriberEvent se) {
		
		String surveyIdent = null;
		
		String sql = "select "
				+ "survey_ident "
				+ "from sms_number "
				+ "where to_number = ?";
		PreparedStatement pstmt = null;
		
		try {
			
			/*
			 * Get destination for the SMS
			 */		
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sms.toNumber);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				surveyIdent = rs.getString("survey_ident");
				
				/*
				 * Write log entry
				 */
				int sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
				lm.writeLog(sd, sId, sms.fromNumber, LogManager.SMS, se.getStatus() + " : " 
						+ (se.getReason() == null ? "" : se.getReason()) + " : ", 0, null);
				
			} else {
				se.setStatus("error");
				se.setReason("SMS Inbound Number not found.  This number will need to be added to the numbers supported by the system before SMS messages to it can be processed.");
			}
			
		} catch (Exception e) {
			se.setStatus("error");
			se.setReason(e.getMessage());
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}	

	}
}


