package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.model.SMSDetails;
import org.smap.sdal.model.SMSNumber;

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
 * Manage messages
 */
public class SMSManager {
	
	private static Logger log =
			 Logger.getLogger(SMSManager.class.getName());
	
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
			log.info("Get SMS destination: " + pstmt.toString());
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
}


