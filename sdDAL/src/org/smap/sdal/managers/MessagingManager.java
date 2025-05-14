package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.EmailTaskMessage;
import org.smap.sdal.model.MailoutMessage;
import org.smap.sdal.model.OrgResourceMessage;
import org.smap.sdal.model.ProjectMessage;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.sdal.model.SurveyMessage;
import org.smap.sdal.model.TaskMessage;
import org.smap.sdal.model.UserMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * 
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 * 
 ******************************************************************************/

/*
 * Manage the table that stores details on the forwarding of data onto other
 * systems
 */
public class MessagingManager {

	private static Logger log = Logger.getLogger(MessagingManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	private ResourceBundle localisation;
	
	public MessagingManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Create a message resulting from a change to a task
	 */
	public void taskChange(Connection sd, int taskId) throws SQLException {
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		String data = gson.toJson(new TaskMessage(taskId));		
		int oId = GeneralUtilityMethods.getOrganisationIdForTask(sd, taskId);	
		if(oId >= 0) {
			createMessage(sd, oId, NotificationManager.TOPIC_TASK, null, data);
		}
	}
	
	/*
	 * Create a message resulting from a change to a user
	 */
	public void userChange(Connection sd, String userIdent) throws SQLException {
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		String data = gson.toJson(new UserMessage(userIdent));		
		int oId = GeneralUtilityMethods.getOrganisationId(sd, userIdent);	
		if(oId >= 0) {
			createMessage(sd, oId, NotificationManager.TOPIC_USER, null, data);
		}
	}
	
	/*
	 * Create a message resulting from a change to a form
	 */
	public void surveyChange(Connection sd, int sId, int linkedId) throws SQLException {
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		SurveyMessage sm = new SurveyMessage(sId);
		sm.linkedSurveyId = linkedId;
		String data = gson.toJson(sm);
		int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);	
		if(oId >= 0) {
			createMessage(sd, oId, NotificationManager.TOPIC_SURVEY, null, data);
		}
	}
	
	/*
	 * Create a message resulting from a change to a shared resource
	 */
	public void resourceChange(Connection sd, int oId, String fileName) throws SQLException {
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();

		OrgResourceMessage sm = new OrgResourceMessage(oId, fileName);
		String data = gson.toJson(sm);		
		if(oId >= 0) {
			createMessage(sd, oId, "resource", null, data);
		}
	}
	
	/*
	 * Create a message resulting from a change to a project
	 */
	public void projectChange(Connection sd, int pId, int oId) throws SQLException {
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		ProjectMessage sm = new ProjectMessage(pId);
		String data = gson.toJson(sm);
		if(oId >= 0) {
			createMessage(sd, oId, "project", null, data);
		}
	}
	
	/*
	 * Create a new message
	 */
	public void createMessage(Connection sd, int oId, String topic, String msg, String data) throws SQLException {
		
		String sqlMsg = "insert into message" 
				+ "(o_id, topic, description, data, outbound, created_time) "
				+ "values(?, ?, ?, ?, 'true', now())";
		PreparedStatement pstmtMsg = null;
		
		try {
			pstmtMsg = sd.prepareStatement(sqlMsg);
			pstmtMsg.setInt(1, oId);
			pstmtMsg.setString(2, topic);
			pstmtMsg.setString(3, msg);
			pstmtMsg.setString(4, data);
			log.info("Add message: " + pstmtMsg.toString());
			pstmtMsg.executeUpdate();
		} finally {

			try {if (pstmtMsg != null) {	pstmtMsg.close();}} catch (SQLException e) {}

		}
	}
	
	/*
	 * Save a message as pending
	 */
	public void saveToPending(Connection sd, int oId, String email, String topic, 
			SubmissionMessage msg,
			EmailTaskMessage tMsg,
			MailoutMessage moMsg,
			Timestamp optInSent,
			String adminEmail,
			EmailServer emailServer,
			String emailKey,
			boolean createPending,
			String scheme,
			String server,
			int messageId) throws Exception {
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		String msgString = null;
	
		if(msg != null) {
			/*
			 * Copy message to a new object so we don't affect processing of the original message
			 * which may have multiple email addresses
			 */
			SubmissionMessage pendingMsg = new SubmissionMessage(msg);
			
			/*
			 * There is only one email address associated with this message
			 * Remove all others
			 */
			pendingMsg.emails = new ArrayList<>();
			pendingMsg.emails.add(email);	// Set email list to one entry only
			pendingMsg.clearEmailQuestions();
			
			msgString = gson.toJson(pendingMsg);
		} else if (tMsg != null) {
			msgString = gson.toJson(tMsg);		// Only one email in a task message
		} else if (moMsg != null) {
			msgString = gson.toJson(moMsg);		// Only one email in a mailout message message
		}
		
		String sql = "insert into pending_message" 
				+ "(o_id, email, topic, data, message_id, created_time) "
				+ "values(?, ?, ?, ?, ?, now())";
		PreparedStatement pstmt = null;
		
		String sqlTrim = "update pending_message "
				+ "set processed_time = now(), "
				+ "status = 'auto_cancel' "
				+ "where id = ?";
		PreparedStatement pstmtTrim = null;
		
		String sqlOld = "select id from pending_message "
				+ "where o_id = ? "
				+ "and email = ? "
				+ "and processed_time is null "
				+ "order by id desc";
		try {
			
			/*
			 * Write the modified message to pending
			 */
			if(createPending && msgString != null && messageId > 0) {
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, oId);
				pstmt.setString(2, email);
				pstmt.setString(3, topic);
				pstmt.setString(4, msgString);
				pstmt.setInt(5, messageId);
				log.info("Add pending message: " + pstmt.toString());
				pstmt.executeUpdate();	
				
				// Cancel all but the last 5
				pstmtTrim = sd.prepareStatement(sqlTrim);
				
				try {if (pstmt != null) {	pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sqlOld);
				pstmt.setInt(1,oId);
				pstmt.setString(2, email);
				ResultSet rs = pstmt.executeQuery();
				
				int count = 0;
				while(rs.next()) {
					count++;
					if(count > 5) {
						pstmtTrim.setInt(1, rs.getInt(1));
						pstmtTrim.executeUpdate();
					}
				}
			}
			
			/*
			 * Send opt in email if one has not been sent
			 */
			if(optInSent == null) {
				sendOptinEmail(sd, oId, email, 
						adminEmail, emailServer, emailKey, scheme, server, true);
			}
		
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw e;
		} finally {
			try {if (pstmt != null) {	pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtTrim != null) {	pstmtTrim.close();}} catch (SQLException e) {}
		}
	}
	
	public void sendOptinEmail(
			Connection sd, 
			int oId, 
			String email, 
			String adminEmail,
			EmailServer emailServer,
			String emailKey,
			String scheme,
			String server,
			boolean sendEmail) throws Exception {
		
		EmailManager em = new EmailManager(localisation);
		
		PreparedStatement pstmt = null;
		try {
			if(sendEmail) {		// Sometimes a specific opt in email is not required
				
				String msg = localisation.getString("c_opt_in_content");
				msg = msg.replace("%s1", GeneralUtilityMethods.getOrganisationName(sd, oId) + " (" + server + ")");
				
				StringBuilder content = new StringBuilder("<p>")
						.append(msg)
						.append("</p>");
				
				content.append("<p>")
					.append(" <a href=\"").append(scheme).append("://").append(server)
					.append("/app/subscriptions.html?subscribe=yes&token=")
					.append(emailKey)
					.append("\">")
					.append("<button type='button' style='margin:auto;style:block'>")
					.append(localisation.getString("optin"))
					.append("</button>")
					.append("</a></p>");
				
				em.sendEmailHtml(
						null,
						GeneralUtilityMethods.getOrganisationName(sd, oId),
						null,
						null,
						email, 
						"bcc", 
						localisation.getString("c_opt_in_subject"), 
						content.toString(), 
						null, 
						null, 
						emailServer,
						server,
						emailKey,
						localisation,
						null,
						adminEmail,
						null,
						GeneralUtilityMethods.getNextEmailId(sd, null));
				
			} else {
				log.info("##### - Specific optin message does not need to be sent");
			}
			
			// Record that the opt in message has been sent
			String sqlDone = "update people "
					+ "set opted_in_sent = now(),"
					+ "opted_in_status = 'success',"
					+ "opted_in_status_msg = '',"
					+ "opted_in_count = opted_in_count + 1 "
					+ "where o_id = ? "
					+ "and email = ? ";
			pstmt = sd.prepareStatement(sqlDone);
			pstmt.setInt(1, oId);
			pstmt.setString(2, email.toLowerCase());
			log.info("Record opt in sent: " + pstmt.toString());
			pstmt.executeUpdate();
			
			String note = localisation.getString("optin_sent");
			note = note.replace("%s1", email);
			lm.writeLogOrganisation(sd, oId, email, LogManager.OPTIN, note, 0);
			
		} catch (Exception e) {
			// Record that the opt in message has not been sent
			String sqlDone = "update people "
					+ "set opted_in_sent = now(),"
					+ "opted_in_status = 'error', "
					+ "opted_in_count = opted_in_count + 1, "
					+ "opted_in_status_msg = ? "
					+ "where o_id = ? "
					+ "and email = ? ";
			
			try {if (pstmt != null) {	pstmt.close();}} catch (SQLException ex) {}
			pstmt = sd.prepareStatement(sqlDone);
			pstmt.setString(1, e.getMessage());
			pstmt.setInt(2, oId);
			pstmt.setString(3, email.toLowerCase());
			log.info("Record opt in send fail: " + pstmt.toString());
			pstmt.executeUpdate();
			
			String note = localisation.getString("optin_failed");
			note = note.replace("%s1", email);
			String err_msg = e.getMessage();
			if(err_msg == null) {
				err_msg = "";
			}
			note = note.replace("%s2", err_msg);
			lm.writeLogOrganisation(sd, oId, null, LogManager.OPTIN, note, 0);
			
			throw e;
		} finally {
			try {if (pstmt != null) {	pstmt.close();}} catch (SQLException ex) {}
		}
	}

	public void deleteTableEvents(Connection rel, String tableName) throws SQLException {
		
		String sql = "delete from case_alert_triggered where table_name = ?";
		String sql2 = "delete from server_calc_triggered where table_name = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = rel.prepareStatement(sql);
			pstmt.setString(1, tableName);
			log.info("Delete case alerts for a table: " + pstmt.toString());
			pstmt.executeUpdate();
			
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {};
			pstmt = rel.prepareStatement(sql2);
			pstmt.setString(1, tableName);
			log.info("Delete server calculate notifications for a table: " + pstmt.toString());
			pstmt.executeUpdate();
			
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {};
		}
		
	}
}
