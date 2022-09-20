package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import org.smap.notifications.interfaces.EmitDeviceNotification;
import org.smap.notifications.interfaces.S3AttachmentUpload;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.EmailTaskMessage;
import org.smap.sdal.model.MailoutMessage;
import org.smap.sdal.model.OrgResourceMessage;
import org.smap.sdal.model.SurveyMessage;
import org.smap.sdal.model.TaskMessage;
import org.smap.sdal.model.UserMessage;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.ProjectMessage;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.sdal.model.SubscriptionStatus;

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
 * Apply any outbound messages
 * This rightly belongs in MessagingManager however it also does legacy direct
 *  email sends and the required email classes are not available to all users
 *  of the MessagingManager
 */
public class MessagingManagerApply {

	private static Logger log = Logger.getLogger(MessagingManagerApply.class.getName());

	LogManager lm = new LogManager(); // Application log
	
	
	/*
	 * Apply any outbound messages
	 */
	public void applyOutbound(Connection sd, Connection cResults, String serverName, String basePath, int count, String awsProperties) {

		ResultSet rs = null;
		PreparedStatement pstmtGetMessages = null;
		PreparedStatement pstmtConfirm = null;
		
		HashMap<Integer, TaskMessage> changedTasks = new HashMap<> ();
		HashMap<Integer, SurveyMessage> changedSurveys = new HashMap<> ();
		HashMap<Integer, ProjectMessage> changedProjects = new HashMap<> ();
		HashMap<String, OrgResourceMessage> changedResources = new HashMap<> ();
		HashMap<String, String> usersImpacted =   new HashMap<> ();

		String sqlGetMessages = "select id, "
				+ "o_id, "
				+ "topic, "
				+ "description, "
				+ "data "
				+ "from message "
				+ "where outbound "
				+ "and processed_time is null";

		String sqlConfirm = "update message set processed_time = now(), status = ? where id = ?; ";

		try {

			pstmtGetMessages = sd.prepareStatement(sqlGetMessages);
			pstmtConfirm = sd.prepareStatement(sqlConfirm);

			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			//log.info("Get messages: " + pstmtGetMessages.toString());
			rs = pstmtGetMessages.executeQuery();
			while (rs.next()) {

				int id = rs.getInt(1);
				int o_id = rs.getInt(2);
				String topic = rs.getString(3);
				String description = rs.getString(4);
				String data = rs.getString(5);
				
				// Localisation
				Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, o_id);
				
				Locale locale = new Locale(organisation.locale);
				ResourceBundle localisation;
				try {
					localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				} catch(Exception e) {
					localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", locale);
				}
				
				String tz = "UTC";		// Default timezone to UTC
				if(organisation.timeZone != null) {
					tz = organisation.timeZone;
				}
				
				log.info("++++++ Message: " + topic + " " + description + " : " + data );

				String status = "success";
				ArrayList<String> unsubscribedList = new ArrayList<>();
				
			
				/*
				 * Record that the message is being processed
				 * After this point it will not be processed again even if it fails unless there is manual intervention
				 */
				pstmtConfirm.setString(1, "Sending");
				pstmtConfirm.setInt(2, id);
				pstmtConfirm.executeUpdate();
				
				if(topic.equals("task")) {
					TaskMessage tm = gson.fromJson(data, TaskMessage.class);
					
					changedTasks.put(tm.id, tm);
					
				} else if(topic.equals("survey")) {
					
					SurveyMessage sm = gson.fromJson(data, SurveyMessage.class);
					
					changedSurveys.put(sm.id, sm);
					
				} else if(topic.equals("user")) {
					UserMessage um = gson.fromJson(data, UserMessage.class);
					
					usersImpacted.put(um.ident, um.ident);
					
				} else if(topic.equals("project")) {
					ProjectMessage pm = gson.fromJson(data, ProjectMessage.class);
					
					changedProjects.put(pm.id, pm);
					
				} else if(topic.equals("resource")) {
					OrgResourceMessage orm = gson.fromJson(data, OrgResourceMessage.class);
					
					changedResources.put(orm.resourceName, orm);
					
				} else if(topic.equals("submission") || topic.equals("cm_alert")) {
					/*
					 * A submission notification is a notification associated with a record of data
					 */
					SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);
			
					NotificationManager nm = new NotificationManager(localisation);
					try {
						nm.processSubmissionNotification(
								sd, 
								cResults, 
								organisation, 
								tz,
								msg,
								id,
								topic,
								true		// create pending if needed
								); 
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
						nm.writeToLog(sd, organisation.id, msg.pId, 
								GeneralUtilityMethods.getSurveyId(sd, msg.survey_ident), 
								organisation.name, status, 
								e.getMessage(), id);
					}
					
				} else if(topic.equals("reminder")) {
					// Use SubmissionMessage structure - this may change
					SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);
			
					NotificationManager nm = new NotificationManager(localisation);
					nm.processReminderNotification(
							sd, 
							cResults, 
							organisation, 
							o_id,
							tz,
							msg,
							id,
							topic,
							true		// create pending if needed
							); 
					
				} else if(topic.equals("email_task")) {
					TaskManager tm = new TaskManager(localisation, tz);

					EmailTaskMessage msg = gson.fromJson(data, EmailTaskMessage.class);	
						
					tm.emailTask(
							sd, 
							cResults, 
							organisation, 
							msg,
							id,
							msg.user,
							basePath,
							"https",
							serverName,
							topic,
							true);		// create pending if needed
					
					
				} else if(topic.equals("mailout")) {
					
					MailoutManager mm = new MailoutManager(localisation);
					
					MailoutMessage msg = gson.fromJson(data, MailoutMessage.class);	
						
					mm.emailMailout(
							sd, 
							cResults, 
							organisation, 
							msg,
							id,
							msg.user,
							basePath,
							"https",
							serverName,
							topic,
							true);		// create pending if needed
					
					
				} else {
					// Assume a direct email to be processed immediately

					log.info("+++++++++ opt in +++++++++ Direct Email");
					EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, null, o_id);
					if (isValidEmail(topic) && 
							emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {
	
						// Set the subject
						String subject = "";
						String from = "";
	
						subject += localisation.getString("c_message");
						try {
							PeopleManager pm = new PeopleManager(localisation);
							EmailManager em = new EmailManager();
							InternetAddress[] emailArray = InternetAddress.parse(topic);
							SubscriptionStatus subStatus = null;
							for(InternetAddress ia : emailArray) {
								
								subStatus = pm.getEmailKey(sd, o_id, ia.getAddress());
								
								if(subStatus.unsubscribed) {
									unsubscribedList.add(ia.getAddress());		// Person has unsubscribed
								} else {
									em.sendEmail(
											ia.getAddress(), 
											null, 
											"notify", 
											subject, 
											description, 
											from, 
											null, 
											null, 
											null, 
											null, 
											null,
											null, 
											organisation.getAdminEmail(), 
											emailServer, 
											"https", 
											serverName, 
											subStatus.emailKey,
											localisation,
											organisation.server_description,
											organisation.name);
								}
							}
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
							status = "error";
						}
	
					} else {
						log.log(Level.SEVERE, "Error: Messaging: Attempt to do email notification but email server not set");
						status = localisation.getString("email_cs");
					}
					
				}
				// Set the final status
				if(unsubscribedList.size() > 0) {
					status += localisation.getString("c_unsubscribed") + ": " + String.join(",", unsubscribedList);
				}
				pstmtConfirm.setString(1, status);
				pstmtConfirm.setInt(2, id);
				log.info(pstmtConfirm.toString());
				pstmtConfirm.executeUpdate();

			}
			
			/*
			 * Device notifications have been accumulated to an array so that duplicates can be eliminated
			 * Process these now
			 */
			// Get a list of users impacted by task changes without duplicates
			for(Integer taskId : changedTasks.keySet()) {
				ArrayList<String> users = getTaskUsers(sd, taskId);
				for(String user : users) {
					usersImpacted.put(user, user);	
					log.info("zzzzzzzzzzzzzzz: task change users: " + user);
				}				
			}
						
			// Get a list of users impacted by survey changes without duplicates
			for(Integer sId : changedSurveys.keySet()) {
				ArrayList<String> users = getSurveyUsers(sd, sId);
				for(String user : users) {
					usersImpacted.put(user, user);
					log.info("zzzzzzzzzzzzzzz: survey change users: " + user);
				}				
			}
			
			// Get a list of users impacted by project changes without duplicates
			for(Integer pId : changedProjects.keySet()) {
				ArrayList<String> users = getProjectUsers(sd, pId);
				for(String user : users) {
					usersImpacted.put(user, user);
					log.info("zzzzzzzzzzzzzzz: project change users: " + user);
				}				
			}
			
			// Get a list of users impacted by  resource changes
			for(String fileName : changedResources.keySet()) {
				ArrayList<String> users = GeneralUtilityMethods.getResourceUsers(sd, fileName, changedResources.get(fileName).orgId);
				for(String user : users) {
					usersImpacted.put(user, user);
					log.info("zzzzzzzzzzzzzzz: resources change users: " + user);
				}				
			}
			
			// For each user send a notification to each of their devices
			if(awsProperties != null && usersImpacted.size() > 0) {
				EmitDeviceNotification emitDevice = new EmitDeviceNotification(awsProperties);
				for(String user : usersImpacted.keySet()) {
					emitDevice.notify(serverName, user);
				}	
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		} finally {
			try {if (pstmtGetMessages != null) {	pstmtGetMessages.close();}} catch (Exception e) {	}
			try {if (pstmtConfirm != null) {	pstmtConfirm.close();}} catch (Exception e) {}
		}

	}

	/*
	 * Send pending email messages
	 */
	public void applyPendingEmailMessages(Connection sd, Connection cResults, String serverName, String basePath) {

		ResultSet rs = null;
		PreparedStatement pstmtGetMessages = null;
		PreparedStatement pstmtConfirm = null;

		String sqlGetMessages = "select pm.id, "
				+ "pm.o_id, "
				+ "pm.topic, "
				+ "pm.description, "
				+ "pm.data,"
				+ "pm.message_id "
				+ "from pending_message pm, people p "
				+ "where pm.email = p.email "
				+ "and pm.o_id = p.o_id "
				+ "and p.unsubscribed = false "
				+ "and (p.opted_in = true or p.o_id in (select id from organisation where not send_optin)) "
				+ "and pm.processed_time is null "
				+ "order by id asc";	// Send in order

		String sqlConfirm = "update pending_message set processed_time = now(), status = ? where id = ?; ";

		try {

			pstmtGetMessages = sd.prepareStatement(sqlGetMessages);
			pstmtConfirm = sd.prepareStatement(sqlConfirm);

			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			rs = pstmtGetMessages.executeQuery();
			while (rs.next()) {

				int id = rs.getInt(1);
				int o_id = rs.getInt(2);
				String topic = rs.getString(3);
				String description = rs.getString(4);
				String data = rs.getString(5);
				int messageId = rs.getInt(6);
				
				// Localisation
				Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, o_id);
				
				Locale locale = new Locale(organisation.locale);
				ResourceBundle localisation;
				try {
					localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				} catch(Exception e) {
					localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", locale);
				}
				
				String tz = "UTC";		// Default timezone to UTC
				if(organisation.timeZone != null) {
					tz = organisation.timeZone;
				}
				
				log.info("++++++ Pending Message: " + topic + " " + description + " : " + data );

				String status = "success";
				
				/*
				 * Record that the message is being processed
				 * After this point it will not be processed again even if it fails unless there is manual intervention
				 */
				pstmtConfirm.setString(1, "Sending");
				pstmtConfirm.setInt(2, id);
				pstmtConfirm.executeUpdate();
				
				String email = null;
				if(topic.equals("submission")) {
					SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);
					email = msg.user;
					
					NotificationManager nm = new NotificationManager(localisation);
					nm.processSubmissionNotification(
							sd, 
							cResults, 
							organisation, 
							tz,
							msg,
							messageId,
							topic,
							false		// Do not create pending
							); 
					
				} else if(topic.equals("reminder")) {
					// Use SubmissionMessage structure - this may change
					SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);
					email = msg.user;
					
					NotificationManager nm = new NotificationManager(localisation);
					nm.processReminderNotification(
							sd, 
							cResults, 
							organisation, 
							o_id,
							tz,
							msg,
							messageId,
							topic,
							false		// Do not create pending
							); 
					
				} else if(topic.equals("email_task")) {
					TaskManager tm = new TaskManager(localisation, tz);				
					EmailTaskMessage msg = gson.fromJson(data, EmailTaskMessage.class);	
					email = msg.user;
					
					tm.emailTask(
							sd, 
							cResults, 
							organisation, 
							msg,
							messageId,
							msg.user,
							basePath,
							"https",
							serverName,
							topic,
							false);		// Do not create pending
					
					
				} else if(topic.equals("mailout")) {
					MailoutManager mm = new MailoutManager(localisation);

					MailoutMessage msg = gson.fromJson(data, MailoutMessage.class);	
					email = msg.email;
					
					mm.emailMailout(
							sd, 
							cResults, 
							organisation, 
							msg,
							id,
							msg.user,
							basePath,
							"https",
							serverName,
							topic,
							false);		// Do not create pending	
					
				} else {

					log.info("+++++++++ opt in send pending +++++++++ Unknown topic: " + topic);
					status = "error";
					
				}
				
				pstmtConfirm.setString(1, status);
				pstmtConfirm.setInt(2, id);
				log.info(pstmtConfirm.toString());
				pstmtConfirm.executeUpdate();

				String note = localisation.getString("mo_pending_sent");
				note = note.replace("%s1", topic);
				lm.writeLogOrganisation(sd, organisation.id, email, LogManager.OPTIN, note, 0);

			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		} finally {
			try {if (pstmtGetMessages != null) {	pstmtGetMessages.close();}} catch (Exception e) {	}
			try {if (pstmtConfirm != null) {	pstmtConfirm.close();}} catch (Exception e) {}
		}

	}
	
	/*
	 * Validate an email
	 */
	public boolean isValidEmail(String email) {
		boolean isValid = true;
		try {
			InternetAddress emailAddr = new InternetAddress(email);
			emailAddr.validate();
		} catch (AddressException ex) {
			isValid = false;
		}
		return isValid;
	}
	
	/*
	 * Upload files to s3
	 */
	public void uploadToS3(Connection sd, String basePath, int s3count) throws SQLException {
		
		String sql = "select id, filepath "
				+ "from s3upload "
				+ "where status = 'new' "
				+ "order by id asc "
				+ "limit 1000";	
		PreparedStatement pstmt = null;
		
		String sqlClean = "delete from s3upload "
				+ "where status = 'success' "
				+ "and processed_time < now() - interval '3 day'";
		PreparedStatement pstmtClean = null;
		
		String sqlDone = "update s3upload "
				+ "set status = ?, "
				+ "reason = ?, "
				+ "processed_time = now() "
				+ "where id = ?";
		PreparedStatement pstmtDone = null;
		
		try {
			pstmtDone = sd.prepareStatement(sqlDone);
			
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				
				String status;
				String reason = null;
				try {
					S3AttachmentUpload.put(basePath, rs.getString("filepath"));
					status = "success";
				} catch (Exception e) {
					status = "failed";
					reason = e.getMessage();					
				}	

				pstmtDone.setString(1, status);
				pstmtDone.setString(2, reason);
				pstmtDone.setInt(3, rs.getInt("id"));
				pstmtDone.executeUpdate();
			}
			
			/*
			 * Clean up old data
			 */
			if(s3count == 0) {
				pstmtClean = sd.prepareStatement(sqlClean);
				pstmtClean.executeUpdate();
			}
			
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtDone != null) {pstmtDone.close();}} catch (SQLException e) {}
			try {if (pstmtClean != null) {pstmtClean.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Get users of a survey
	 */
	ArrayList<String> getSurveyUsers(Connection sd, int sId) throws SQLException {
		
		ArrayList<String> users = new ArrayList<String> ();
		String sql = "select u.ident "
				+ "from users u, user_project up, survey s, user_group ug "
				+ "where u.id = up.u_id "
				+ "and u.id = ug.u_id "
				+ "and ug.g_id = 3 "			// enum
				+ "and s.p_id = up.p_id "
				+ "and s.s_id = ? and not u.temporary "
				+ "and ((s.s_id not in (select s_id from survey_role where enabled = true)) or "
				+ " (s.s_id in (select s_id from users ux, user_role ur, survey_role sr "
				+ "where ux.ident = u.ident and sr.enabled = true and ux.id = ur.u_id and ur.r_id = sr.r_id)))";

		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				users.add(rs.getString(1));
			}
		} finally {

			try {if (pstmt != null) {	pstmt.close();}} catch (SQLException e) {}

		}
		
		return users;
	}
	
	/*
	 * Get users of a project
	 */
	ArrayList<String> getProjectUsers(Connection sd, int pId) throws SQLException {
		
		ArrayList<String> users = new ArrayList<String> ();
		String sql = "select u.ident "
				+ "from users u, user_project up, user_group ug "
				+ "where u.id = up.u_id "
				+ "and u.id = ug.u_id "
				+ "and not u.temporary "
				+ "and ug.g_id = 3 "			// enum
				+ "and up.p_id = ?";

		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, pId);
			log.info("Get Project users: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				users.add(rs.getString(1));
			}
		} finally {

			try {if (pstmt != null) {	pstmt.close();}} catch (SQLException e) {}

		}
		
		return users;
	}
	

	
	
	/*
	 * Get users of a task
	 */
	ArrayList<String> getTaskUsers(Connection sd, int taskId) throws SQLException {
		
		ArrayList<String> users = new ArrayList<String> ();
		String sql = "select u.ident as user "
				+ "from tasks t, assignments a, users u "
				+ "where a.task_id = t.id " 
				+ "and a.assignee = u.id "
				+ "and not u.temporary "
				+ "and t.id = ? ";

		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, taskId);
			log.info("Get task users: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				users.add(rs.getString(1));
			}
		} finally {

			try {if (pstmt != null) {	pstmt.close();}} catch (SQLException e) {}

		}
		
		return users;
	}
}
