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
import com.vonage.client.VonageClient;

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
	public void applyOutbound(Connection sd, Connection cResults, 
			String queueName,
			String serverName, 
			String basePath, 
			String urlprefix,
			String attachmentPrefix,
			String hyperlinkPrefix,
			VonageClient vonageClient) {

		ResultSet rs = null;
		PreparedStatement pstmtGetMessages = null;
		PreparedStatement pstmtConfirm = null;

		String sqlConfirm = "update message "
				+ "set processed_time = now(), "
				+ "status = ?, "
				+ "queued = false "
				+ "where id = ? ";

		// dequeue
		String sql = "delete "
				+ "from message_queue q "
				+ "where q.element_identifier = "
				+ "(select q_inner.element_identifier "
				+ "from message_queue q_inner "
				+ "order by q_inner.time_inserted ASC "
				+ "for update skip locked "
				+ "limit 1) "
				+ "returning q.m_id, q.o_id, q.topic, q.description, q.data";
		try {

			pstmtGetMessages = sd.prepareStatement(sql);
			pstmtConfirm = sd.prepareStatement(sqlConfirm);

			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			boolean loop = true;
			while(loop) {
				rs = pstmtGetMessages.executeQuery();
				if (rs.next()) {
	
					int id = rs.getInt("m_id");
					int o_id = rs.getInt("o_id");
					String topic = rs.getString("topic");
					String description = rs.getString("description");
					String data = rs.getString("data");
					
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
					
					GeneralUtilityMethods.log(log, "++++++ Message: " + topic + " " + description + " : " + data, 
							queueName, String.valueOf(id));
	
					String status = "success";
					ArrayList<String> unsubscribedList = new ArrayList<>();
					
				
					/*
					 * Record that the message is being processed
					 * After this point it will not be processed again even if it fails unless there is manual intervention
					 */
					pstmtConfirm.setString(1, "Sending");
					pstmtConfirm.setInt(2, id);
					pstmtConfirm.executeUpdate();
					
					if(topic.equals(NotificationManager.TOPIC_SUBMISSION) 
							|| topic.equals(NotificationManager.TOPIC_CM_ALERT)
							|| topic.equals(NotificationManager.TOPIC_SERVER_CALC)) {
						/*
						 * A submission notification is a notification associated with a record of data
						 */
						SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);
				
						NotificationManager nm = new NotificationManager(localisation);
						try {
							nm.processSubmissionNotification(
									sd, 
									cResults, 
									vonageClient,
									organisation, 
									queueName,
									tz,
									msg,
									id,
									topic,
									true,		// create pending if needed
									serverName,
									basePath,
									urlprefix,
									attachmentPrefix,
									hyperlinkPrefix
									); 
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
							nm.writeToLog(sd, organisation.id, msg.pId, 
									GeneralUtilityMethods.getSurveyId(sd, msg.survey_ident), 
									organisation.name, status, 
									e.getMessage(), id, msg.target);
						}
						
					} else if(topic.equals(NotificationManager.TOPIC_REMINDER)) {
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
								true,		// create pending if needed
								serverName,
								basePath
								); 
						
					} else if(topic.equals(NotificationManager.TOPIC_EMAIL_TASK)) {
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
						
						
					} else if(topic.equals(NotificationManager.TOPIC_MAILOUT)) {
						
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
						
						
					} else if(topic.equals(NotificationManager.TOPIC_PERIODIC)) {
						
						NotificationManager nm = new NotificationManager(localisation);
						
						SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);	
							
						GeneralUtilityMethods.log(log, "--------- Starting periodic notifications for " + data,
								queueName, String.valueOf(id));
						try {
							nm.processPeriodicNotification(
									sd, 
									cResults, 
									organisation, 
									msg,
									id,
									topic,
									true,		// create pending if needed
									serverName,
									basePath,
									urlprefix,
									attachmentPrefix
									); 
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
							lm.writeLogOrganisation(sd, o_id, localisation.getString("pn"), LogManager.NOTIFICATION_ERROR, e.getMessage(), 0);
						}
						GeneralUtilityMethods.log(log, "--------- Periodic notifications processed", queueName, String.valueOf(id));
						
						
					} else {
						// Assume a direct email to be processed immediately
	
						log.info("+++++++++ opt in +++++++++ Direct Email");
						EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, null, o_id);
						if (emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {
		
							// Set the subject
							String subject = "";
							String from = "";
		
							subject += localisation.getString("c_message");
							try {
								PeopleManager pm = new PeopleManager(localisation);
								EmailManager em = new EmailManager(localisation);
								InternetAddress[] emailArray = InternetAddress.parse(topic);
								SubscriptionStatus subStatus = null;
								for(InternetAddress ia : emailArray) {
									
									subStatus = pm.getEmailKey(sd, o_id, ia.getAddress());
									
									if(subStatus.unsubscribed) {
										unsubscribedList.add(ia.getAddress());		// Person has unsubscribed
									} else {
										StringBuilder content = new StringBuilder("");
										
										content.append(localisation.getString("email_ian"));
										content.append(" https://");
										content.append(serverName);
										content.append(". ");
	
										content.append(localisation.getString("email_dnr"));
										content.append("\n\n");
										
										em.sendEmailHtml(
												ia.getAddress(), 	// email
												"bcc",
												subject, 
												content.toString(), 
												null,
												null,
												emailServer,
												serverName, 
												subStatus.emailKey,
												localisation,
												null,
												organisation.getAdminEmail(), 
												null,
												GeneralUtilityMethods.getNextEmailId(sd)
												);
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
					GeneralUtilityMethods.log(log, pstmtConfirm.toString(), queueName, String.valueOf(id));
					pstmtConfirm.executeUpdate();
	
				} else {
					loop = false;
					//GeneralUtilityMethods.log(log, "Message queue empty", queueName, null);
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
	 * Apply any device messages - these are done as a single queue
	 */
	public void applyDeviceMessages(Connection sd, Connection cResults, 
			String serverName,  
			String awsProperties) {

		ResultSet rs = null;
		PreparedStatement pstmtGetMessages = null;
		PreparedStatement pstmtConfirm = null;
		
		HashMap<Integer, TaskMessage> changedTasks = new HashMap<> ();
		HashMap<Integer, SurveyMessage> changedSurveys = new HashMap<> ();
		HashMap<Integer, ProjectMessage> changedProjects = new HashMap<> ();
		HashMap<String, OrgResourceMessage> changedResources = new HashMap<> ();
		HashMap<String, String> usersImpacted =   new HashMap<> ();

		String sqlConfirm = "update message "
				+ "set processed_time = now(), "
				+ "status = ?, "
				+ "queued = false "
				+ "where id = ? ";

		// Get the messages that can trigger a message to a device
		String sql = "select id, "
				+ "topic, "
				+ "data "
				+ "from message "
				+ "where outbound "
				+ "and processed_time is null "
				+ "and (topic = 'task' or topic = 'survey' or topic = 'user' or topic = 'project' or topic = 'resource') ";
		try {

			pstmtGetMessages = sd.prepareStatement(sql);
			pstmtConfirm = sd.prepareStatement(sqlConfirm);

			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();			

			GeneralUtilityMethods.log(log, "zzzzzzzzzzzzzzz: " + pstmtGetMessages.toString(), "device_message", null);

			rs = pstmtGetMessages.executeQuery();
			while (rs.next()) {
	
				int id = rs.getInt("id");
				String topic = rs.getString("topic");
				String data = rs.getString("data");
								
				/*
				 * Record that the message is being processed
				 * After this point it will not be processed again even if it fails unless there is manual intervention
				 */
				pstmtConfirm.setString(1, "success");
				pstmtConfirm.setInt(2, id);
				pstmtConfirm.executeUpdate();
					
				if(topic.equals(NotificationManager.TOPIC_TASK)) {
					TaskMessage tm = gson.fromJson(data, TaskMessage.class);					
					changedTasks.put(tm.id, tm);					
				} else if(topic.equals(NotificationManager.TOPIC_SURVEY)) {						
					SurveyMessage sm = gson.fromJson(data, SurveyMessage.class);
					changedSurveys.put(sm.id, sm);			
				} else if(topic.equals(NotificationManager.TOPIC_USER)) {
					UserMessage um = gson.fromJson(data, UserMessage.class);		
					usersImpacted.put(um.ident, um.ident);		
				} else if(topic.equals(NotificationManager.TOPIC_PROJECT)) {
					ProjectMessage pm = gson.fromJson(data, ProjectMessage.class);	
					changedProjects.put(pm.id, pm);	
				} else if(topic.equals(NotificationManager.TOPIC_RESOURCE)) {
					OrgResourceMessage orm = gson.fromJson(data, OrgResourceMessage.class);		
					changedResources.put(orm.resourceName, orm);			
				} 
					
			}
			
			/*
			 * Device notifications have been accumulated to an array so that duplicates can be eliminated
			 * Process these now
			 *
			 * Disable temporarily until this is fixed
			 */
			// Get a list of users impacted by task changes without duplicates
			for(Integer taskId : changedTasks.keySet()) {
				ArrayList<String> users = getTaskUsers(sd, taskId);
				for(String user : users) {
					usersImpacted.put(user, user);	
					GeneralUtilityMethods.log(log, "zzzzzzzzzzzzzzz: task change users: " + user, "device_message", null);
				}				
			}
						
			// Get a list of users impacted by survey changes without duplicates
			for(Integer sId : changedSurveys.keySet()) {
				ArrayList<String> users = getSurveyUsers(sd, sId);
				for(String user : users) {
					usersImpacted.put(user, user);
					GeneralUtilityMethods.log(log, "zzzzzzzzzzzzzzz: survey change users: " + user, "device_message", null);
				}				
			}
			
			// Get a list of users impacted by project changes without duplicates
			for(Integer pId : changedProjects.keySet()) {
				ArrayList<String> users = getProjectUsers(sd, pId);
				for(String user : users) {
					usersImpacted.put(user, user);
					GeneralUtilityMethods.log(log, "zzzzzzzzzzzzzzz: project change users: " + user, "device_message", null);
				}				
			}
			
			// Get a list of users impacted by  resource changes
			for(String fileName : changedResources.keySet()) {
				ArrayList<String> users = GeneralUtilityMethods.getResourceUsers(sd, fileName, changedResources.get(fileName).orgId);
				for(String user : users) {
					usersImpacted.put(user, user);
					GeneralUtilityMethods.log(log, "zzzzzzzzzzzzzzz: resources change users: " + user, "device_message", null);
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
	 * These are stored in the pending queue until the user opts in to receive them
	 */
	public void applyPendingEmailMessages(Connection sd, 
			Connection cResults, 
			String serverName, 
			String basePath,
			String urlprefix,
			String attachmentPrefix,
			String hyperlinkPrefix) {

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
				+ "order by id asc "
				+ "limit 200";	// Send in order

		String sqlConfirm = "update pending_message set processed_time = now(), status = ? where id = ?; ";

		try {

			pstmtGetMessages = sd.prepareStatement(sqlGetMessages);
			pstmtConfirm = sd.prepareStatement(sqlConfirm);

			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
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
				if(topic.equals(NotificationManager.TOPIC_SUBMISSION) 
						|| topic.equals(NotificationManager.TOPIC_CM_ALERT)
						|| topic.equals(NotificationManager.TOPIC_SERVER_CALC)) {
					SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);
					email = msg.user;
					
					NotificationManager nm = new NotificationManager(localisation);
					nm.processSubmissionNotification(
							sd, 
							cResults, 
							null,		// Should be no pending SMS messages hence Vonage client is not set
							organisation, 
							null,
							tz,
							msg,
							messageId,
							topic,
							false,		// Do not create pending
							serverName,
							basePath,
							urlprefix,
							attachmentPrefix,
							hyperlinkPrefix
							); 
					
				} else if(topic.equals(NotificationManager.TOPIC_REMINDER)) {
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
							false,		// Do not create pending
							serverName,
							basePath
							); 
					
				} else if(topic.equals(NotificationManager.TOPIC_EMAIL_TASK)) {
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
					
					
				} else if(topic.equals(NotificationManager.TOPIC_MAILOUT)) {
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
					
				} else if(topic.equals(NotificationManager.TOPIC_PERIODIC)) {
					
					NotificationManager nm = new NotificationManager(localisation);
					
					
					SubmissionMessage msg = gson.fromJson(data, SubmissionMessage.class);	
					email = msg.user;
					
					nm.processPeriodicNotification(
							sd, 
							cResults, 
							organisation, 
							msg,
							id,
							topic,
							false,		// Do not create pending
							serverName,
							basePath,
							urlprefix,
							attachmentPrefix
							); 
					
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
				+ "and ((s.ident not in (select survey_ident from survey_role where enabled = true)) or "
				+ " (s.ident in (select sr.survey_ident from users ux, user_role ur, survey_role sr "
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
