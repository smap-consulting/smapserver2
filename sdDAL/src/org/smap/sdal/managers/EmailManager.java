package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.InternetAddress;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.SendEmailResponse;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.sdal.model.SubscriptionStatus;

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
 * Manage the sending of emails
 */
public class EmailManager {

	private static Logger log =
			Logger.getLogger(EmailManager.class.getName());

	private LogManager lm = new LogManager();		// Application log
	private ResourceBundle localisation;
	
	private HtmlSanitise sanitise = new HtmlSanitise();
	


	public EmailManager(ResourceBundle l) {
		localisation = l;
	}
	
	/*
	 * Send an email alert to an administrator
	 */
	public void alertAdministrator(Connection sd, int oId, String userIdent, 
			ResourceBundle localisation,
			String serverName,
			String subject,
			StringBuilder template,
			String type,
			String emailId) throws SQLException, ApplicationException {
				
		EmailServer emailServer = null;
		SubscriptionStatus subStatus = null;
		StringBuilder content = null;
		HashMap<String, String> customTokens = new HashMap<> ();
		
		if(!alertEmailSent(sd, oId, type)) {
			Organisation org = GeneralUtilityMethods.getOrganisation(sd, oId);
			template = template.append(" ").append(org.getEmailFooter());
			content = new StringBuilder(template.toString());
			
			if(org.admin_email != null) {
				emailServer = UtilityMethodsEmail.getEmailServer(sd, localisation, null, userIdent, oId);
				if(emailServer.smtpHost != null) {
					
					PeopleManager pm = new PeopleManager(localisation);
					subStatus = pm.getEmailKey(sd, oId, org.getAdminEmail());
					if(subStatus.unsubscribed) {
						// Person has unsubscribed
						String msg = localisation.getString("email_us");
						msg = msg.replaceFirst("%s1", org.getAdminEmail());
						log.info(msg);
					} else {
						
						// Add custom tokens
						if(org.limits != null) {
							String submissionLimit = "0";
							try {
								submissionLimit = String.valueOf(org.limits.get(LogManager.SUBMISSION));
							} catch (Exception e) {}
							customTokens.put("${submission_limit}", submissionLimit);
						}
						
								
						// Catch and log exceptions
						try {
							sendEmailHtml(
									org.name,
									org.getAdminEmail(), 
									"bcc", 
									subject, 
									content.toString(), 
									null, 
									null, 
									emailServer,
									serverName,
									subStatus.emailKey,
									localisation,
									customTokens,
									null,
									null,
									emailId);	
						} catch(Exception e) {
							lm.writeLogOrganisation(sd, oId, userIdent, LogManager.EMAIL, e.getMessage(), 0);
						}
					}
				}
			}
		}
	}
	
	/*
	 * Send an email 
	 * Create an opt in email message if the user has not subscribed
	 */
	public SendEmailResponse sendEmails(Connection sd, 
			Connection cResults, 
			Logger log,
			String emails, 
			Organisation organisation, 
			int surveyId, 
			String logContent,
			String docURL,
			String name,
			ArrayList<String> unsubscribedList,
			String filePath,
			String filename,
			int messageId,
			boolean createPending,
			String topic,
			String user,
			String serverName,
			String surveyName,
			String projectName,
			String subject,
			String from,
			String msgContent,
			String caseReference,
			String scheme,
			SubmissionMessage msg		// Used if saving to pending
			) throws Exception {
		
		SendEmailResponse resp = new SendEmailResponse();
		
		MessagingManager mm = new MessagingManager(localisation);
		EmailServer emailServer = UtilityMethodsEmail.getEmailServer(sd, localisation, null, user, organisation.id);
		if(emailServer != null) {

			if(emails.trim().length() > 0) {
				log.info("userevent: " + user + " sending email of '" + logContent + "' to " + emails);

				// Set the subject

				if(subject == null || subject.trim().length() == 0) {
					subject = "";
					if(serverName != null && serverName.contains("smap")) {
						subject = "Smap ";
					}
					subject += localisation.getString("c_notify");
				}

				if(from == null || from.trim().length() == 0) {
					from = "smap";
				}
				StringBuilder content = null;
				if(msgContent != null && msgContent.trim().length() > 0) {
					content = new StringBuilder(msgContent);
				} else if(organisation.default_email_content != null && organisation.default_email_content.trim().length() > 0){
					content = new StringBuilder(organisation.default_email_content);
				} else {
					content = new StringBuilder(localisation.getString("email_ian"))
							.append(" " + scheme + "://")
							.append(serverName)
							.append(". ");
				}

				if(docURL != null) {
					String url = scheme + "://" + serverName + docURL;
					if(content.toString().contains("${url}")) {					
						content = new StringBuilder(content.toString().replaceAll("\\$\\{url\\}", url));
					} else {
						content.append("<p style=\"color:blue;text-align:center;\">")
						.append("<a href=\"")
						.append(url)
						.append("\">")
						.append(surveyName)
						.append("</a>")
						.append("</p>");
					}
				}

				/*
				 * Create notification details for the monitor
				 */
				if(topic.equals(NotificationManager.TOPIC_PERIODIC)) {
					resp.notify_details = localisation.getString("msg_pn");
					resp.notify_details = resp.notify_details.replaceAll("%s2", name);
				} else {
					resp.notify_details = localisation.getString("msg_en");
					if(logContent != null) {
						resp.notify_details = resp.notify_details.replaceAll("%s2", logContent);
					} else {
						resp.notify_details = resp.notify_details.replaceAll("%s2", "-");
					}
				}
				resp.notify_details = resp.notify_details.replaceAll("%s1", emails);	
				if(surveyName != null) {
					resp.notify_details = resp.notify_details.replaceAll("%s3", surveyName);
				}
				if(projectName != null) {
					resp.notify_details = resp.notify_details.replaceAll("%s4", projectName);
				}

				log.info("+++ emailing to: " + emails + " docUrl: " + logContent + 
						" from: " + from + 
						" subject: " + subject +
						" type: " + emailServer.getClass() +
						" smtp_host: " + emailServer.smtpHost +
						" email_domain: " + emailServer.emailDomain);
				try {
					PeopleManager peopleMgr = new PeopleManager(localisation);
					InternetAddress[] emailArray = InternetAddress.parse(emails);
							
					for(InternetAddress ia : emailArray) {	
						SubscriptionStatus subStatus = peopleMgr.getEmailKey(sd, organisation.id, ia.getAddress());				
						/*
						 * If this email address is unsubscribed and we care if they are unsubscribed,
						 * which is shown by the unsubscribed list not being null, then we just record that they
						 * are unsubscribed
						 */
						if(subStatus.unsubscribed && unsubscribedList != null) {
							log.info("#########: Email " + ia.getAddress() + " User has unsubscribed");
							unsubscribedList.add(ia.getAddress());		// Person has unsubscribed
						} else {
							log.info("#########: Email " + ia.getAddress() + " Opted in: " + subStatus.optedIn + "org:  " + !organisation.send_optin);
							if(subStatus.optedIn || !organisation.send_optin) {
								sendEmailHtml(
										organisation.name,
										ia.getAddress(),  
										"bcc", 
										subject, 
										content.toString(), 
										filePath,
										filename,
										emailServer,
										serverName,
										subStatus.emailKey,
										localisation,
										null,
										organisation.getAdminEmail(),
										organisation.getEmailFooter(),
										GeneralUtilityMethods.getNextEmailId(sd, caseReference));

							} else {
								/*
								 * User needs to opt in before email can be sent
								 * Move message to pending messages and send opt in message if needed
								 */ 
								mm.saveToPending(sd, organisation.id, ia.getAddress(), topic, msg, 
										null,
										null,
										subStatus.optedInSent,
										organisation.getAdminEmail(),
										emailServer,
										subStatus.emailKey,
										createPending,
										scheme,
										serverName,
										messageId);
								log.info("#########: Email " + ia.getAddress() + " saved to pending while waiting for optin");

								lm.writeLogOrganisation(sd, organisation.id, ia.getAddress(), LogManager.OPTIN, localisation.getString("mo_pending_saved2"), 0);
							}
						}
					}
					resp.status = "success";
				} catch(Exception e) {
					resp.status = "error";
					resp.error_details = e.getMessage();
					log.log(Level.SEVERE, resp.error_details, e);
				}
			} else {
				log.log(Level.INFO, "Info: List of email recipients is empty");
				lm.writeLog(sd, surveyId, "subscriber", LogManager.EMAIL, localisation.getString("email_nr"), 0, null);
				resp.writeToMonitor = false;
			}
		} else {
			resp.status = "error";
			resp.error_details = localisation.getString("email_ns");
			log.log(Level.SEVERE, "Error: Notification, Attempt to do email notification but email server not set");
		}
		
		return resp;
	}
	
	// Send an email using HTML format
	public void sendEmailHtml( 
			String orgName,
			String email, 
			String ccType, 
			String subject,
			String template,
			String filePath,	// The next two parameters are for an attachment TODO make an array
			String filename,
			EmailServer emailServer,
			String serverName,
			String emailKey,
			ResourceBundle localisation,
			HashMap<String, String> tokens,
			String adminEmail,
			String orgFooter,
			String emailId) throws Exception  {

		/* 
		 * Create the content
		 */
		StringBuilder content = new StringBuilder(template);
		if(adminEmail != null) {
			content.append("</p>")
				.append("<p>")
				.append(localisation.getString("email_dnr"))
				.append("</p>");
		}
		if(orgFooter != null) {
			content.append(" ").append(orgFooter);
		}
		
		// Add source
		StringBuilder source = new StringBuilder();
		source.append("<p>").append(localisation.getString("c_sent_from")).append("</p>");
		source.append("<p style=\"margin-left: 25px;\">").append(localisation.getString("bill_org"))
		.append(": ").append(orgName).append("</p>");
		source.append("<p style=\"margin-left: 25px;\">").append(localisation.getString("c_server"))
				.append(": ").append(serverName).append("</p>");
		content.append(source.toString());
		
		// Add unsubscribe
		StringBuilder unsubscribe = new StringBuilder();
		if(emailKey != null) {
			unsubscribe.append("<br/><p style=\"color:blue;text-align:center;\">")
					.append("<a href=\"https://")
					.append(serverName)
					.append("/app/subscriptions.html?token=")
					.append(emailKey)
					.append("\">")
					.append(localisation.getString("c_unsubscribe"))
					.append("</a>")
					.append("</p>");		
					
			content.append(unsubscribe.toString());
		} 
		
		/*
		 * Perform custom token replacements
		 */
		String contentString = content.toString();
		if(tokens != null) {
			for(String token : tokens.keySet()) {
				String val = tokens.get(token);
				if(val == null) {
					val = "";
				}
				contentString = contentString.replace(token, val);
			}
		}
		
		/*
		 * Sanitise the HTML
		 */
		sanitise.sanitiseHtml(contentString);
		
		/*
		 * Send the email
		 */
		emailServer.send(email, ccType, subject, 
						emailId, 
						contentString,
						filePath,
						filename);
	}
	
	boolean alertEmailSent(Connection sd, int oId, String type) throws SQLException {
		boolean sent = false;
		String sql = "select count(*) from email_alerts "
				+ "where o_id = ? "
				+ "and alert_type = ? "
				+ "and (alert_recorded > now() - interval '1 day')";
		PreparedStatement pstmt = null;
		
		String sqlDel = "delete from email_alerts "
				+ "where o_id = ? "
				+ "and alert_type = ?";
		PreparedStatement pstmtDel = null;
		
		String sqlAdd = "insert into email_alerts "
				+ "(o_id, alert_type, alert_recorded) values(?, ?, now())";
		PreparedStatement pstmtAdd = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2,  type);
			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next() && rs.getInt(1) > 0) {
				sent = true;
			} else {
				pstmtDel = sd.prepareStatement(sqlDel);
				pstmtDel.setInt(1, oId);
				pstmtDel.setString(2,  type);
				pstmtDel.executeUpdate();
				
				pstmtAdd = sd.prepareStatement(sqlAdd);
				pstmtAdd.setInt(1, oId);
				pstmtAdd.setString(2,  type);
				pstmtAdd.executeUpdate();
			}
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmtDel != null) {try {pstmtDel.close();} catch(Exception e) {}}
			if(pstmtAdd != null) {try {pstmtAdd.close();} catch(Exception e) {}}
		}
		return sent;
	}
	
	/*
	 * Get the email addresses from the message settings
	 */
	public String getEmails(Connection sd, Connection cResults, int surveyId, SubmissionMessage msg) throws Exception {
		
		String emails = "";
		HashMap<String, String> sentEndPoints = new HashMap<> ();
		
		ArrayList<String> emailList = null;
		if(msg.emailQuestionSet()) {
			String emailQuestionName = msg.getEmailQuestionName(sd);
			log.info("Email question: " + emailQuestionName);
			emailList = GeneralUtilityMethods.getResponseForQuestion(sd, cResults, surveyId, emailQuestionName, msg.instanceId);
		} else {
			emailList = new ArrayList<String> ();
		}

		// Add any meta email addresses to the per question emails
		String metaEmail = GeneralUtilityMethods.getResponseMetaValue(sd, cResults, surveyId, msg.emailMeta, msg.instanceId);
		if(metaEmail != null) {
			emailList.add(metaEmail);
		}
		
		// Add the static emails to the per question emails
		if(msg.emails != null) {
			for(String email : msg.emails) {
				if(email.length() > 0) {
					log.info("Adding static email: " + email); 
					emailList.add(email);
				}
			}
		}

		// Add the assigned user email
		UserManager um = new UserManager(localisation);
		if(msg.emailAssigned) {
			log.info("--------------------------------------- Adding Assigned User Email Address -----------------");
			ArrayList<String> assignedUser = GeneralUtilityMethods.getResponseForQuestion(sd, cResults, surveyId, "_assigned", msg.instanceId);
			if(assignedUser != null) {
				for(String user : assignedUser) {	// Should only be one assigned user but in future could be more
					log.info("----- User: " + user);
					String email = um.getUserEmailByIdent(sd, user);
					log.info("----- Email: " + user);
					if(email != null) {
						emailList.add(email);
					}
				}
			}
		}
		
		// Convert emails into a comma separated string		
		for(String email : emailList) {	
			if(sentEndPoints.get(email) == null) {
				if(UtilityMethodsEmail.isValidEmail(email)) {
					if(emails.length() > 0) {
						emails += ",";
					}
					emails += email;
				} else {
					log.info("Email Notifications: Discarding invalid email: " + email);
				}
				sentEndPoints.put(email, email);
			} else {
				log.info("Duplicate email: " + email);
			}
		}
		
		return emails;
		
	}
	
	/*
	 * Get the email addresses from the message settings
	 */
	public String getAssignedUserEmails(Connection sd, Connection cResults, int surveyId, String instanceId) throws Exception {
		
		String emails = "";
		HashMap<String, String> sentEndPoints = new HashMap<> ();
		ArrayList<String> emailList = new ArrayList<String> ();

		// Add the assigned user email
		UserManager um = new UserManager(localisation);
		log.info("--------------------------------------- Adding Assigned User Email Address -----------------");
		ArrayList<String> assignedUser = GeneralUtilityMethods.getResponseForQuestion(sd, cResults, surveyId, "_assigned", instanceId);
		if(assignedUser != null) {
			for(String user : assignedUser) {	// Should only be one assigned user but in future could be more
				log.info("----- User: " + user);
				String email = um.getUserEmailByIdent(sd, user);
				log.info("----- Email: " + user);
				if(email != null) {
					emailList.add(email);
				}
			}
		}
		
		// Convert emails into a comma separated string		
		for(String email : emailList) {	
			if(sentEndPoints.get(email) == null) {
				if(UtilityMethodsEmail.isValidEmail(email)) {
					if(emails.length() > 0) {
						emails += ",";
					}
					emails += email;
				} else {
					log.info("Assigned User Email: Discarding invalid email: " + email);
				}
				sentEndPoints.put(email, email);
			} else {
				log.info("Duplicate email: " + email);
			}
		}
		
		return emails;
		
	}
	
	/*
	 * Combine two comma separated email lists into one unique list of valid emails
	 */
	public String mergeEmailLists(String e1, String e2 ) {
		String emails = null;
		if(e1 != null || e2 != null) {
			HashMap<String, String> map = new HashMap<>();
			addEmailsToMap(e1, map);
			addEmailsToMap(e2, map);
			
			StringBuilder emailBuild = new StringBuilder("");
			for(String email : map.keySet()) {	
				if(emailBuild.length() > 0) {
					emailBuild.append(",");
				}
				emailBuild.append(email);
			}
			if(emailBuild.length() > 0) {
				emails = emailBuild.toString();
			}
		}
		return emails;
	}
	
	private void addEmailsToMap(String eList, HashMap<String, String> map) {
		if(eList != null) {
			String [] eArray = eList.split(",");
			for(String e : eArray) {
				e = e.trim();
				if(UtilityMethodsEmail.isValidEmail(e)) {
					map.put(e, e);
				}
			}
		}
	}
}


