package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.InternetAddress;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.MailoutLinks;
import org.smap.sdal.model.MailoutMessage;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.MailoutPersonTotals;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SubscriptionStatus;

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
 * Manage the log table
 * Assume emails are case insensitive
 */
public class MailoutManager {
	
	private static Logger log =
			 Logger.getLogger(MailoutManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	ResourceBundle localisation = null;
	
	public MailoutManager(ResourceBundle l) {
		localisation = l;	
	}

	public static String STATUS_NEW = "new";
	public static String STATUS_SENT = "sent";
	public static String STATUS_UNSUBSCRIBED = "unsubscribed";
	public static String STATUS_PENDING = "pending";
	public static String STATUS_ERROR = "error";
	public static String STATUS_COMPLETE = "complete";
	public static String STATUS_EXPIRED = "expired";
	public static String STATUS_MANUAL = "manual";
	
	/*
	 * Get mailouts for a survey
	 */
	public ArrayList<Mailout> getMailouts(Connection sd, 
			String surveyIdent,
			boolean links,
			String urlprefix) throws SQLException {
		
		ArrayList<Mailout> mailouts = new ArrayList<> ();
		
		String sql = "select id, survey_ident, name, subject, content "
				+ "from mailout "
				+ "where survey_ident = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, surveyIdent);
			log.info("Get mailouts: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				Mailout mo = new Mailout(
						rs.getInt("id"),
						rs.getString("survey_ident"), 
						rs.getString("name"),
						rs.getString("subject"),
						rs.getString("content"));
				
				if(links) {
					mo.links = new MailoutLinks();
					mo.links.emails = urlprefix + "api/v1/mailout/" + mo.id + "/emails?links=true";
				}
				mailouts.add(mo);			
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mailouts;
	}
	
	/*
	 * Add a mailout
	 */
	public int addMailout(Connection sd, Mailout mailout) throws SQLException, ApplicationException {
		
		int mailoutId = 0;
		
		String sql = "insert into mailout "
				+ "(survey_ident, name, subject, content, created, modified) "
				+ "values(?, ?, ?, ?, now(), now())";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1,  mailout.survey_ident);
			pstmt.setString(2, mailout.name);
			pstmt.setString(3, mailout.subject);
			pstmt.setString(4, mailout.content);
			log.info("Add mailout: " + pstmt.toString());
			pstmt.executeUpdate();
			
			ResultSet rsKeys = pstmt.getGeneratedKeys();
			if(rsKeys.next()) {
				mailoutId = rsKeys.getInt(1);
			} 
		
		} catch(Exception e) {
			String msg = e.getMessage();
			if(msg != null && msg.contains("duplicate key value violates unique constraint")) {
				throw new ApplicationException(localisation.getString("msg_dup_name"));
			} else {
				throw e;
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mailoutId;
	}
	
	/*
	 * Update a mailout
	 */
	public void updateMailout(Connection sd, Mailout mailout) throws SQLException, ApplicationException {
		
		String sql = "update mailout "
				+ "set survey_ident = ?, "
				+ "name = ?,"
				+ "subject = ?, "
				+ "content = ?, "
				+ "modified = now() "
				+ "where id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  mailout.survey_ident);
			pstmt.setString(2, mailout.name);
			pstmt.setString(3, mailout.subject);
			pstmt.setString(4, mailout.content);
			pstmt.setInt(5, mailout.id);
			log.info("Update mailout: " + pstmt.toString());
			pstmt.executeUpdate();
		
		} catch(Exception e) {
			String msg = e.getMessage();
			if(msg != null && msg.contains("duplicate key value violates unique constraint")) {
				throw new ApplicationException(localisation.getString("msg_dup_name"));
			} else {
				throw e;
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
	}
	
	/*
	 * Get mailouts Details
	 */
	public Mailout getMailoutDetails(Connection sd, int mailoutId) throws SQLException {
		
		Mailout mailout = null;
		
		String sql = "select survey_ident, name, subject, content "
				+ "from mailout "
				+ "where id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			log.info("Get mailout details: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				mailout = new Mailout(
						mailoutId,
						rs.getString("survey_ident"), 
						rs.getString("name"),
						rs.getString("subject"),
						rs.getString("content"));
				
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mailout;
	}
	
	/*
	 * Get People in a mailout
	 */
	public ArrayList<MailoutPerson> getMailoutPeople(Connection sd, int mailoutId) throws SQLException {
		
		ArrayList<MailoutPerson> mpList = new ArrayList<> ();
		
		String sql = "select mp.id, p.name, p.email, mp.status, mp.status_details, "
				+ "mp.initial_data, mp.link "
				+ "from mailout_people mp, people p "
				+ "where p.id = mp.p_id "
				+ "and mp.m_id = ? "
				+ "order by p.email asc ";
		
		PreparedStatement pstmt = null;
		
		String loc_new = localisation.getString("c_new");
		String loc_sent = localisation.getString("c_sent");
		String loc_unsub = localisation.getString("c_unsubscribed");
		String loc_pending = localisation.getString("c_pending");
		String loc_error = localisation.getString("c_error");
		String loc_complete = localisation.getString("c_complete");
		String loc_expired = localisation.getString("c_expired");
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			log.info("Get mailout people: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				MailoutPerson mp = new MailoutPerson(
						rs.getInt("id"),
						rs.getString("email"), 
						rs.getString("name"),
						rs.getString("status"),
						rs.getString("status_details"),
						rs.getString("link"));	
				
				String initialData = rs.getString("initial_data");
				if(initialData != null) {
					mp.initialData = gson.fromJson(initialData, Instance.class);
				} 
				
				if(mp.status == null) {
					mp.status_loc = loc_new;
				} else if(mp.status.equals(MailoutManager.STATUS_SENT)) {
					mp.status_loc = loc_sent;
				} else if(mp.status.equals(MailoutManager.STATUS_UNSUBSCRIBED)) {
					mp.status_loc = loc_unsub;
				} else if(mp.status.equals(MailoutManager.STATUS_PENDING)) {
					mp.status_loc = loc_pending;
				} else if(mp.status.equals(MailoutManager.STATUS_ERROR)) {
					mp.status_loc = loc_error;
				} else if(mp.status.equals(MailoutManager.STATUS_COMPLETE)) {
					mp.status_loc = loc_complete;
				} else if(mp.status.equals(MailoutManager.STATUS_EXPIRED)) {
					mp.status_loc = loc_expired;
				} else {
					mp.status_loc = loc_new;
				}
				
				mpList.add(mp);
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mpList;
	}
	
	/*
	 * Get mailouts Detail Totals
	 */
	public MailoutPersonTotals getMailoutPeopleTotals(Connection sd, int mailoutId) throws SQLException {
		
		MailoutPersonTotals totals = new MailoutPersonTotals();
		
		totals.complete = getTotal(sd, mailoutId, " and status = 'complete' ");
		totals.error = getTotal(sd, mailoutId, " and status = 'error' ");
		totals.unsent = getTotal(sd, mailoutId, " and status = 'new' ");
		totals.sent = getTotal(sd, mailoutId, " and status = 'sent' ");
		totals.unsubscribed = getTotal(sd, mailoutId, " and status = 'unsubscribed' ");
		totals.pending = getTotal(sd, mailoutId, " and status = 'pending' ");
		totals.expired = getTotal(sd, mailoutId, " and status = 'expired' ");
		
		return totals;
	}
	
	private int getTotal(Connection sd, int mailoutId, String filter) throws SQLException {
		
		String sqlTotal = "select count(*) "
				+ "from mailout_people "
				+ "where  m_id = ? ";
		
		int total = 0;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			pstmt = sd.prepareStatement(sqlTotal + filter);
			pstmt.setInt(1, mailoutId);
			rs = pstmt.executeQuery();
			if(rs.next()) {
				total = rs.getInt(1);
			}	
		} finally {
			try {if (rs != null) {rs.close();} } catch (SQLException e) {	}
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		return total;
	}
	
	public void deleteUnsentEmails(Connection sd, int mailoutId) throws SQLException {
		
		String sql = "delete from mailout_people "
				+ "where m_id = ? "
				+ "and status = ? ";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			pstmt.setString(2, STATUS_NEW);
			log.info("Delete unsent: " + pstmt.toString());
			pstmt.executeUpdate();	
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	public int writeEmails(Connection sd, 
		int oId, 
		ArrayList<MailoutPerson> mop, 
		int mailoutId,
		String status) throws Exception {
		
		int mpId = 0;
		
		String sqlGetPerson = "select id from people "
				+ "where o_id = ? "
				+ "and email = ? ";		
		PreparedStatement pstmtGetPerson = null;
		
		String sqlAddPerson = "insert into people "
				+ "(o_id, email, name) "
				+ "values(?, ?, ?)";		
		PreparedStatement pstmtAddPerson = null;
		
		String sqlAddMailoutPerson = "insert into mailout_people "
				+ "(p_id, m_id, status, initial_data) "
				+ "values(?, ?, ?, ?) ";
		PreparedStatement pstmtAddMailoutPerson = null;
		
		String sqlMailoutExists = "select count(*) "
				+ "from mailout_people "
				+ "where p_id = ? "
				+ "and m_id = ? ";
		PreparedStatement pstmtMailoutExists = null;
		
		try {
			pstmtGetPerson = sd.prepareStatement(sqlGetPerson);
			pstmtGetPerson.setInt(1, oId);
			
			pstmtAddPerson = sd.prepareStatement(sqlAddPerson, Statement.RETURN_GENERATED_KEYS);
			pstmtAddPerson.setInt(1, oId);
			
			pstmtAddMailoutPerson = sd.prepareStatement(sqlAddMailoutPerson, Statement.RETURN_GENERATED_KEYS);
			
			pstmtMailoutExists = sd.prepareStatement(sqlMailoutExists);
			pstmtMailoutExists.setInt(2, mailoutId);
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			int index = 0;
			for(MailoutPerson person : mop) {
				
				int personId = 0;
				
				// 1. Get person details
				pstmtGetPerson.setString(2, person.email);
				ResultSet rs = pstmtGetPerson.executeQuery();
				if(rs.next()) {
					personId = rs.getInt(1);
				} else {
					
					// 2. Add person to people table if they do not exist
					pstmtAddPerson.setString(2, person.email);
					pstmtAddPerson.setString(3, person.name);
					pstmtAddPerson.executeUpdate();
					ResultSet rsKeys = pstmtAddPerson.getGeneratedKeys();
					if(rsKeys.next()) {
						personId = rsKeys.getInt(1);
					} else {
						throw new Exception("Failed to get id of person");
					}
				}
				
				// Check to see if this person is already in the mailout
				pstmtMailoutExists.setInt(1, personId);
				ResultSet rsExists = pstmtMailoutExists.executeQuery();
				if(rsExists.next() && rsExists.getInt(1) > 0) {
					//  skip
				} else {
					// Write the entry into the mailout person table
					pstmtAddMailoutPerson.setInt(1, personId);
					pstmtAddMailoutPerson.setInt(2, mailoutId);
					pstmtAddMailoutPerson.setString(3, status);
					
					String initialData = null;
					if(person.initialData != null) {
						initialData = gson.toJson(person.initialData);
					}
					pstmtAddMailoutPerson.setString(4, initialData);				
					pstmtAddMailoutPerson.executeUpdate();
					
					if(index++ == 0) {
						// Only get the Id of the first create mailout person
						// This will only be used when the requestor is only asking
						// for a single mailout to be created
						ResultSet rsKeys = pstmtAddMailoutPerson.getGeneratedKeys();
						if(rsKeys.next()) {
							mpId = rsKeys.getInt(1);
						} 
					}
				}
			}		
	
		} finally {
			try {if (pstmtGetPerson != null) {pstmtGetPerson.close();} } catch (SQLException e) {	}
			try {if (pstmtAddPerson != null) {pstmtAddPerson.close();} } catch (SQLException e) {	}
			try {if (pstmtAddMailoutPerson != null) {pstmtAddMailoutPerson.close();} } catch (SQLException e) {	}
			try {if (pstmtMailoutExists != null) {pstmtMailoutExists.close();} } catch (SQLException e) {	}
		}
		
		return mpId;
	}

	/*
	 * Send emails for a mailout
	 */
	public void sendEmails(Connection sd, int mailoutId, boolean retry) throws SQLException {
		
		String sql = "update mailout_people set status_details = null, "
				+ "processed = null, "
				+ "status = '" + MailoutManager.STATUS_PENDING +"'  "
				+ "where m_id = ? "
				+ "and status = '" + 
						(retry ? MailoutManager.STATUS_ERROR : MailoutManager.STATUS_NEW)
						+ "'";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			log.info("Send unsent: " + pstmt.toString());
			pstmt.executeUpdate();
			
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return;
	}
	
	/*
	 * Send an email mailout message
	 */
	public void emailMailout(
			Connection sd, 
			Connection cResults, 
			Organisation organisation,
			MailoutMessage msg,
			int messageId,
			String user,
			String basePath,
			String scheme,
			String server,
			String topic,
			boolean createPending) throws Exception {
		
		String docURL = null;
		if(msg.actionLink != null) {
			docURL = "/webForm" + msg.actionLink;
		}
		String filePath = null;
		String filename = "instance";
		int surveyId = GeneralUtilityMethods.getSurveyId(sd, msg.survey_ident);
		
		boolean writeToMonitor = true;
		MessagingManager mm = new MessagingManager(localisation);
		
		PreparedStatement pstmtNotificationLog = null;
		String sqlNotificationLog = "insert into notification_log " +
				"(o_id, p_id, s_id, notify_details, status, status_details, event_time, message_id, type) " +
				"values( ?, ?,?, ?, ?, ?, now(), ?, 'mailout'); ";
		
		// Time Zone
		int utcOffset = 0;	
		LocalDateTime dt = LocalDateTime.now();
		if(organisation.timeZone != null) {
			try {
				ZoneId zone = ZoneId.of(organisation.timeZone);
			    ZonedDateTime zdt = dt.atZone(zone);
			    ZoneOffset offset = zdt.getOffset();
			    utcOffset = offset.getTotalSeconds() / 60;
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		
		try {
			
			pstmtNotificationLog = sd.prepareStatement(sqlNotificationLog);
			
			// Notification log
			String error_details = null;
			String notify_details = null;
			String status = null;
			boolean unsubscribed = false;
			
			if(organisation.email_task) {		// Organisation is pernitted to do mailouts
					
				/*
				 * Send document to target
				 */
				status = "success";				// Notification log
				notify_details = null;			// Notification log
				error_details = null;				// Notification log
				unsubscribed = false;
				if(msg.target.equals("email")) {
					EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, msg.user);
					if(emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {
						if(UtilityMethodsEmail.isValidEmail(msg.email)) {
								
							log.info("userevent: " + msg.user + " sending email of '" + docURL + "' to " + msg.email);
							
							// Set the subject
							String subject = "";
							if(msg.subject != null && msg.subject.trim().length() > 0) {
								subject = msg.subject;
							} else {
								if(server != null && server.contains("smap")) {
									subject = "Smap ";
								}
								subject += localisation.getString("ar_survey");
							}
							
							String from = "smap";
							if(msg.from != null && msg.from.trim().length() > 0) {
								from = msg.from;
							}
							String content = null;
							if(msg.content != null && msg.content.trim().length() > 0) {
								content = msg.content;
							} else {
								content = organisation.default_email_content;
							}
							
							notify_details = "Sending mailout email to: " + msg.email + " containing link " + docURL;
							
							log.info("+++ emailing mailout to: " + msg.email + " docUrl: " + docURL + 
									" from: " + from + 
									" subject: " + subject +
									" smtp_host: " + emailServer.smtpHost +
									" email_domain: " + emailServer.emailDomain);
							try {
								EmailManager em = new EmailManager();
								PeopleManager peopleMgr = new PeopleManager(localisation);
								InternetAddress[] emailArray = InternetAddress.parse(msg.email);
								
								for(InternetAddress ia : emailArray) {	
									SubscriptionStatus subStatus = peopleMgr.getEmailKey(sd, organisation.id, ia.getAddress());							
									if(subStatus.unsubscribed) {
										unsubscribed = true;
										setMailoutStatus(sd, msg.mpId, STATUS_UNSUBSCRIBED, null);
									} else {
										if(subStatus.optedIn || !organisation.send_optin) {
											log.info("Send email: " + msg.email + " : " + docURL);
											em.sendEmail(
													ia.getAddress(), 
													null, 
													"notify", 
													subject, 
													content,
													from,		
													null, 
													null, 
													null, 
													docURL, 
													filePath,
													filename,
													organisation.getAdminEmail(), 
													emailServer,
													scheme,
													server,
													subStatus.emailKey,
													localisation,
													organisation.server_description,
													organisation.name);
										
										} else {
											/*
											 * User needs to opt in before email can be sent
											 * Move message to pending messages and send opt in message if needed
											 */ 
											mm.saveToPending(sd, organisation.id, ia.getAddress(), topic, null, 
													null,
													msg, 
													subStatus.optedInSent,
													organisation.getAdminEmail(),
													emailServer,
													subStatus.emailKey,
													createPending,
													scheme,
													server);
										}
										setMailoutStatus(sd, msg.mpId, STATUS_SENT, null);
									}
								}
							} catch(Exception e) {
								status = "error";
								error_details = e.getMessage();
								
							}
						} else {
							log.log(Level.INFO, "Info: List of email recipients is empty");
							lm.writeLog(sd, surveyId, "subscriber", LogManager.EMAIL, localisation.getString("email_nr"));
							writeToMonitor = false;
						}
					} else {
						status = "error";
						error_details = "smtp_host not set";
						log.log(Level.SEVERE, "Error: Attempt to do email notification but email server not set");
					}
					
				}  else {
					status = "error";
					error_details = "Invalid target: " + msg.target;
					log.log(Level.SEVERE, "Error: Invalid target" + msg.target);
				}
			} else {
				status = "error";
				error_details = localisation.getString("susp_email_tasks");
				log.log(Level.SEVERE, "Error: notification services suspended");
			}
			
			// Update pending message
			if(status != null && status.equals("error")) {
				setMailoutStatus(sd, msg.mpId, "error", error_details);
			}
			
			// Write log message
			if(writeToMonitor) {
				if(unsubscribed) {
					error_details += localisation.getString("c_unsubscribed") + ": " + msg.email;
				}
				pstmtNotificationLog.setInt(1, organisation.id);
				pstmtNotificationLog.setInt(2, msg.pId);
				pstmtNotificationLog.setInt(3, surveyId);
				pstmtNotificationLog.setString(4, notify_details);
				pstmtNotificationLog.setString(5, status);
				pstmtNotificationLog.setString(6, error_details);
				pstmtNotificationLog.setInt(7, messageId);
				
				pstmtNotificationLog.executeUpdate();
			}
		} finally {
			try {if (pstmtNotificationLog != null) {pstmtNotificationLog.close();}} catch (SQLException e) {}
			
		}
	}
	
	public void setMailoutStatus(Connection sd, int mpId, String status, String details) throws SQLException {
		
		String sql = "update mailout_people "
				+ "set status = ?,"
				+ "status_details = ?, "
				+ "status_updated = now() "
				+ "where id = ? ";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, status);
			pstmt.setString(2, details);
			pstmt.setInt(3, mpId);
			pstmt.executeUpdate();
			
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Delete a mailout
	 */
	public void deleteMailout(Connection sd, int mailoutId) throws SQLException {
		
		String sql = "delete from mailout where id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			log.info("Delete Mailout: " + pstmt.toString());
			pstmt.executeUpdate();
			
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return;
	}
	
}


