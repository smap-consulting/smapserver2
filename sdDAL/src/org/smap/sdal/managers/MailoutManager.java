package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.InternetAddress;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Action;
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
		
		String sql = "select id, survey_ident, name, subject, content, multiple_submit "
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
						rs.getString("content"),
						rs.getBoolean("multiple_submit"));
				
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
				+ "(survey_ident, name, subject, content, multiple_submit, created, modified) "
				+ "values(?, ?, ?, ?, ?, now(), now())";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1,  mailout.survey_ident);
			pstmt.setString(2, mailout.name);
			pstmt.setString(3, mailout.subject);
			pstmt.setString(4, mailout.content);
			pstmt.setBoolean(5, mailout.multiple_submit);
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
				+ "content = ?,"
				+ "multiple_submit = ?,"
				+ "modified = now() "
				+ "where id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  mailout.survey_ident);
			pstmt.setString(2, mailout.name);
			pstmt.setString(3, mailout.subject);
			pstmt.setString(4, mailout.content);
			pstmt.setBoolean(5, mailout.multiple_submit);
			pstmt.setInt(6, mailout.id);
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
		
		String sql = "select survey_ident, name, subject, content, multiple_submit "
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
						rs.getString("content"),
						rs.getBoolean("multiple_submit"));
				
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mailout;
	}
	
	/*
	 * Get People in a mailout
	 */
	public ArrayList<MailoutPerson> getMailoutPeople(Connection sd, int mailoutId, int oId, boolean isDt) throws SQLException {
		
		ArrayList<MailoutPerson> mpList = new ArrayList<> ();
		
		String sql = "select mp.id, p.name, p.email, mp.status, mp.status_details, "
				+ "mp.initial_data, mp.link, "
				+ "(select  count(*) from upload_event ue "
					+ "where ue.o_id = ? "
					+ "and ue.user_name = p.email "
					+ "and ue.db_status = 'success' ) as submissions "
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
		String loc_manual = localisation.getString("c_manual");
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, mailoutId);
			log.info("Get mailout people: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				MailoutPerson mp = new MailoutPerson(
						rs.getInt("id"),
						GeneralUtilityMethods.getSafeText(rs.getString("email"), isDt),
						GeneralUtilityMethods.getSafeText(rs.getString("name"), isDt),
						rs.getString("status"),
						rs.getString("status_details"),
						rs.getString("link"),
						rs.getInt("submissions"));	
				
				String initialData = rs.getString("initial_data");
				if(initialData != null) {
					mp.initialData = gson.fromJson(initialData, Instance.class);
				} 
				
				if(mp.status == null || mp.status.equals(MailoutManager.STATUS_NEW)) {
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
				} else if(mp.status.equals(MailoutManager.STATUS_MANUAL)) {
					mp.status_loc = loc_manual;
				} else {
					mp.status_loc = mp.status;
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
		totals.manual = getTotal(sd, mailoutId, " and status = 'manual' ");
		
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
		
		String sqlGetPerson = "select id, name from people "
				+ "where o_id = ? "
				+ "and email = ? ";		
		PreparedStatement pstmtGetPerson = null;
		
		String sqlAddPerson = "insert into people "
				+ "(o_id, email, name) "
				+ "values(?, ?, ?)";		
		PreparedStatement pstmtAddPerson = null;
		
		String sqlUpdatePerson = "update people "
				+ "set name = ? "
				+ "where id = ? ";	
		PreparedStatement pstmtUpdatePerson = null;
		
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
			
			pstmtUpdatePerson = sd.prepareStatement(sqlUpdatePerson);
			
			pstmtAddMailoutPerson = sd.prepareStatement(sqlAddMailoutPerson, Statement.RETURN_GENERATED_KEYS);
			
			pstmtMailoutExists = sd.prepareStatement(sqlMailoutExists);
			pstmtMailoutExists.setInt(2, mailoutId);
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			int index = 0;
			for(MailoutPerson person : mop) {
				
				int personId = 0;
				String personName = null;
				
				// 1. Get person details
				pstmtGetPerson.setString(2, person.email);
				ResultSet rs = pstmtGetPerson.executeQuery();
				if(rs.next()) {
					personId = rs.getInt("id");
					personName = rs.getString("name");
					
					// If the existing person's Name is empty (probably a legacy person) then update the name
					if(personName == null || personName.trim().length() == 0) {
						pstmtUpdatePerson.setString(1, person.name);
						pstmtUpdatePerson.setInt(2, personId);
						
						pstmtUpdatePerson.executeUpdate();
					}
					
				} else {
					
					// 2. Add person to people table if they do not exist
					pstmtAddPerson.setString(2, person.email);
					pstmtAddPerson.setString(3, person.name);
					log.info("Add person to people: " + pstmtAddPerson.toString());
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
					log.info("Add person to mailout table: " + pstmtAddMailoutPerson.toString());
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
			try {if (pstmtUpdatePerson != null) {pstmtUpdatePerson.close();} } catch (SQLException e) {	}
			try {if (pstmtAddMailoutPerson != null) {pstmtAddMailoutPerson.close();} } catch (SQLException e) {	}
			try {if (pstmtMailoutExists != null) {pstmtMailoutExists.close();} } catch (SQLException e) {	}
		}
		
		return mpId;
	}

	/*
	 * Send emails for a mailout
	 */
	public void sendEmails(Connection sd, int mailoutId, boolean retry) throws SQLException {
		
		StringBuffer sql = new StringBuffer("update mailout_people set status_details = null, "
				+ "processed = null, "
				+ "status = '" + MailoutManager.STATUS_PENDING +"'  "
				+ "where m_id = ? ");
		
		if(retry) {
			sql.append("and (status = '");
			sql.append(MailoutManager.STATUS_ERROR);
			sql.append("' or status = '");
			sql.append(MailoutManager.STATUS_UNSUBSCRIBED);
			sql.append("')");
		} else {
			sql.append("and status = '");
			sql.append(MailoutManager.STATUS_NEW);
			sql.append("'");
		}
				
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setInt(1, mailoutId);
			log.info("Send unsent: " + pstmt.toString());
			pstmt.executeUpdate();
			
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return;
	}
	
	/*
	 * Generate the links for a mailout
	 */
	public void genEmailLinks(Connection sd, int mailoutId, String serverName) throws Exception {
		
		// Sql to get mailouts
		String sql = "select "
				+ "mp.id, "
				+ "p.o_id, "
				+ "m.survey_ident,"
				+ "m.multiple_submit,"
				+ "p.id as p_id, "
				+ "ppl.email, "
				+ "mp.initial_data, "
				+ "m.name as campaign_name, "
				+ "m.anonymous "
				+ "from mailout_people mp, mailout m, people ppl, survey s, project p "
				+ "where mp.m_id = m.id "
				+ "and mp.p_id = ppl.id "
				+ "and m.survey_ident = s.ident "
				+ "and s.p_id = p.id "
				+ "and mp.link is null ";
		PreparedStatement pstmt = null;
		
		// SQL to record the link
		String sqlLinkCreated = "update mailout_people set link = ?, user_ident = ? where id = ?";
		PreparedStatement pstmtLinkCreated = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmtLinkCreated = sd.prepareStatement(sqlLinkCreated);
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			ActionManager am = new ActionManager(localisation, "UTC");

			log.info("----- Gen mailout links: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {		
				int id = rs.getInt("id");
				int oId = rs.getInt("o_id");
				String surveyIdent = rs.getString("survey_ident");
				int pId = rs.getInt("p_id");
				String email = rs.getString("email");
				String initialData = rs.getString("initial_data");
				boolean single = !rs.getBoolean("multiple_submit");
				String campaignName = rs.getString("campaign_name");
				boolean anonymousCampaign = rs.getBoolean("anonymous");
				
				// Create the action 					
				Action action = new Action("mailout");
				action.surveyIdent = surveyIdent;
				action.pId = pId;
				action.single = single;
				action.mailoutPersonId = id;
				action.email = email;
				action.campaignName = campaignName;
				action.anonymousCampaign = anonymousCampaign;
					
				if(initialData != null) {
					action.initialData = gson.fromJson(initialData, Instance.class);
				}
					
				String link = am.getLink(sd, action, oId, action.single);
				String userIdent = null;
				int idx = link.lastIndexOf("/");
				if(idx >= 0) {
					userIdent = link.substring(idx + 1);
				}
				
				// record the sending of the notification
				pstmtLinkCreated.setString(1, "https://" + serverName + "/webForm" + link);
				pstmtLinkCreated.setString(2, userIdent);
				pstmtLinkCreated.setInt(3, id);
				log.info("Generate link: " + pstmtLinkCreated.toString());
				pstmtLinkCreated.executeUpdate();
				
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtLinkCreated != null) {pstmtLinkCreated.close();}} catch (SQLException e) {}
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
			docURL = "https://" + server + "/webForm" + msg.actionLink;
		}
		int surveyId = GeneralUtilityMethods.getSurveyId(sd, msg.survey_ident);
		
		boolean writeToMonitor = true;
		MessagingManager mm = new MessagingManager(localisation);

		try {
			
			// Notification log
			String error_details = null;
			String notify_details = null;
			String status = null;
			boolean unsubscribed = false;
			
			if(organisation.email_task) {		// Organisation is permitted to do mailouts
					
				/*
				 * Send document to target
				 */
				status = "success";				// Notification log
				notify_details = null;			// Notification log
				error_details = null;				// Notification log
				unsubscribed = false;
				if(msg.target.equals("email")) {
					EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, msg.user, organisation.id);
					if(emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {
						if(UtilityMethodsEmail.isValidEmail(msg.email)) {
							try {	
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
								StringBuilder content = null;
								if(msg.content != null && msg.content.trim().length() > 0) {
									content = new StringBuilder(msg.content);
								} else {
									content = new StringBuilder(organisation.default_email_content);
								}
								
								// Add the survey link
								if(docURL != null) {
									content.append("<br />")
										.append("<a href=\"").append(docURL).append("\">")
										.append(localisation.getString("ar_survey")).append("</a>");
								} 
								
								notify_details = localisation.getString("msg_mo");
								notify_details = notify_details.replace("%s1", msg.email);
								notify_details = notify_details.replace("%s2", docURL == null ? "" : docURL);
								
								log.info("+++ emailing mailout to: " + msg.email + " docUrl: " + docURL + 
										" from: " + from + 
										" subject: " + subject +
										" smtp_host: " + emailServer.smtpHost +
										" email_domain: " + emailServer.emailDomain);

								EmailManager em = new EmailManager(localisation);
								PeopleManager peopleMgr = new PeopleManager(localisation);
								InternetAddress[] emailArray = InternetAddress.parse(msg.email);
								
								for(InternetAddress ia : emailArray) {	
									SubscriptionStatus subStatus = peopleMgr.getEmailKey(sd, organisation.id, ia.getAddress());							
									if(subStatus.unsubscribed) {
										unsubscribed = true;
										setMailoutStatus(sd, msg.mpId, STATUS_UNSUBSCRIBED, null);
									} else {
										if(subStatus.optedIn || !organisation.send_optin 
												|| subStatus.optedInSent == null	// First mailout is the optin
												) {
											
											log.info("Send email: " + msg.email + " : " + docURL);									
											lm.writeLog(sd, 
													GeneralUtilityMethods.getSurveyId(sd, msg.survey_ident), 
													ia.getAddress(), 
													LogManager.MAILOUT, 
													localisation.getString("mo_sent"), 0, null);

											em.sendEmailHtml(
													ia.getAddress(), 
													"bcc", 
													subject, 
													content, 
													null, 
													null, 
													emailServer,
													server,
													subStatus.emailKey,
													localisation,
													null,
													organisation.getAdminEmail(),
													organisation.getEmailFooter()
													);
											
											if(subStatus.optedInSent == null) {
												mm.sendOptinEmail(sd, organisation.id, ia.getAddress(), 
														organisation.getAdminEmail(), emailServer, 
														subStatus.emailKey, scheme, server,
														false);		// Do not sent the optin email just record it as having been done
												
											}
										
										} else {
											/*
											 * User needs to opt in before email can be sent
											 * Move message to pending
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
													server,
													messageId);
											
											String note = localisation.getString("mo_pending_saved");
											note = note.replace("%s1", msg.survey_ident);
											lm.writeLogOrganisation(sd, organisation.id, ia.getAddress(), LogManager.MAILOUT, note, 0);

										}
										setMailoutStatus(sd, msg.mpId, STATUS_SENT, null);
									}
								}
							} catch(Exception e) {
								log.log(Level.SEVERE, e.getMessage(),e);
								status = "error";
								error_details = e.getMessage();
								
							}
						} else {
							log.log(Level.INFO, "Info: List of email recipients is empty");
							lm.writeLog(sd, surveyId, "subscriber", LogManager.EMAIL, localisation.getString("email_nr"), 0, null);
							writeToMonitor = false;
						}
					} else {
						status = "error";
						error_details = "smtp_host not set";
						log.log(Level.SEVERE, "Error: Mailout, Attempt to do email notification but email server not set");
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
				NotificationManager nm = new NotificationManager(localisation);
				nm.writeToLog(sd, organisation.id, msg.pId, surveyId, notify_details, status, 
						error_details, messageId);
			}
		} finally {
			//
			
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
	public void deleteMailout(Connection sd, int mailoutId, int oId) throws Exception {
		
		String sqlGet = "select link from mailout_people where m_id = ?";
		String sql = "delete from mailout where id = ?";
		
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmt = null;
		
		try {
			/*
			 * Delete temporary users
			 */
			pstmtGet = sd.prepareStatement(sqlGet);
			pstmtGet.setInt(1, mailoutId);
			ResultSet rs = pstmtGet.executeQuery();
			while (rs.next()) {
				String link = rs.getString(1);
				if(link != null) {
					int idx = link.lastIndexOf("/");
					if(idx >= 0) {
						String temp_user = link.substring(idx + 1);
						GeneralUtilityMethods.deleteTempUser(sd, localisation, oId, temp_user);
					}
				}
			}
			
			/*
			 * Delete mailout entries
			 */
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			log.info("Delete Mailout: " + pstmt.toString());
			pstmt.executeUpdate();
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtGet != null) {pstmtGet.close();} } catch (SQLException e) {	}
		}
		
		return;
	}
	
}


