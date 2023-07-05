package org.smap.sdal.managers;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.InternetAddress;
import org.codehaus.jettison.json.JSONArray;
import org.smap.notifications.interfaces.EmitAwsSMS;
import org.smap.notifications.interfaces.EmitSMS;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.PdfUtilities;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Notification;
import org.smap.sdal.model.NotifyDetails;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.sdal.model.SubscriptionStatus;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;

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
 * Manage the table that stores details on the forwarding of data onto other systems
 */
public class NotificationManager {

	private static Logger log =
			Logger.getLogger(NotificationManager.class.getName());

	LogManager lm = new LogManager();		// Application log

	private ResourceBundle localisation;

	public NotificationManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Get all Enabled notifications
	 * Used by Subscriber to do forwarding
	 */
	public ArrayList<Notification> getEnabledNotifications(
			Connection sd, 
			String trigger,
			String target,
			String tz) throws Exception {

		ArrayList<Notification> forwards = new ArrayList<Notification>();	// Results of request

		ResultSet resultSet = null;
		String sql = "select f.id, "
				+ "f.s_id, "
				+ "f.enabled, " +
				" f.remote_s_id, "
				+ "f.remote_s_name, "
				+ "f.remote_host, "
				+ "f.remote_user, "
				+ "f.trigger, "
				+ "f.target, "
				+ "s.display_name, "
				+ "f.notify_details, "
				+ "f.filter, "
				+ "f.name, "
				+ "f.tg_id,"
				+ "f.period,"
				+ "f.update_survey,"
				+ "f.update_question,"
				+ "f.update_value,"
				+ "f.remote_password,"
				+ "f.alert_id "
				+ "from forward f, survey s "
				+ "where f.s_id = s.s_id "
				+ "and f.enabled = 'true' "
				+ "and f.trigger = ? ";
		PreparedStatement pstmt = null;

		try {
			if(target.equals("forward")) {
				sql += " and f.target = 'forward' and f.remote_host is not null";
			} else if(target.equals("message")) {
				sql += " and (f.target = 'email' or f.target = 'sms')";
			} else if(target.equals("document")) {
				sql += " and f.target = 'document'";
			}	

			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, trigger);
			resultSet = pstmt.executeQuery();

			addToList(sd, resultSet, forwards, true, false, tz);
		} finally {
			try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
		}

		return forwards;

	}

	/*
	 * Add a record to the notification table
	 */
	public void addNotification(Connection sd, PreparedStatement pstmt, String user, 
			Notification n, String tz) throws Exception {

		String sql = "insert into forward(" +
				" s_id, enabled, " +
				" remote_s_id, remote_s_name, remote_host, remote_user, remote_password, notify_details, "
				+ "trigger, target, filter, name, tg_id, period, update_survey, update_question, update_value,"
				+ "alert_id, "
				+ "p_id, periodic_time, periodic_period, periodic_day_of_week, periodic_day_of_month, periodic_month, r_id) " +
				" values (?, ?, ?, ?, ?, ?, ?, ?"
				+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
				+ "?, ?, ?, ?, ?, ?, ?)";

		try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String notifyDetails = gson.toJson(n.notifyDetails);

		pstmt = sd.prepareStatement(sql);	 			
		pstmt.setInt(1, n.s_id);
		pstmt.setBoolean(2, n.enabled);
		pstmt.setString(3, n.remote_s_ident);
		pstmt.setString(4, n.remote_s_name);
		pstmt.setString(5, n.remote_host);
		pstmt.setString(6, n.remote_user);
		pstmt.setString(7, n.remote_password);
		pstmt.setString(8, notifyDetails);
		pstmt.setString(9, n.trigger);
		pstmt.setString(10, n.target);
		pstmt.setString(11, n.filter);
		pstmt.setString(12, n.name);
		pstmt.setInt(13, n.tgId);
		pstmt.setString(14, n.period);
		pstmt.setString(15, n.updateSurvey);
		pstmt.setString(16, n.updateQuestion);
		pstmt.setString(17, n.updateValue);
		pstmt.setInt(18, n.alert_id);
		
		/*
		 * Periodic Values
		 */
		pstmt.setInt(19, n.p_id);
		pstmt.setTime(20, GeneralUtilityMethods.convertTimeUtc(n.periodic_time, tz));
		pstmt.setString(21, n.periodic_period);
		pstmt.setInt(22, n.periodic_week_day);
		pstmt.setInt(23, n.periodic_month_day);
		pstmt.setInt(24, n.periodic_month);
		pstmt.setInt(25, n.r_id);
		
		pstmt.executeUpdate();
	}

	/*
	 * Update a record to the forwarding table
	 * A password may need to be updated if the target is a webhook
	 */
	public void updateNotification(Connection sd, PreparedStatement pstmt, String user, 
			Notification n, String tz) throws Exception {

		String sql = null;
		if(n.update_password) {
			sql = "update forward set "
					+ "s_id = ?, "
					+ "enabled = ?, "
					+ "remote_s_id = ?, "
					+ "remote_s_name = ?, "
					+ "remote_host = ?, "
					+ "remote_user = ?, "
					+ "notify_details = ?, "
					+ "trigger = ?, "
					+ "target = ?, "
					+ "filter = ?, "
					+ "name = ?, "
					+ "tg_id = ?, "
					+ "period = ?, "
					+ "update_survey = ?, "
					+ "update_question = ?, "
					+ "update_value = ?,"
					+ "alert_id = ?,"
					+ "remote_password = ? "
					+ "p_id = ?, "
					+ "periodic_time = ?, "
					+ "periodic_period = ?, "
					+ "periodic_day_of_week = ?, "
					+ "periodic_day_of_month = ?, "
					+ "periodic_month = ?, "
					+ "r_id = ? "
					+ "where id = ?";
		} else {
			sql = "update forward set "
					+ "s_id = ?, "
					+ "enabled = ?, "
					+ "remote_s_id = ?, "
					+ "remote_s_name = ?, "
					+ "remote_host = ?, "
					+ "remote_user = ?, "
					+ "notify_details = ?, "
					+ "trigger = ?, "
					+ "target = ?, "
					+ "filter = ?, "
					+ "name = ?, "
					+ "tg_id = ?, "
					+ "period = ?, "
					+ "update_survey = ?, "
					+ "update_question = ?, "
					+ "update_value = ?, "
					+ "alert_id = ?, "
					+ "p_id = ?, "
					+ "periodic_time = ?, "
					+ "periodic_period = ?, "
					+ "periodic_day_of_week = ?, "
					+ "periodic_day_of_month = ?, "
					+ "periodic_month = ?, "
					+ "r_id = ? "
					+ "where id = ?";
		}

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String notifyDetails = gson.toJson(n.notifyDetails);

		int idx = 1;
		try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
		pstmt = sd.prepareStatement(sql);	 			
		pstmt.setInt(idx++, n.s_id);
		pstmt.setBoolean(idx++, n.enabled);
		pstmt.setString(idx++, n.remote_s_ident);
		pstmt.setString(idx++, n.remote_s_name);
		pstmt.setString(idx++, n.remote_host);
		pstmt.setString(idx++, n.remote_user);
		pstmt.setString(idx++, notifyDetails);
		pstmt.setString(idx++, n.trigger);
		pstmt.setString(idx++, n.target);
		pstmt.setString(idx++, n.filter);
		pstmt.setString(idx++, n.name);
		pstmt.setInt(idx++, n.tgId);
		pstmt.setString(idx++, n.period);
		pstmt.setString(idx++, n.updateSurvey);
		pstmt.setString(idx++, n.updateQuestion);
		pstmt.setString(idx++, n.updateValue);
		pstmt.setInt(idx++, n.alert_id);
		if(n.update_password) {
			pstmt.setString(idx++, n.remote_password);
		} 
		
		/*
		 * Periodic Values
		 */
		pstmt.setInt(idx++, n.p_id);
		pstmt.setTime(idx++, GeneralUtilityMethods.convertTimeUtc(n.periodic_time, tz));
		pstmt.setString(idx++, n.periodic_period);
		pstmt.setInt(idx++, n.periodic_week_day);
		pstmt.setInt(idx++, n.periodic_month_day);
		pstmt.setInt(idx++, n.periodic_month);	
		pstmt.setInt(idx++, n.r_id);		
		pstmt.setInt(idx++, n.id);
		
		log.info("Update Notifications: " + pstmt.toString());
		pstmt.executeUpdate();
	}

	/*
	 * Get all Notifications that are accessible by the requesting user and in a specific project
	 */
	public ArrayList<Notification> getProjectNotifications(Connection sd, PreparedStatement pstmt,
			String user,
			int projectId,
			String tz) throws Exception {

		ArrayList<Notification> notifications = new ArrayList<Notification>();	// Results of request

		ResultSet resultSet = null;
		String sql = "select f.id, f.s_id, f.enabled, "
				+ "f.remote_s_id, f.remote_s_name, f.remote_host, f.remote_user,"
				+ "f.trigger, f.target, s.display_name, f.notify_details, f.filter, f.name,"
				+ "f.tg_id, f.period, f.update_survey, f.update_question, f.update_value, f.alert_id,"
				+ "f.p_id, f.periodic_time, f.periodic_period, f.periodic_day_of_week, f.periodic_day_of_month, f.periodic_month, f.r_id,"
				+ "a.name as alert_name "
				+ "from forward f "
				+ "left outer join survey s "
				+ "on s.s_id = f.s_id "
				+ "left outer join cms_alert a "
				+ "on a.id = f.alert_id "
				+ "where (f.p_id = ? "
				+ "or f.s_id in (select s_id from survey s where s.p_id = ? and not s.deleted)) "
				+ "order by f.name, s.display_name asc";

		try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
		pstmt = sd.prepareStatement(sql);	 			

		pstmt.setInt(1, projectId);
		pstmt.setInt(2, projectId);
		log.info("Project Notifications: " + pstmt.toString());
		resultSet = pstmt.executeQuery();

		addToList(sd, resultSet, notifications, false, false, tz);

		return notifications;

	}

	/*
	 * Get all Notifications in the users organisation
	 */
	public ArrayList<Notification> getOrganisationNotifications(
			Connection sd, 
			PreparedStatement pstmt,
			int oId,
			String tz) throws Exception {

		ArrayList<Notification> notifications = new ArrayList<Notification>();	// Results of request

		ResultSet resultSet = null;
		String sql = "select f.id, f.s_id, f.enabled, "
				+ "f.remote_s_id, f.remote_s_name, f.remote_host, f.remote_user,"
				+ "f.trigger, f.target, s.display_name, f.notify_details, f.filter, f.name,"
				+ "f.tg_id, f.period, f.update_survey,"
				+ "f.update_question, f.update_value,"
				+ "p.name as project_name, f.alert_id, a.name as alert_name, "
				+ "f.p_id, f.periodic_time, f.periodic_period, f.periodic_day_of_week, f.periodic_day_of_month, f.periodic_month, f.r_id "
				+ "from forward f "
				+ "left outer join survey s "
				+ "on s.s_id = f.s_id "
				+ "left outer join project p "
				+ "on s.p_id = p.id "
				+ "left outer join cms_alert a "
				+ "on a.id = f.alert_id "
				+ "where f.p_id in (select id from project where o_id = ?) "
					+ "or f.s_id in (select s.s_id from survey s, project p where s.p_id = p.id and p.o_id = ?) "
				+ "and s.deleted = 'false' "
				+ "order by p.name, f.name, s.display_name asc";

		try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
		pstmt = sd.prepareStatement(sql);	 			

		pstmt.setInt(1, oId);
		pstmt.setInt(2, oId);
		log.info("All Notifications: " + pstmt.toString());
		resultSet = pstmt.executeQuery();

		addToList(sd, resultSet, notifications, false, true, tz);

		return notifications;

	}

	/*
	 * Get a list of notification types
	 */
	public ArrayList<String> getNotificationTypes(Connection sd, String user) throws SQLException {

		ArrayList<String> types = new ArrayList<>();

		PreparedStatement pstmt = null;
		String sql = "select s.sms_url, s.document_sync from server s";

		PreparedStatement orgLevelPstmt = null;
		String sqlOrgLevel = "select o.can_sms from organisation o, users u "
				+ "where o.id = u.o_id "
				+ "and u.ident = ?";

		types.add("email");
		types.add("webhook");
		types.add("escalate");

		boolean awsSMS = false;

		try {

			pstmt = sd.prepareStatement(sql);	 			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String smsUrl = rs.getString("sms_url");
				if(smsUrl != null) {
					if(smsUrl.trim().length() > 0  && !smsUrl.equals("aws")) {
						types.add("sms");
					} else if(smsUrl.equals("aws")) {
						awsSMS = true;
					}
				}
				if(rs.getBoolean("document_sync")) {
					types.add("document");
				}
			}

			// If SMS is enabled using AWS then check users organisation for SMS being enabled
			if(awsSMS) {
				orgLevelPstmt = sd.prepareStatement(sqlOrgLevel);
				orgLevelPstmt.setString(1, user);
				log.info("Check for SMS: " + orgLevelPstmt.toString());
				rs = orgLevelPstmt.executeQuery();
				if(rs.next()) {
					if(rs.getBoolean(1)) {
						types.add("sms");
					}
				}
			}

		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (orgLevelPstmt != null) {orgLevelPstmt.close();}} catch (SQLException e) {}
		}

		return types;

	}

	/*
	 * Delete the notification
	 */
	public void deleteNotification
	(Connection sd,
			String user,
			int id) throws SQLException {

		String nName = GeneralUtilityMethods.getNotificationName(sd, id);

		String sql = "delete from forward where id = ?; ";
		PreparedStatement pstmt = null;

		try {

			// Delete
			pstmt = sd.prepareStatement(sql);	 			
			pstmt.setInt(1, id);
			log.info("Delete: " + pstmt.toString());
			pstmt.executeUpdate();

			// Log the delete event
			String logMessage = localisation.getString("lm_del_notification");
			if(nName == null) {
				nName = "";
			}
			logMessage = logMessage.replaceAll("%s1", nName);
			lm.writeLog(sd, 0, user, LogManager.DELETE, logMessage, 0, null);
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}

	}

	private void addToList(Connection sd, 
			ResultSet resultSet, 
			ArrayList<Notification> notifications, 
			boolean getPassword,
			boolean getProject,
			String tz) throws Exception {

		while (resultSet.next()) {								

			Notification n = new Notification();
			n.id = resultSet.getInt("id");
			n.s_id = resultSet.getInt("s_id");
			n.enabled = resultSet.getBoolean("enabled");
			String remote_s_id = resultSet.getString("remote_s_id");
			n.remote_s_ident = remote_s_id;
			n.remote_s_name = resultSet.getString("remote_s_name");
			n.remote_host = resultSet.getString("remote_host");
			n.remote_user = resultSet.getString("remote_user");
			n.trigger = resultSet.getString("trigger");
			n.target = resultSet.getString("target");
			n.s_name = resultSet.getString("display_name");
			String notifyDetailsString = resultSet.getString("notify_details");
			n.notifyDetails = new Gson().fromJson(notifyDetailsString, NotifyDetails.class);
			// Temporary - set question name from question id if this is set
			if(n.notifyDetails != null && (n.notifyDetails.emailQuestionName == null || n.notifyDetails.emailQuestionName.equals("-1")) && n.notifyDetails.emailQuestion > 0) {
				n.notifyDetails.emailQuestionName = GeneralUtilityMethods.getQuestionNameFromId(sd, n.s_id, n.notifyDetails.emailQuestion);
			}
			n.filter = resultSet.getString("filter");
			n.name = resultSet.getString("name");
			n.tgId = resultSet.getInt("tg_id");
			n.period = resultSet.getString("period");
			n.updateSurvey = resultSet.getString("update_survey");
			n.updateQuestion = resultSet.getString("update_question");
			n.updateValue = resultSet.getString("update_value");
			n.alert_id = resultSet.getInt("alert_id");
			n.alert_name = resultSet.getString("alert_name");
			n.p_id = resultSet.getInt("p_id");
			n.periodic_time = GeneralUtilityMethods.convertTimeLocal(resultSet.getTime("periodic_time"), tz);
			n.periodic_period = resultSet.getString("periodic_period");
			n.periodic_week_day = resultSet.getInt("periodic_day_of_week");
			n.periodic_month_day = resultSet.getInt("periodic_day_of_month");
			n.periodic_month = resultSet.getInt("periodic_month");
			n.r_id = resultSet.getInt("r_id");
			if(getPassword) {
				n.remote_password = resultSet.getString("remote_password");
			}
			if(getProject) {
				n.project = resultSet.getString("project_name");
				if(n.project == null & n.p_id > 0) {
					/*
					 * This is a hack because previously it was assumed that all notification entries and that is the path
					 * by which the project name is determined.  However not all do now. For example periodic notifications. 
					 */
					n.project = GeneralUtilityMethods.getProjectName(sd, n.p_id);
				}
			}

			if(n.trigger.equals("task_reminder")) {
				n.tg_name = GeneralUtilityMethods.getTaskGroupName(sd, n.tgId);
			}

			notifications.add(n);

		}
	}

	/*
	 * Apply any notification for the passed in submission
	 * or console update
	 */
	public void notifyForSubmission(
			Connection sd, 
			Connection cResults,
			int ue_id,
			String submittingUser,
			boolean temporaryUser,
			String scheme,
			String serverName,
			String basePath,
			String serverRoot,
			String ident,			// Survey Ident
			String instanceId,
			int pId,
			String updateSurvey,
			String updateQuestion,
			String updateValue) throws Exception {

		/*
		 * 1. Get notifications that may apply to the passed in upload event.
		 * 		Notifications can be re-applied so the the notifications flag in upload event is ignored
		 * 2. Apply any additional filtering
		 * 3. Invoke each notification
		 *    3a) Create document
		 *    3b) Send document to target
		 *    3c) Update notification log
		 * 4. Update upload event table to show that notifications have been applied
		 */

		ResultSet rsNotifications = null;		
		PreparedStatement pstmtGetNotifications = null;
		PreparedStatement pstmtUpdateUploadEvent = null;

		int sId = GeneralUtilityMethods.getSurveyId(sd, ident);

		try {

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			MessagingManager mm = new MessagingManager(localisation);
			int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);

			log.info("notifyForSubmission:: " + ue_id + " : " + updateQuestion + " : " + updateValue);

			StringBuffer sqlGetNotifications = new StringBuffer("select n.target, n.notify_details, n.filter, n.remote_user, n.remote_password "
					+ "from forward n "
					+ "where n.s_id = ? " 
					+ "and n.target != 'forward' "
					+ "and n.target != 'document' "
					+ "and n.enabled = 'true'");

			if(updateQuestion == null) {
				sqlGetNotifications.append(" and n.trigger = 'submission'");
			} else {
				sqlGetNotifications.append(" and n.trigger = 'console_update'");
				sqlGetNotifications.append(" and n.update_survey = ?");
				sqlGetNotifications.append(" and n.update_question = ?");
				sqlGetNotifications.append(" and n.update_value = ?");
			}
			pstmtGetNotifications = sd.prepareStatement(sqlGetNotifications.toString());

			// Localisation
			Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
			Locale locale = new Locale(organisation.locale);

			ResourceBundle localisation;
			try {
				localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			} catch(Exception e) {
				localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", locale);
			}

			String tz = "UTC";		// Set default time to UTC

			pstmtGetNotifications.setInt(1, sId);
			if(updateQuestion != null) {
				pstmtGetNotifications.setString(2, updateSurvey);
				pstmtGetNotifications.setString(3, updateQuestion);
				pstmtGetNotifications.setString(4, updateValue);
			}
			log.info("Get notifications:: " + pstmtGetNotifications.toString());
			rsNotifications = pstmtGetNotifications.executeQuery();
			while(rsNotifications.next()) {

				String target = rsNotifications.getString(1);
				String notifyDetailsString = rsNotifications.getString(2);
				String filter = rsNotifications.getString(3);
				String remoteUser = rsNotifications.getString(4);
				String remotePassword = rsNotifications.getString(5);
				NotifyDetails nd = new Gson().fromJson(notifyDetailsString, NotifyDetails.class);

				/*
				 * Get survey details
				 */
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				Survey survey = sm.getById(
						sd, 
						cResults, 
						submittingUser,
						temporaryUser,
						sId, 
						true, 
						basePath, 
						instanceId, true, false, true, false, true, "real", 
						false, false, 
						true, 			// pretend to be super user
						"geojson",
						false,			// Do not follow links to child surveys
						false,	// launched only
						false		// Don't merge set value into default values
						);	

				/*
				 * Test the filter
				 */
				boolean proceed = true;
				if(filter != null && filter.trim().length() > 0) {
					try {
						proceed = GeneralUtilityMethods.testFilter(sd, cResults, submittingUser, localisation, survey, filter, instanceId, tz, "PdfTemplate Selection");
					} catch(Exception e) {
						String msg = e.getMessage();
						if(msg == null) {
							msg = "";
						}
						log.log(Level.SEVERE, e.getMessage(), e);
						lm.writeLog(sd, sId, "subscriber", LogManager.NOTIFICATION_ERROR, 
								localisation.getString("filter_error")
								.replace("%s1", filter)
								.replace("%s2", msg), 0, null);
					}
				}

				if(!proceed) {
					log.info("Notification not sent because of filter rule: " + filter);
				} else {

					if(nd.attach != null && nd.attach.equals("pdf")) {
						// Get the id of a PDF template that matches the template rules or set it to 0
						nd.pdfTemplateId = GeneralUtilityMethods.testForPdfTemplate(sd, cResults, localisation, survey, submittingUser,
								instanceId, tz);
					}
					SubmissionMessage subMsg = new SubmissionMessage(
							0,				// Task Id - ignore, only relevant for a reminder
							ident,			// Survey Ident
							updateSurvey,
							pId,
							instanceId, 
							nd.from,
							nd.subject, 
							nd.content,
							nd.attach,
							nd.include_references,
							nd.launched_only,
							nd.emailQuestion,
							nd.emailQuestionName,
							nd.emailMeta,
							nd.emailAssigned,
							nd.emails,
							target,
							submittingUser,
							scheme,
							serverName,
							basePath,
							nd.callback_url,
							remoteUser,
							remotePassword,
							nd.pdfTemplateId,
							nd.survey_case,
							nd.assign_question);
					mm.createMessage(sd, oId, "submission", "", gson.toJson(subMsg));

					lm.writeLog(sd, sId, "subscriber", LogManager.NOTIFICATION, 
							localisation.getString("filter_applied")
							.replace("%s1", survey.displayName)
							.replace("%s2", filter)
							.replace("%s3", instanceId), 0, null);

				}
			}

			/*
			 * Update upload event to record application of notifications
			 */
			if(updateQuestion == null) {
				String sqlUpdateUploadEvent = "update upload_event set notifications_applied = 'true' where ue_id = ?; ";
				pstmtUpdateUploadEvent = sd.prepareStatement(sqlUpdateUploadEvent);
				pstmtUpdateUploadEvent.setInt(1, ue_id);
				pstmtUpdateUploadEvent.executeUpdate();
			}
		} finally {
			try {if (pstmtGetNotifications != null) {pstmtGetNotifications.close();}} catch (SQLException e) {}
			try {if (pstmtUpdateUploadEvent != null) {pstmtUpdateUploadEvent.close();}} catch (SQLException e) {}
		}

	}

	private class SendEmailResponse {
		String notify_details = null;
		String status = null;
		String error_details = null;
		boolean writeToMonitor = true;
	}
	
	/*
	 * Process a submission notification
	 * Has access to the data from the submitted survey
	 */
	public void processSubmissionNotification(Connection sd, 
			Connection cResults, 
			Organisation organisation,
			String tz,
			SubmissionMessage msg,
			int messageId,
			String topic,
			boolean createPending) throws Exception {

		String docURL = null;
		String filePath = null;
		String filename = "instance";
		String logContent = null;

		boolean writeToMonitor = true;
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();


		boolean generateBlank =  (msg.instanceId == null) ? true : false;	// If false only show selected options

		PreparedStatement pstmtGetSMSUrl = null;

		SurveyManager sm = new SurveyManager(localisation, "UTC");
		DataManager dm = new DataManager(localisation, "UTC");
		int surveyId = GeneralUtilityMethods.getSurveyId(sd, msg.survey_ident);

		Survey survey = sm.getById(sd, cResults, null, false, surveyId, true, msg.basePath, 
				msg.instanceId, true, generateBlank, true, false, true, "real", 
				false, false, true, "geojson",
				msg.include_references,	// For PDFs follow links to referenced surveys
				msg.launchedOnly,			// launched only
				false		// Don't merge set value into default values
				);

		Survey updateSurvey = null;
		if(msg.update_ident != null) {
			int oversightId = GeneralUtilityMethods.getSurveyId(sd, msg.update_ident);
			updateSurvey = sm.getById(sd, cResults, null, false, oversightId, true, msg.basePath, 
					msg.instanceId, true, generateBlank, true, false, true, "real", 
					false, false, true, "geojson",
					msg.include_references,		// For PDFs follow links to referenced surveys
					msg.launchedOnly,			// launched only
					false		// Don't merge set value into default values
					);
		}

		PDFSurveyManager pm = new PDFSurveyManager(localisation, sd, cResults, survey, msg.user, organisation.timeZone);

		try {

			// Notification log
			ArrayList<String> unsubscribedList  = new ArrayList<> ();
			String error_details = null;
			String notify_details = null;
			String status = null;

			if(organisation.can_notify) {

				/*
				 * Add details from the survey to the subject and email content
				 */
				msg.subject = sm.fillStringTemplate(survey, msg.subject);
				msg.content = sm.fillStringTemplate(survey, msg.content);
				TextManager tm = new TextManager(localisation, organisation.timeZone);
				ArrayList<String> text = new ArrayList<> ();
				text.add(msg.subject);
				text.add(msg.content);
				tm.createTextOutput(sd,
						cResults,
						text,
						msg.basePath, 
						msg.user,
						survey,
						"none",
						organisation.id);
				// Update text with oversight data if it exists
				if(updateSurvey != null) {
					tm.createTextOutput(sd,
							cResults,
							text,
							msg.basePath, 
							msg.user,
							updateSurvey,
							"none",
							organisation.id);
				}
				msg.subject = text.get(0);
				msg.content = text.get(1);

				if(msg.attach != null && !msg.attach.equals("none")) {

					if(msg.attach.startsWith("pdf")) {
						docURL = null;

						// Create temporary PDF and get file name
						filePath = msg.basePath + "/temp/" + String.valueOf(UUID.randomUUID()) + ".pdf";
						FileOutputStream outputStream = null;
						try {
							outputStream = new FileOutputStream(filePath); 
						} catch (Exception e) {
							log.log(Level.SEVERE, "Error creating temporary PDF file", e);
						}

						// Split orientation from nd.attach
						boolean landscape = false;
						if(msg.attach != null && msg.attach.startsWith("pdf")) {
							landscape = msg.attach.equals("pdf_landscape");
							msg.attach = "pdf";
						}

						String urlprefix = "https://" + msg.server + "/";

						filename = pm.createPdf(
								outputStream,
								msg.basePath, 
								urlprefix,
								msg.user,
								"none", 
								msg.pdfTemplateId,
								generateBlank,
								null,
								landscape,
								null);

						outputStream.close();
						if(survey.compress_pdf) {
							// Compress the temporary file and write it toa new temporary file
							String compressedPath = msg.basePath + "/temp/" + String.valueOf(UUID.randomUUID()) + ".pdf";	// New temporary file
							try {
								outputStream = new FileOutputStream(compressedPath); 
							} catch (Exception e) {
								log.log(Level.SEVERE, "Error creating temporary PDF file", e);
							}
							PdfUtilities.resizePdf(filePath, outputStream);
							outputStream.close();
							filePath = compressedPath;
						}
						logContent = filePath;

					} else {
						docURL = "/webForm/" + msg.survey_ident +
								"?datakey=instanceid&datakeyvalue=" + msg.instanceId;
						logContent = docURL;
					}
				} 

				/*
				 * Send document to target
				 */
				status = "success";					// Notification log
				notify_details = null;				// Notification log
				error_details = null;				// Notification log
				if(msg.target.equals("email")) {
					
					SendEmailResponse resp = sendEmails(sd, cResults, msg, organisation, surveyId, logContent, docURL, survey, unsubscribedList,
							filePath, filename, messageId, createPending, topic);
					
					notify_details = resp.notify_details;
					status = resp.status;
					error_details = resp.error_details;
					writeToMonitor = resp.writeToMonitor;
					
				} else if(msg.target.equals("sms")) {   // SMS URL notification - SMS message is posted to an arbitrary URL 

					// Get the URL to use in sending the SMS
					HashMap<String, String> sentEndPoints = new HashMap<> ();
					
					String sql = "select s.sms_url "
							+ "from server s";

					String sms_url = null;
					pstmtGetSMSUrl = sd.prepareStatement(sql);
					ResultSet rs = pstmtGetSMSUrl.executeQuery();
					if(rs.next()) {
						sms_url = rs.getString("sms_url");	
					}

					if(sms_url != null) {
						ArrayList<String> smsList = null;
						ArrayList<String> responseList = new ArrayList<> ();
						log.info("SMS question: " + msg.getEmailQuestionName(sd));
						if(msg.emailQuestionSet()) {
							smsList = GeneralUtilityMethods.getResponseForQuestion(sd, cResults, surveyId, msg.getEmailQuestionName(sd), msg.instanceId);
						} else {
							smsList = new ArrayList<String> ();
						}

						// Add the static sms numbers to the per question sms numbers
						for(String sms : msg.emails) {
							if(sms.length() > 0) {
								log.info("Adding static sms: " + sms); 
								smsList.add(sms);
							}
						}

						// Write the combined list back into the emails field, if something goes wrong this is the 
						// list of numbers that we attempted to contact
						msg.emails = smsList;

						if(smsList.size() > 0) {

							EmitSMS smsMgr = null;
							if(sms_url.equals("aws")) {
								String sender_id = "Smap";
								if(msg.subject != null && msg.subject.trim().length() > 0) {
									sender_id = msg.subject;
								}
								smsMgr = new EmitAwsSMS(sender_id, localisation);
							} else {
								smsMgr = new SMSExternalManager(sms_url, localisation);
							}

							for(String sms : smsList) {

								if(sentEndPoints.get(sms) == null) {
									log.info("userevent: " + msg.user + " sending sms of '" + msg.content + "' to " + sms);
									try {
										responseList.add(smsMgr.sendSMS(sms, msg.content));
									} catch (Exception e) {
										status = "error";
										error_details = e.getMessage();
									}
									sentEndPoints.put(sms, sms);
								} else {
									log.info("Duplicate phone number: " + sms);
								}

							} 
						} else {
							log.info("No phone numbers to send to");
							writeToMonitor = false;
						}

						notify_details = "Sending sms " + smsList.toString() 
						+ ((logContent == null || logContent.equals("null")) ? "" :" containing link " + logContent)
						+ " with response " + responseList.toString();

					} else {
						status = "error";
						error_details = "SMS URL not set";
						log.log(Level.SEVERE, "Error: Attempt to do SMS notification but SMS URL not set");
					}


				} else if(msg.target.equals("webhook")) {   // webhook call

					log.info("+++++ webhook call");
					notify_details = localisation.getString("cb_nd");
					notify_details = notify_details.replaceAll("%s1", msg.callback_url);
					notify_details = notify_details.replaceAll("%s2", survey.displayName);
					notify_details = notify_details.replaceAll("%s3", survey.projectName);

					try {
						JSONArray data = dm.getInstanceData(
								sd,
								cResults,
								survey,
								survey.getFirstForm(),
								0,
								null,
								msg.instanceId,
								sm,
								true);
						String resp = "{}";
						if(data.length() > 0) {
							resp = data.getString(0).toString();
						}

						WebhookManager wm = new WebhookManager(localisation);
						wm.callRemoteUrl(msg.callback_url, resp, msg.remoteUser, msg.remotePassword);

					} catch (Exception e) {
						status = "error";
						error_details = e.getMessage();
						log.log(Level.SEVERE, e.getMessage(), e);
					}
					
				} else if(msg.target.equals("escalate")) {   // Assign a user to a case

					String assignTo = msg.remoteUser;
					if(msg.remoteUser.equals("_submitter")) {
						assignTo = msg.user;
					} else if(msg.remoteUser.equals("_data")) {
						ArrayList<String> userList = GeneralUtilityMethods.getResponseForQuestion(sd, cResults, surveyId, msg.assignQuestion, msg.instanceId);
						if(userList.size() > 0) {
							assignTo = userList.get(0);	// Only one user can be assigned
						}
					}
					
					log.info("+++++ escalate notification");
					notify_details = localisation.getString("esc_nd");
					notify_details = notify_details.replaceAll("%s1", msg.instanceId);
					notify_details = notify_details.replaceAll("%s2", survey.displayName);
					notify_details = notify_details.replaceAll("%s3", survey.projectName);
					notify_details = notify_details.replaceAll("%s4", assignTo);

					try {
						String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, msg.survey_ident);
						String surveyCase = msg.survey_case;
						if(surveyCase == null) {	// if no survey to complete has been specified then complete with the submitting survey
							surveyCase = msg.survey_ident;
						}
						
						
						int count = GeneralUtilityMethods.assignRecord(sd, cResults, localisation, tableName, msg.instanceId, assignTo, "assign", surveyCase, notify_details);
						if(count == 0) {
							status = "error";
							error_details = "case not found, attempting: " + notify_details;
							log.log(Level.SEVERE, "Error: " + error_details);
						} 
					} catch (Exception e) {
						status = "error";
						error_details = e.getMessage();
						log.log(Level.SEVERE, e.getMessage(), e);
					}
					
					/*
					 * Send emails associated with this escalation
					 */
					sendEmails(sd, cResults, msg, organisation, surveyId, logContent, docURL, survey, unsubscribedList,
							filePath, filename, messageId, createPending, topic);
					
				} else {
					status = "error";
					error_details = "Invalid target: " + msg.target;
					log.log(Level.SEVERE, "Error: Invalid target" + msg.target);
				}

			} else {
				notify_details = organisation.name;
				status = "error";
				error_details = localisation.getString("susp_notify");
				log.log(Level.SEVERE, "Error: notification services suspended");
			}

			// Write log message
			if(writeToMonitor) {
				if(!unsubscribedList.isEmpty()) {
					if(error_details == null) {
						error_details = "";
					}
					error_details += localisation.getString("c_unsubscribed") + ": " + String.join(",", unsubscribedList);
				}
				writeToLog(sd, organisation.id, msg.pId, surveyId, notify_details, status, 
						error_details, messageId);

				/*
				 * Write log entry
				 */
				String logTopic;
				if(status != null && status.toLowerCase().equals("error")) {
					logTopic = LogManager.NOTIFICATION_ERROR;
				} else {
					logTopic = LogManager.NOTIFICATION;
				}

				lm.writeLog(sd, surveyId, "subscriber", logTopic, status + " : " + notify_details + (error_details == null ? "" : error_details), 0, null);
			}

			/*
			 * If this notification is for a record then update the Record Event Manager
			 */
			if(msg.instanceId != null) {
				RecordEventManager rem = new RecordEventManager();
				String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, msg.survey_ident);

				rem.writeEvent(
						sd, 
						cResults, 
						RecordEventManager.NOTIFICATION, 
						status, 
						msg.user, 
						tableName, 
						msg.instanceId, 
						null, 
						null, 
						gson.toJson(msg),
						error_details, 
						0, 
						msg.survey_ident,
						0,
						0);
			}

		} finally {
			try {if (pstmtGetSMSUrl != null) {pstmtGetSMSUrl.close();}} catch (SQLException e) {}

		}
	}


	/*
	 * Process a reminder
	 * Has access to data from the task
	 */
	public void processReminderNotification(Connection sd, 
			Connection cResults, 
			Organisation organisation,
			int o_id,
			String tz,
			SubmissionMessage msg,
			int messageId,
			String topic,
			boolean createPending) throws Exception {

		String logContent = null;

		log.info("----------- Process Reminder Notification");
		
		boolean writeToMonitor = true;

		MessagingManager mm = new MessagingManager(localisation);
		PreparedStatement pstmtGetSMSUrl = null;


		String urlprefix = msg.scheme + "://" + msg.server;
		TaskManager tm = new TaskManager(localisation, tz);
		TaskListGeoJson t = tm.getTasks(
				sd, 
				urlprefix,
				0,		// Organisation id 
				0, 		// task group id
				msg.taskId,
				0,		// Assignment id
				true, 
				0,		// userId 
				null, 
				null,	// period 
				0,		// start 
				0,		// limit
				null,	// sort
				null);	// sort direction	

		TaskProperties task = null;
		if(t != null && t.features.size() > 0) {
			task = t.features.get(0).properties;
		}

		try {

			// Notification log
			ArrayList<String> unsubscribedList  = new ArrayList<> ();
			String error_details = null;
			String notify_details = null;
			String status = null;

			int surveyId = GeneralUtilityMethods.getSurveyId(sd, msg.survey_ident);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");

			Survey survey = sm.getById(sd, cResults, null, false, surveyId, true, msg.basePath, 
					msg.instanceId, true, false, true, false, true, "real", 
					false, false, true, "geojson",
					msg.include_references,	// For PDFs follow links to referenced surveys
					msg.launchedOnly,			// launched only
					false		// Don't merge set value into default values
					);

			if(organisation.can_notify) {

				msg.subject = tm.fillStringTaskTemplate(task, msg, msg.subject);
				msg.content = tm.fillStringTaskTemplate(task, msg, msg.content);

				/*
				 * Send document to target
				 */
				status = "success";					// Notification log
				notify_details = null;				// Notification log
				error_details = null;				// Notification log
				if(msg.target.equals("email")) {
					EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, msg.user, o_id);
					if(emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {

						String emails = getEmails(sd, cResults, surveyId, msg);   // Get the email addresses from the message

						if(emails.trim().length() > 0) {
							log.info("userevent: " + msg.user + " sending email of '" + logContent + "' to " + emails);

							// Set the subject
							String subject = "";
							if(msg.subject != null && msg.subject.trim().length() > 0) {
								subject = msg.subject;
							} else {
								if(msg.server != null && msg.server.contains("smap")) {
									subject = "Smap ";
								}
								subject += localisation.getString("c_notify");
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

							notify_details = "Sending email to: " + emails + " containing link " + logContent;
							notify_details = localisation.getString("msg_er");
							notify_details = notify_details.replaceAll("%s1", emails);
							notify_details = notify_details.replaceAll("%s2", "-");			// No attachments in reminder
							notify_details = notify_details.replaceAll("%s3", survey.displayName);
							notify_details = notify_details.replaceAll("%s4", survey.projectName);

							log.info("+++ emailing reminder to: " + emails + " docUrl: " + logContent + 
									" from: " + from + 
									" subject: " + subject +
									" smtp_host: " + emailServer.smtpHost +
									" email_domain: " + emailServer.emailDomain);
							try {
								EmailManager em = new EmailManager();
								PeopleManager peopleMgr = new PeopleManager(localisation);
								InternetAddress[] emailArray = InternetAddress.parse(emails);

								for(InternetAddress ia : emailArray) {		
									SubscriptionStatus subStatus = peopleMgr.getEmailKey(sd, organisation.id, ia.getAddress());						
									if(subStatus.unsubscribed) {
										unsubscribedList.add(ia.getAddress());		// Person has unsubscribed
									} else {
										if(subStatus.optedIn || !organisation.send_optin) {
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
													null, 
													null,
													null,
													organisation.getAdminEmail(), 
													emailServer,
													msg.scheme,
													msg.server,
													subStatus.emailKey,
													localisation,
													organisation.server_description,
													organisation.name);
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
													msg.scheme,
													msg.server,
													messageId);
										}
									}
								}
							} catch(Exception e) {
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
						log.log(Level.SEVERE, "Error: Notification, Attempt to do email notification but email server not set");
					}

				} else if(msg.target.equals("sms")) {   // SMS URL notification - SMS message is posted to an arbitrary URL 

					HashMap<String, String> sentEndPoints = new HashMap<> ();
					
					// Get the URL to use in sending the SMS
					String sql = "select s.sms_url "
							+ "from server s";

					String sms_url = null;
					pstmtGetSMSUrl = sd.prepareStatement(sql);
					ResultSet rs = pstmtGetSMSUrl.executeQuery();
					if(rs.next()) {
						sms_url = rs.getString("sms_url");	
					}

					if(sms_url != null) {
						ArrayList<String> smsList = null;
						ArrayList<String> responseList = new ArrayList<> ();
						log.info("SMS question: " + msg.getEmailQuestionName(sd));
						if(msg.emailQuestionSet()) {
							smsList = GeneralUtilityMethods.getResponseForQuestion(sd, cResults, surveyId, msg.getEmailQuestionName(sd), msg.instanceId);
						} else {
							smsList = new ArrayList<String> ();
						}

						// Add the static sms numbers to the per question sms numbers
						for(String sms : msg.emails) {
							if(sms.length() > 0) {
								log.info("Adding static sms: " + sms); 
								smsList.add(sms);
							}
						}

						if(smsList.size() > 0) {

							EmitSMS smsMgr = null;
							if(sms_url.equals("aws")) {
								String sender_id = "Smap";
								if(msg.subject != null && msg.subject.trim().length() > 0) {
									sender_id = msg.subject;
								}
								smsMgr = new EmitAwsSMS(sender_id, localisation);
							} else {
								smsMgr = new SMSExternalManager(sms_url, localisation);
							}

							for(String sms : smsList) {

								if(sentEndPoints.get(sms) == null) {
									log.info("userevent: " + msg.user + " sending sms of '" + msg.content + "' to " + sms);
									responseList.add(smsMgr.sendSMS(sms, msg.content));
									sentEndPoints.put(sms, sms);
								} else {
									log.info("Duplicate phone number: " + sms);
								}

							} 
						} else {
							log.info("No phone numbers to send to");
							writeToMonitor = false;
						}

						notify_details = "Sending sms " + smsList.toString() 
						+ ((logContent == null || logContent.equals("null")) ? "" :" containing link " + logContent)
						+ " with response " + responseList.toString();

					} else {
						status = "error";
						error_details = "SMS URL not set";
						log.log(Level.SEVERE, "Error: Attempt to do SMS notification but SMS URL not set");
					}


				} else {
					status = "error";
					error_details = "Invalid target: " + msg.target;
					log.log(Level.SEVERE, "Error: Invalid target" + msg.target);
				}
			} else {
				notify_details = organisation.name;
				status = "error";
				error_details = localisation.getString("susp_notify");
				log.log(Level.SEVERE, "Error: notification services suspended");
			}

			// Write log message
			if(writeToMonitor) {
				if(!unsubscribedList.isEmpty()) {
					if(error_details == null) {
						error_details = "";
					}
					error_details += localisation.getString("c_unsubscribed") + ": " + String.join(",", unsubscribedList);
				}
				writeToLog(sd, organisation.id, msg.pId, surveyId, notify_details, status, 
						error_details, messageId);
			}
		} finally {
			try {if (pstmtGetSMSUrl != null) {pstmtGetSMSUrl.close();}} catch (SQLException e) {}

		}
	}

	public void writeToLog(Connection sd, int oId, int pId, int surveyId, String notify_details,
			String status, String error_details, int messageId) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "insert into notification_log " +
				"(o_id, p_id, s_id, notify_details, status, status_details, event_time, message_id, type) " +
				"values( ?, ?,?, ?, ?, ?, now(), ?, 'submission'); ";

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, pId);
			pstmt.setInt(3, surveyId);
			pstmt.setString(4, notify_details);
			pstmt.setString(5, status);
			pstmt.setString(6, error_details);
			pstmt.setInt(7, messageId);

			pstmt.executeUpdate();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}

	/*
	 * Get the email addresses from the message settings
	 */
	private String getEmails(Connection sd, Connection cResults, int surveyId, SubmissionMessage msg) throws Exception {
		
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
	
	private SendEmailResponse sendEmails(Connection sd, Connection cResults, SubmissionMessage msg, Organisation organisation, int surveyId, 
			String logContent,
			String docURL,
			Survey survey,
			ArrayList<String> unsubscribedList,
			String filePath,
			String filename,
			int messageId,
			boolean createPending,
			String topic) throws Exception {
		
		SendEmailResponse resp = new SendEmailResponse();;
		
		MessagingManager mm = new MessagingManager(localisation);
		EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, msg.user, organisation.id);
		if(emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {
			String emails = getEmails(sd, cResults, surveyId, msg);
			

			if(emails.trim().length() > 0) {
				log.info("userevent: " + msg.user + " sending email of '" + logContent + "' to " + emails);

				// Set the subject
				String subject = "";
				if(msg.subject != null && msg.subject.trim().length() > 0) {
					subject = msg.subject;
				} else {
					if(msg.server != null && msg.server.contains("smap")) {
						subject = "Smap ";
					}
					subject += localisation.getString("c_notify");
				}

				String from = "smap";
				if(msg.from != null && msg.from.trim().length() > 0) {
					from = msg.from;
				}
				StringBuilder content = null;
				if(msg.content != null && msg.content.trim().length() > 0) {
					content = new StringBuilder(msg.content);
				} else if(organisation.default_email_content != null && organisation.default_email_content.trim().length() > 0){
					content = new StringBuilder(organisation.default_email_content);
				} else {
					content = new StringBuilder(localisation.getString("email_ian"))
							.append(" " + msg.scheme + "://")
							.append(msg.server)
							.append(". ");
				}

				if(docURL != null) {
					content.append("<p style=\"color:blue;text-align:center;\">")
					.append("<a href=\"")
					.append(msg.scheme + "://")
					.append(msg.server)
					.append(docURL)
					.append("\">")
					.append(survey.displayName)
					.append("</a>")
					.append("</p>");
				}

				resp.notify_details = localisation.getString("msg_en");
				resp.notify_details = resp.notify_details.replaceAll("%s1", emails);
				if(logContent != null) {
					resp.notify_details = resp.notify_details.replaceAll("%s2", logContent);
				} else {
					resp.notify_details = resp.notify_details.replaceAll("%s2", "-");
				}
				resp.notify_details = resp.notify_details.replaceAll("%s3", survey.displayName);
				resp.notify_details = resp.notify_details.replaceAll("%s4", survey.projectName);

				log.info("+++ emailing to: " + emails + " docUrl: " + logContent + 
						" from: " + from + 
						" subject: " + subject +
						" smtp_host: " + emailServer.smtpHost +
						" email_domain: " + emailServer.emailDomain);
				try {
					EmailManager em = new EmailManager();
					PeopleManager peopleMgr = new PeopleManager(localisation);
					InternetAddress[] emailArray = InternetAddress.parse(emails);

					for(InternetAddress ia : emailArray) {	
						SubscriptionStatus subStatus = peopleMgr.getEmailKey(sd, organisation.id, ia.getAddress());				
						if(subStatus.unsubscribed) {
							unsubscribedList.add(ia.getAddress());		// Person has unsubscribed
						} else {
							if(subStatus.optedIn || !organisation.send_optin) {
								em.sendEmailHtml(
										ia.getAddress(),  
										"bcc", 
										subject, 
										content, 
										filePath,
										filename,
										emailServer,
										msg.server,
										subStatus.emailKey,
										localisation,
										null,
										organisation.getAdminEmail(),
										organisation.getEmailFooter());

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
										msg.scheme,
										msg.server,
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
			resp.error_details = "smtp_host not set";
			log.log(Level.SEVERE, "Error: Notification, Attempt to do email notification but email server not set");
		}
		
		return resp;
	}
}
