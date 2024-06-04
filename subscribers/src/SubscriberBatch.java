import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.Tables;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.ForeignKeyManager;
import org.smap.sdal.managers.LinkageManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.CaseManagementSettings;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.LinkageItem;
import org.smap.sdal.model.MailoutMessage;
import org.smap.sdal.model.NotifyDetails;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.ServerCalculation;
import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.server.entities.UploadEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import JdbcManagers.JdbcUploadEventManager;

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

public class SubscriberBatch {

	String confFilePath;

	DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
	DatabaseConnections dbc = new DatabaseConnections();

	private static Logger log =
			Logger.getLogger(SubscriberBatch.class.getName());

	private static LogManager lm = new LogManager();		// Application log
	
	HashMap<String, String> autoErrorCheck = new HashMap<> ();

	/**
	 * @param args
	 */
	public void go(String smapId, String basePath, String subscriberType) {

		confFilePath = "./" + smapId;

		// Get the connection details for the meta data database

		JdbcUploadEventManager uem = null;
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		String serverName = null;

		/*
		 * SQL to enqueue the submission
		 */
		String sqlEnqueue = "insert into submission_queue(element_identifier, time_inserted, ue_id, instanceid, restore, payload) "
				+ "values(gen_random_uuid(), current_timestamp, ?, ?, ?, ?::jsonb)";
		PreparedStatement pstmtEnqueue = null;
		
		/*
		 * SQL to prevent duplicates being processed in the queue in parallel
		 */
		String sqlDup = "select count(*) from submission_queue where instanceid = ?";
		PreparedStatement pstmtDup = null;
		
		/*
		 * SQL to inform the upload event table that the submission has been moved to the queue
		 */
		String sqlQueueDone = "update upload_event "
				+ "set queued = 'true' "
				+ "where ue_id = ?";
		PreparedStatement pstmtQueueDone = null;
		
		try {
			GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
			serverName = GeneralUtilityMethods.getSubmissionServer(dbc.sd);

			pstmtEnqueue = dbc.sd.prepareStatement(sqlEnqueue);
			pstmtQueueDone = dbc.sd.prepareStatement(sqlQueueDone);
			pstmtDup = dbc.sd.prepareStatement(sqlDup);
			
			uem = new JdbcUploadEventManager(dbc.sd);

			// Default to English though we could get the locales from a server level setting
			Locale locale = new Locale("en");
			ResourceBundle localisation;
			try {
				localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			} catch(Exception e) {
				localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", locale);
			}
			serverName = GeneralUtilityMethods.getSubmissionServer(dbc.sd);

			LinkageManager linkMgr = new LinkageManager(localisation);
			Date timeNow = new Date();
			
			/*
			 * Process all pending uploads
			 */
			List<UploadEvent> uel = null;

			if(subscriberType.equals("upload")) {
				uel = uem.getPending();		// Get pending jobs
			
				if(uel.isEmpty()) {
					System.out.print(".");		// Log the running of the upload processor
				} else {
					log.info("\nUploading: "  + timeNow.toString());

					for(UploadEvent ue : uel) {					
						// Enqueue event
						pstmtDup.setString(1,ue.getInstanceId());
						ResultSet rs = pstmtDup.executeQuery();
						if(!rs.next() || rs.getInt(1) == 0) {						
							pstmtEnqueue.setInt(1, ue.getId());
							pstmtEnqueue.setString(2, ue.getInstanceId());
							pstmtEnqueue.setBoolean(3, ue.getRestore());
							pstmtEnqueue.setString(4, gson.toJson(ue));
							pstmtEnqueue.executeUpdate();	
							
							// Mark it as queued
							pstmtQueueDone.setInt(1, ue.getId());
							pstmtQueueDone.executeUpdate();
						}
					} 
				}
			} 

			/*
			 * Apply any other subscriber type dependent processing
			 */
			if(subscriberType.equals("forward")) {		// Note forward is just another batch process, it no longer forwards surveys to other servers
				
				applyCaseManagementReminders(dbc.sd, dbc.results, basePath, serverName);
				applyPeriodicNotifications(dbc.sd, dbc.results, basePath, serverName);
				applyServerCalculateNotifications(dbc.sd, dbc.results, basePath, serverName);
				
				// Erase any templates that were deleted more than a set time ago
				eraseOldSurveyTemplates(dbc.sd, dbc.results, localisation, basePath);

				// Delete any case management alerts not linked to by a survey
				deleteOldCaseManagementAlerts(dbc.sd,localisation);
				
				// Delete linked csv files logically deleted more than 10 minutes age
				deleteOldLinkedCSVFiles(dbc.sd, dbc.results, localisation, basePath);
				
				applyReminderNotifications(dbc.sd, dbc.results, basePath, serverName);
				sendMailouts(dbc.sd, basePath, serverName);
				expireTemporaryUsers(localisation, dbc.sd);
				
				/*
				 * Apply foreign keys
				 */
				ForeignKeyManager fkm = new ForeignKeyManager();
				fkm.apply(dbc.sd, dbc.results);
				
				/*
				 * Initialise the linkage table if that has been requested
				 * Linkage is any arbitrary connection between survey instances 
				 * Currently it is only used with fingerprints
				 */
				if(rebuildLinkageTable(dbc.sd)) {
					log.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Rebuild Linkage ");
					
					String sqlInst = "select table_name, f_id from form where s_id = ? and parentform = 0";
					
					String sqlClear = "truncate linkage";
					
					String sql = "select q.q_id, q.column_name, q.appearance, q.parameters, s.ident, s.s_id, f.table_name, f.f_id, f.parentform, p.o_id "
							+ "from question q, form f, survey s, project p "
							+ "where q.f_id = f.f_id "
							+ "and f.s_id = s.s_id "
							+ "and p.id = s.p_id "
							+ "and not q.soft_deleted "
							+ "and not s.deleted "
							+ "and (q.appearance like '%keppel%' or q.appearance like '%fingerprintreader%')";
					
					PreparedStatement pstmt = null;
					PreparedStatement pstmtClear = null;
					PreparedStatement pstmtRebuild = null;
					PreparedStatement pstmtData = null;
					PreparedStatement pstmtInst = null;

					ResultSet rsData = null;
					
					try {
						pstmtInst = dbc.sd.prepareStatement(sqlInst);
						
						pstmtClear = dbc.sd.prepareStatement(sqlClear);
						pstmtClear.executeUpdate();
						
						pstmtRebuild = dbc.sd.prepareStatement(sql);
						ResultSet rs = pstmtRebuild.executeQuery();
							
						while(rs.next()) {
							
							String appearance = rs.getString("appearance");
							if(linkMgr.isLinkageItem(rs.getString("appearance"))) {
										
								int sId = rs.getInt("s_id");
								int fId = rs.getInt("f_id");
								int parentForm = rs.getInt("parentform");
								String sIdent = rs.getString("ident");
								String colName = rs.getString("column_name");
								String tableName = rs.getString("table_name");
								int oId = rs.getInt("o_id");
								ArrayList<KeyValueSimp> params = GeneralUtilityMethods.convertParametersToArray(rs.getString("parameters"));
								log.info("------ " + sIdent + " : " + colName + " : " + tableName);
								
								Tables tables = new Tables(sId);
								tables.add(tableName, fId, parentForm);
								
								// Add instance id table if it is not this one
								if(parentForm != 0) {
									try {
										pstmtInst.setInt(1, sId);

										log.info("Getting main form: " + pstmt.toString());
										ResultSet rsInst = pstmtInst.executeQuery();
										if (rsInst.next()) {
											String mainTable = rs.getString(1);
											int mainForm = rs.getInt(2);
											tables.add(mainTable, mainForm, 0);
										}

									} catch (Exception e) {
										log.log(Level.SEVERE, "Exception", e);
									}
								}
								
								tables.addIntermediateTables(dbc.sd);
								
								String sqlTables = tables.getTablesSQL();
								String sqlTableJoin = tables.getTableJoinSQL();
								String sqlNoBad = tables.getNoBadClause();
								
								// Get the data for each column
								StringBuilder sqlData = new StringBuilder("select instanceid, ")
										.append(colName)
										.append(" from ")
										.append(sqlTables)
										.append(" where ").append(sqlNoBad)
										.append(" and ")
										.append(colName)
										.append(" is not null ")
										.append(" and ")
										.append(colName)
										.append(" != '' ");
								if(sqlTableJoin.trim().length() > 0) {
									sqlData.append("and ").append(sqlTableJoin);
								}
								 
								if(pstmtData != null) try {pstmtData.close();} catch (Exception e) {}
								if(rsData != null) try {rsData.close();} catch (Exception e) {}
								pstmtData = dbc.results.prepareStatement(sqlData.toString());
								
								log.info("Get values: " + pstmtData.toString());
								rsData = pstmtData.executeQuery();
								while(rsData.next()) {
									ArrayList<LinkageItem> linkageItems = new ArrayList<> ();
									log.info("  Value: " + rsData.getString(1));
									linkMgr.addDataitemToList(linkageItems, rsData.getString(colName), appearance, params, sIdent, colName);
									linkMgr.writeItems(dbc.sd, oId, "rebuild", rsData.getString("instanceid"), linkageItems);
								}
								
							}
							
						}
					} finally {
						if (pstmt != null) { try {pstmt.close();} catch (SQLException e) {}}
						if(pstmtClear != null) try {pstmtClear.close();} catch(Exception e) {}
						if(pstmtRebuild != null) try {pstmtRebuild.close();} catch(Exception e) {}
						if(pstmtData != null) try {pstmtData.close();} catch (Exception e) {}
						if(pstmtInst != null) try {pstmtInst.close();} catch (Exception e) {}
					}
					
					rebuildLinkageTableComplete(dbc.sd);
				}
				
				// Set fingerprint templates for new fingerprint images
				linkMgr.setFingerprintTemplates(dbc.sd, basePath, serverName);
			}


		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {if (pstmtEnqueue != null) { pstmtEnqueue.close();}} catch (SQLException e) {}
			try {if (pstmtQueueDone != null) { pstmtQueueDone.close();}} catch (SQLException e) {}
			try {if (pstmtDup != null) { pstmtDup.close();}} catch (SQLException e) {}
			
			if(uem != null) {uem.close();}

			try {				
				if (dbc.sd != null) {
					dbc.sd.close();
					dbc.sd = null;
				}
			} catch (SQLException e) {
				log.log(Level.SEVERE, "Failed to close connection");
				e.printStackTrace();
			}

			try {
				if (dbc.results != null) {
					dbc.results.close();
					dbc.results = null;
				}
			} catch (SQLException e) {
				log.log(Level.SEVERE, "Failed to close results connection");
				e.printStackTrace();
			}
		}

	}
	
	/*
	 * Erase deleted Survey templates more than a specified number of days old
	 */
	private void eraseOldSurveyTemplates(Connection sd, Connection cResults, ResourceBundle localisation, String basePath) {

		PreparedStatement pstmt = null;
		PreparedStatement pstmtTemp = null;
		PreparedStatement pstmtFix = null;

		try {
			
			ServerManager server = new ServerManager();
			ServerData sdata = server.getServer(sd, localisation);
			int interval = sdata.keep_erased_days;
			if(interval <= 0) {
				interval = 100;		// Default to 100
			}

			ServerManager sm = new ServerManager();
			String sql = "select s_id, "
					+ "p_id, "
					+ "last_updated_time, "
					+ "ident,"
					+ "display_name "
					+ "from survey where deleted "
					+ "and hidden = 'false' "
					+ "and (last_updated_time < now() - interval '" + interval + " days') "
					+ "order by last_updated_time;";
			pstmt = sd.prepareStatement(sql);
			
			String sqlFix = "select s_id, "
					+ "p_id, "
					+ "last_updated_time, "
					+ "ident,"
					+ "display_name "
					+ "from survey where deleted "
					+ "and hidden = 'false' "
					+ "and last_updated_time is null";
			pstmtFix = sd.prepareStatement(sqlFix);

			String sqlTemp = "update survey set last_updated_time = ? where s_id = ?";
			pstmtTemp = sd.prepareStatement(sqlTemp);

			/*
			 * Temporary fix for lack of accurate date when a survey was deleted
			 */
			ResultSet rs = pstmtFix.executeQuery();
			while(rs.next()) {
				int sId = rs.getInt("s_id");
				String deletedDate = rs.getString("last_updated_time");
				String surveyDisplayName = rs.getString("display_name");

				// Get deleted date from the display name
				int idx1 = surveyDisplayName.indexOf("(20");
				String date = null;
				if(idx1 > -1) {
					int idx2 = surveyDisplayName.lastIndexOf(")");
					if(idx2 > -1 && idx2 > idx1) {
						String d = surveyDisplayName.substring(idx1 +1, idx2);
						String [] da = d.split(" ");
						if(da.length > 0) {
							date = da[0].replaceAll("_", "-");
						}
					}
				}

				if(date == null) {
					idx1 = surveyDisplayName.lastIndexOf("_20");
					if(idx1 > -1) {
						String d = surveyDisplayName.substring(idx1 +1);
						String [] da = d.split("_");
						int year = -1;
						int month = -1;
						int day = -1;
						if(da.length > 0) {
							try {
								year = Integer.parseInt(da[0]);
								month = Integer.parseInt(da[1]);
								String [] dd = da[2].split(" ");
								day = Integer.parseInt(dd[0]);
							} catch (Exception e) {

							}
						}
						if(year > -1 && month > -1 && day > -1) {
							date = year + "-" + month + "-" + day;
						}
					}
				}
				if(date == null) {
					log.info("******** Failed to get date from: " + surveyDisplayName + " deleted date was: " + deletedDate);
				} else {
					try {
						java.sql.Date dx = java.sql.Date.valueOf(date);
						pstmtTemp.setDate(1, dx);
						pstmtTemp.setInt(2,  sId);
						pstmtTemp.executeUpdate();	
					} catch (Exception e) {
						log.log(Level.SEVERE, "Error: " + surveyDisplayName + " : " + e.getMessage());
					}
				}

			}

			/*
			 * Process surveys to be deleted for real now
			 */
			rs = pstmt.executeQuery();	
			while(rs.next()) {
				int sId = rs.getInt("s_id");
				int projectId = rs.getInt("p_id");
				String deletedDate = rs.getString("last_updated_time");
				String surveyIdent = rs.getString("ident");
				String surveyDisplayName = rs.getString("display_name");

				log.info("######### Erasing: " + surveyDisplayName + " which was deleted on " +  deletedDate);
				sm.deleteSurvey(sd, cResults, "auto erase", projectId, sId, surveyIdent, surveyDisplayName, basePath, true, "yes");
			}


		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}	
			try {if (pstmtTemp != null) {pstmtTemp.close();}} catch (SQLException e) {}
			try {if (pstmtFix != null) {pstmtFix.close();}} catch (SQLException e) {}	
		}
	}
	
	/*
	 * Delete old case management alerts
	 * These are attached to a survey group so it is easier to delete them here
	 *  when they are no longer referenced than to delete them when the last survey in the
	 *  group is deleted
	 */
	private void deleteOldCaseManagementAlerts(Connection sd, ResourceBundle localisation) {

		PreparedStatement pstmt = null;

		try {
			
			String sql = "delete from cms_alert where group_survey_ident not in (select group_survey_ident from survey)";					
			pstmt = sd.prepareStatement(sql);		
			pstmt.executeUpdate();	
			
			sql = "delete from cms_setting where group_survey_ident not in (select group_survey_ident from survey)";
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}	
			pstmt = sd.prepareStatement(sql);		
			pstmt.executeUpdate();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}	
			
		}
	}
	
	/*
	 * When a new linked CSV file is generated the old one is marked for deletion
	 * Allow 10 minutes which should give any web services that are downloading it time to complete and then delete it
	 */
	private void deleteOldLinkedCSVFiles(Connection sd, Connection cResults, ResourceBundle localisation, String basePath) {

		PreparedStatement pstmt = null;
		PreparedStatement pstmtFix = null;
		PreparedStatement pstmtDel = null;

		try {

			String sql = "select id, file, to_char(deleted_time, 'YYYY-MM-DD HH24:MI:SS') as deleted_time "
					+ "from linked_files_old "
					+ "where deleted_time  < (now() - interval '600 seconds') "
					+ "and erase_time is null ";
			pstmt = sd.prepareStatement(sql);
			
			String sqlFix = "update linked_files_old set erase_time = now() where id = ?";
			pstmtFix = sd.prepareStatement(sqlFix);

			/*
			 * Delete the files that have expired
			 */
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				int id = rs.getInt("id");
				String filePath = rs.getString("file");
				String logicalDelDate = rs.getString("deleted_time");
				
				File f = new File(filePath);
				if(f.exists()) {
					log.info("Delete linked CSV file: " + f.getAbsolutePath() + " logical delete date was " + logicalDelDate);
					f.delete();
				}
				pstmtFix.setInt(1,  id);
				pstmtFix.executeUpdate();
			}
			
			/*
			 * Erase record of files older than 7 days
			 * The last 7 days can be kept for performance analysis
			 */
			String sqlDel = "delete from linked_files_old where erase_time < (now() - interval '7 days')";
			pstmtDel = sd.prepareStatement(sqlDel);
			pstmtDel.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}	
			try {if (pstmtFix != null) {pstmtFix.close();}} catch (SQLException e) {}
			try {if (pstmtDel != null) {pstmtDel.close();}} catch (SQLException e) {}
		}
	}


	
	/*
	 * Apply Reminder notifications
	 * Triggered by a time period
	 */
	private void applyReminderNotifications(Connection sd, Connection cResults, String basePath, String serverName) {

		// Sql to get notifications that need a reminder
		String sql = "select "
				+ "t.id as t_id, "
				+ "n.id as f_id, "
				+ "a.id as a_id, "
				+ "t.survey_ident, "
				+ "t.update_id,"
				+ "t.p_id,"
				+ "n.target,"
				+ "n.remote_user,"
				+ "n.notify_details,"
				+ "n.remote_password,"
				+ "n.s_id as source_s_id "
				+ "from tasks t, assignments a, forward n "
				+ "where t.tg_id = n.tg_id "
				+ "and t.id = a.task_id "
				+ "and n.enabled "
				+ "and n.trigger = 'task_reminder' "
				+ "and a.status = 'accepted' "
				+ "and t.schedule_at < now() - cast(n.period as interval) "
				+ "and a.id not in (select a_id from reminder where n_id = n.id)";
		PreparedStatement pstmt = null;
		
		// SQL to record a reminder being sent
		String sqlSent = "insert into reminder (n_id, a_id, reminder_date) values (?, ?, now())";
		PreparedStatement pstmtSent = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmtSent = sd.prepareStatement(sqlSent);
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			HashMap<Integer, ResourceBundle> locMap = new HashMap<> ();
			
			ResultSet rs = pstmt.executeQuery();
			int idx = 0;
			while (rs.next()) {
				
				if(idx++ == 0) {
					log.info("\n-------------");
				}
				int tId = rs.getInt(1);
				int nId = rs.getInt(2);
				int aId = rs.getInt(3);
				//String surveyIdent = rs.getString(4);
				String instanceId = rs.getString(5);
				int pId = rs.getInt(6);
				String target = rs.getString(7);
				String remoteUser = rs.getString(8);
				String notifyDetailsString = rs.getString(9);
				String remotePassword = rs.getString(10);
				int sourceSurveyId = rs.getInt(11);
				NotifyDetails nd = new Gson().fromJson(notifyDetailsString, NotifyDetails.class);
				
				int oId = GeneralUtilityMethods.getOrganisationIdForNotification(sd, nId);
				String sourceSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sourceSurveyId);
				
				// Send the reminder
				SubmissionMessage subMgr = new SubmissionMessage(
						"reminder title",	// todo
						tId,
						pId,
						sourceSurveyIdent,
						null,
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
						remoteUser,
						"https",
						nd.callback_url,
						remoteUser,
						remotePassword,
						0,
						null,
						nd.assign_question,
						null,					// Report Period
						0						// report id
						);
				
				ResourceBundle localisation = locMap.get(oId);
				if(localisation == null) {
					Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
					Locale orgLocale = new Locale(organisation.locale);
					try {
						localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
					} catch(Exception e) {
						localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
					}
					locMap.put(oId, localisation);
				}
				MessagingManager mm = new MessagingManager(localisation);
				mm.createMessage(sd, oId, "reminder", "", gson.toJson(subMgr));
				
				// record the sending of the notification
				pstmtSent.setInt(1, nId);
				pstmtSent.setInt(2, aId);
				pstmtSent.executeUpdate();
				
				// Write to the log
				String logMessage = "Reminder sent for: " + nId;
				if(localisation != null) {
					logMessage = localisation.getString("lm_reminder");
					logMessage = logMessage.replaceAll("%s1", GeneralUtilityMethods.getNotificationName(sd, nId));
				}
				lm.writeLogOrganisation(sd, oId, "subscriber", LogManager.REMINDER, logMessage, 0);
							
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtSent != null) {pstmtSent.close();}} catch (SQLException e) {}
			
		}
	}
	
	/*
	 * Apply Reminder notifications
	 * Set up on cases
	 */
	private void applyCaseManagementReminders(Connection sd, Connection cResults, String basePath, String serverName) {

		String tz = null;
		
		/*
		 * SQL to get the alerts
		 */
		String sql = "select distinct a.id as a_id, a.group_survey_ident, a.name, a.period, a.filter,"
				+ "f.table_name "
				+ "from cms_alert a, survey s, form f "
				+ "where f.s_id = s.s_id "
				+ "and f.parentform = 0 "
				+ "and s.group_survey_ident = a.group_survey_ident "
				+ "and not s.deleted";	
		
		PreparedStatement pstmt = null;	
		
		/*
		 * Get the notifications associated with an alert
		 */
		String sqlNotifications = "select name as notification_name,"
				+ "id,"
				+ "target,"
				+ "remote_user,"
				+ "notify_details, "
				+ "p_id "
				+ "from forward "
				+ "where trigger = 'cm_alert' "
				+ "and enabled "
				+ "and alert_id = ? ";
		PreparedStatement pstmtNotifications = null;
		
		String sqlSettings = "select settings from cms_setting "
				+ "where group_survey_ident = ? "
				+ "and settings is not null";
		PreparedStatement pstmtSettings = null;
		
		PreparedStatement pstmtMatches = null;
		PreparedStatement pstmtCaseUpdated = null;

		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		HashMap<String, CaseManagementSettings> settingsCache = new HashMap<>();
		HashMap<Integer, ResourceBundle> locMap = new HashMap<> ();
		
		// SQL to record an alert being triggered
		String sqlTriggered = "insert into case_alert_triggered (a_id, table_name, thread, final_status, alert_sent) values (?, ?,  ?, ?, now())";
		PreparedStatement pstmtTriggered = null;
		
		try {
			
			pstmtSettings = sd.prepareStatement(sqlSettings);
			pstmtTriggered = cResults.prepareStatement(sqlTriggered);
			pstmtNotifications = sd.prepareStatement(sqlNotifications);
			
			// 1. Get case management alerts 
			pstmt = sd.prepareStatement(sql);
			//log.info("Cm alerts: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				
				int aId = rs.getInt("a_id");
				String alertName = rs.getString("name");
				String groupSurveyIdent = rs.getString("group_survey_ident");
				String table = rs.getString("table_name");
				String period = rs.getString("period");	
				String filter = rs.getString("filter");
				int oId = GeneralUtilityMethods.getOrganisationIdForGroupSurveyIdent(sd, groupSurveyIdent);
				
				if(!isValidPeriod(period)) {
					log.info("Error: ++++++ : Invalid Period: " + period);
					continue;
				}
				if(GeneralUtilityMethods.tableExists(cResults, table)) {
					
					ResourceBundle localisation = locMap.get(oId);
					if(localisation == null) {
						Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
						Locale orgLocale = new Locale(organisation.locale);
						try {
							localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
						} catch(Exception e) {
							localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
						}
						locMap.put(oId, localisation);
					}
					
					/*
					 * Get the case management settings for this case (group survey)
					 */
					CaseManagementSettings settings = settingsCache.get(groupSurveyIdent);
					if(settings == null) {
						pstmtSettings.setString(1,groupSurveyIdent);
						//log.info("CMS Settings: " + pstmtSettings.toString());
						ResultSet srs = pstmtSettings.executeQuery();
						if(srs.next()) {
							settings = gson.fromJson(srs.getString("settings"), CaseManagementSettings.class);
							settingsCache.put(groupSurveyIdent, settings);
						}						
					}
					if(settings != null && settings.finalStatus != null && settings.statusQuestion != null &&
							GeneralUtilityMethods.hasColumn(cResults, table, settings.statusQuestion)) {
						
						/*
						 * Find records in the case that match this alert
						 */
						StringBuilder sqlMatch = new StringBuilder("select prikey, instanceid, _thread from "); 
						sqlMatch.append(table); 
						sqlMatch.append(" where not _bad and (")
							.append(settings.statusQuestion)
							.append(" is null or ")
							.append("cast (").append(settings.statusQuestion).append(" as text)").append(" != ? ) ")
							.append("and  _thread not in (select thread from case_alert_triggered where table_name = ? and a_id = ?) ")
							.append("and _thread_created < now() - ?::interval ");	
						
						/*
						 * Add context filter
						 */
						SqlFrag filterFrag = null;
						if(filter != null && filter.length() > 0) {			
							filterFrag = new SqlFrag();
							filterFrag.addSqlFragment(filter, false, localisation, 0);
						}
						if(filterFrag != null) {
							sqlMatch.append(" and (").append(filterFrag.sql).append(")");
						}
						
						pstmtMatches = cResults.prepareStatement(sqlMatch.toString());
						int idx = 1;
						pstmtMatches.setString(idx++, settings.finalStatus);
						pstmtMatches.setString(idx++, table);
						pstmtMatches.setInt(idx++, aId);
						pstmtMatches.setString(idx++, period);						
						
						if(filterFrag != null) {
							idx = GeneralUtilityMethods.setFragParams(pstmtMatches, filterFrag, idx, tz);
						}
						
						//log.info(pstmtMatches.toString());
						try {
							ResultSet mrs = pstmtMatches.executeQuery();
							
							while(mrs.next()) {
								
								int prikey = mrs.getInt("prikey");						
								String instanceid = mrs.getString("instanceid");
								String thread = mrs.getString("_thread");
								
								/*
								 * Record the triggering of the alert
								 */
								String details = localisation.getString(NotificationManager.TOPIC_CM_ALERT);
								details = details.replace("%s1", alertName);
								RecordEventManager rem = new RecordEventManager();
								rem.writeEvent(
										sd, 
										cResults, 
										RecordEventManager.ALERT, 
										"success",
										null, 
										table, 
										instanceid, 
										null,				// Change object
										null,				// Task Object
										null,				// Notification object
										details, 
										0,					// sId (don't care legacy)
										groupSurveyIdent,
										0,					// Don't need task id if we have an assignment id
										0					// Assignment id
										);
								
								// update case_alert_triggered to record the raising of this alert	
								pstmtTriggered.setInt(1, aId);	
								pstmtTriggered.setString(2, table);
								pstmtTriggered.setString(3, thread);
								pstmtTriggered.setString(4, settings.finalStatus);
								
								pstmtTriggered.executeUpdate();
								
								// Update the case so that the alert status can be charted
								StringBuilder sqlUpdate = new StringBuilder("update ") 
										.append(table)
										.append(" set _alert = ? where prikey = ?");	
								
								pstmtCaseUpdated = cResults.prepareStatement(sqlUpdate.toString());
								pstmtCaseUpdated.setString(1, alertName);
								pstmtCaseUpdated.setInt(2, prikey);	
								pstmtCaseUpdated.executeUpdate();
								
								/*
								 * Process notifications associated with this alert
								 */
								pstmtNotifications.setInt(1, aId);
								log.info("Notifications to be triggered: " + pstmtNotifications.toString());
								
								ResultSet notrs = pstmtNotifications.executeQuery();
								
								while(notrs.next()) {
								
									int nId = notrs.getInt("id");
									String notificationName = notrs.getString("notification_name");
									String notifyDetailsString = notrs.getString("notify_details");
									NotifyDetails nd = new Gson().fromJson(notifyDetailsString, NotifyDetails.class);
									String target = notrs.getString("target");
									String user = notrs.getString("remote_user");
									int pId  = notrs.getInt("p_id");
									
									SubmissionMessage subMgr = new SubmissionMessage(
											"Case Management",		// TODO title
											0,
											pId,
											groupSurveyIdent,
											null,
											instanceid, 
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
											user,
											"https",
											nd.callback_url,
											user,
											null,
											0,
											nd.survey_case,
											nd.assign_question,
											null,					// Report Period
											0						// report id
											);
									
									MessagingManager mm = new MessagingManager(localisation);
									mm.createMessage(sd, oId, NotificationManager.TOPIC_CM_ALERT, "", gson.toJson(subMgr));						
								
									// Write to the log
									String logMessage = "Notification triggered by alert id " + aId + " for notification: " + nId;
									if(localisation != null) {
										logMessage = localisation.getString(NotificationManager.TOPIC_CM_ALERT);
										logMessage = logMessage.replaceAll("%s1", alertName);
										logMessage = logMessage.replaceAll("%s2", notificationName);
									}
									lm.writeLogOrganisation(sd, oId, "subscriber", LogManager.REMINDER, logMessage, 0);
								}
	
							}
						} catch (Exception e) {
							int sId = GeneralUtilityMethods.getSurveyId(sd, groupSurveyIdent);
							String msg = e.getMessage();
							if(msg == null) {
								msg = "";
							}
							if(!duplicateLogEntry(sId + "alert" + LogManager.CASE_MANAGEMENT + msg)) {
								lm.writeLog(sd, sId, "alert", LogManager.CASE_MANAGEMENT, msg, 0, serverName);
							}
							log.log(Level.SEVERE, e.getMessage(), e);
						}
					 
					} else {
						//log.info("cm: no status settings");
					}
				}
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtTriggered != null) {pstmtTriggered.close();}} catch (SQLException e) {}
			try {if (pstmtSettings != null) {pstmtSettings.close();}} catch (SQLException e) {}
			try {if (pstmtMatches != null) {pstmtMatches.close();}} catch (SQLException e) {}
			try {if (pstmtNotifications != null) {pstmtNotifications.close();}} catch (SQLException e) {}
			try {if (pstmtCaseUpdated != null) {pstmtCaseUpdated.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Apply notifications resulting from a server calculate change
	 */
	private void applyServerCalculateNotifications(Connection sd, Connection cResults, String basePath, String serverName) {

		String tz = null;
		
		PreparedStatement pstmtMatches = null;
		
		/*
		 * Get the notifications associated with a server calculate change
		 */
		String sqlNotifications = "select n.name as notification_name,"
				+ "n.update_question as calculate_question,"
				+ "n.update_value as calculate_value,"
				+ "n.id,"
				+ "n.s_id,"
				+ "n.target,"
				+ "n.remote_user,"
				+ "notify_details, "
				+ "n.p_id,"
				+ "n.filter,"
				+ "f.table_name,"
				+ "q.server_calculate,"
				+ "n.updated "
				+ "from forward n, form f, question q "
				+ "where n.trigger = 'server_calc' "
				+ "and n.enabled "
				+ "and n.s_id = f.s_id "
				+ "and f.f_id = q.f_id "
				+ "and q.qname = n.update_question "
				+ "and q.qtype = 'server_calculate' ";
		PreparedStatement pstmtNotifications = null;

		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		HashMap<Integer, ResourceBundle> locMap = new HashMap<> ();

		// SQL to record an alert being triggered
		String sqlTriggered = "insert into server_calc_triggered "
				+ "(n_id, table_name, question_name, value, thread, updated_value, notification_sent) "
				+ "values (?, ?, ?, ?, ?, ?, now())";
		PreparedStatement pstmtTriggered = null;
	
		String sqlUpdateNot = "update forward set updated = false where id = ?";
		PreparedStatement pstmtUpdateNot = null;
		
		int sId = 0;
		String notificationName = null;
		
		try {
			pstmtTriggered = cResults.prepareStatement(sqlTriggered);
			pstmtUpdateNot = sd.prepareStatement(sqlUpdateNot);
			pstmtNotifications = sd.prepareStatement(sqlNotifications);
			//log.info("Server Calculate Notifications to be triggered: " + pstmtNotifications.toString());

			ResultSet notrs = pstmtNotifications.executeQuery();

			while(notrs.next()) {
				
				int nId = notrs.getInt("id");
				sId = notrs.getInt("s_id");
				notificationName = notrs.getString("notification_name");
				String notifyDetailsString = notrs.getString("notify_details");
				NotifyDetails nd = new Gson().fromJson(notifyDetailsString, NotifyDetails.class);
				String target = notrs.getString("target");
				String user = notrs.getString("remote_user");
				int pId  = notrs.getInt("p_id");
				String table = notrs.getString("table_name");
				String calculateQuestion = notrs.getString("calculate_question");
				String calculateValue = notrs.getString("calculate_value");
				String filter = notrs.getString("filter");
				String serverCalculate = notrs.getString("server_calculate");	
				boolean updated = notrs.getBoolean("updated");
				
				int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);
				
				log.info("xxxxxxxxxxxxx server calculate notification for " + notificationName + " on table " + table);
				
				ResourceBundle localisation = locMap.get(oId);
				if(localisation == null) {
					Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
					Locale orgLocale = new Locale(organisation.locale);
					try {
						localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
					} catch(Exception e) {
						localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
					}
					locMap.put(oId, localisation);
				}
				
				String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				
				SqlFrag calculationFrag = null;
				if(serverCalculate != null && GeneralUtilityMethods.tableExists(cResults, table)) {
					ServerCalculation sc = gson.fromJson(serverCalculate, ServerCalculation.class);
					calculationFrag = new SqlFrag();
					sc.populateSql(calculationFrag, localisation);
						
					/*
					 * Find records that match this server calculation
					 */
					StringBuilder sqlMatch = new StringBuilder("select prikey, instanceid, _thread from "); 
					sqlMatch.append(table); 
					sqlMatch.append(" where not _bad and cast(")
						.append(calculationFrag.sql)
						.append(" as text) = ? ")
						.append("and  _thread not in "
								+ "(select thread from server_calc_triggered "
								+ "where table_name = ? "
								+ "and question_name = ? "
								+ "and value = ?) ");	
					
					/*
					 * Add context filter
					 */
					SqlFrag filterFrag = null;
					if(filter != null && filter.length() > 0) {			
						filterFrag = new SqlFrag();
						filterFrag.addSqlFragment(filter, false, localisation, 0);
					}
					if(filterFrag != null) {
						sqlMatch.append(" and (").append(filterFrag.sql).append(")");
					}
					
					pstmtMatches = cResults.prepareStatement(sqlMatch.toString());
					int idx = 1;
					if(calculationFrag != null) {
						idx = GeneralUtilityMethods.setFragParams(pstmtMatches, calculationFrag, idx, tz);
					}
					pstmtMatches.setString(idx++, calculateValue);
					pstmtMatches.setString(idx++, table);
					pstmtMatches.setString(idx++, calculateQuestion);	
					pstmtMatches.setString(idx++, calculateValue);	
					
					if(filterFrag != null) {
						idx = GeneralUtilityMethods.setFragParams(pstmtMatches, filterFrag, idx, tz);
					}
					
					log.info(pstmtMatches.toString());
					ResultSet rs = pstmtMatches.executeQuery();
					while (rs.next()) {
						String instanceid = rs.getString("instanceid");		// TODO - get these in a loop checking the server calculations in a survey
						String thread = rs.getString("_thread");
						log.info("Server Calculation Triggered for Instance: " + instanceid + " in table " + table);
						
						/*
						 * Only send the notification if this is a new change that happened after the notification was
						 * created or updated
						 */
						if(!updated) {
							SubmissionMessage subMgr = new SubmissionMessage(
									"Server Calculation",	
									0,
									pId,
									surveyIdent,
									null,
									instanceid, 
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
									user,
									"https",
									nd.callback_url,
									user,
									null,
									0,
									nd.survey_case,
									nd.assign_question,
									null,					// Report Period
									0						// report id
									);
			
							MessagingManager mm = new MessagingManager(localisation);
							mm.createMessage(sd, oId, NotificationManager.TOPIC_SERVER_CALC, "", gson.toJson(subMgr));	
						} else {
							log.info("Message not send as the notification has been newly created ");
						}
		
						// update server_calc_triggered to record the raising of this alert	
						pstmtTriggered.setInt(1, nId);	
						pstmtTriggered.setString(2, table);
						pstmtTriggered.setString(3, calculateQuestion);
						pstmtTriggered.setString(4, calculateValue);
						pstmtTriggered.setString(5, thread);
						pstmtTriggered.setBoolean(6, updated);
						
						pstmtTriggered.executeUpdate();
						
						// Write to the log
						String logMessage = "Notification triggered by server calculation for notification: " + notificationName;
						if(localisation != null) {
							logMessage = localisation.getString(NotificationManager.TOPIC_SERVER_CALC);
							logMessage = logMessage.replaceAll("%s1", notificationName);
						}
						lm.writeLogOrganisation(sd, oId, "subscriber", LogManager.REMINDER, logMessage, 0);
					}
				} else {
					log.info("Error: Server calculation is null or data table has not been created: " + notificationName);
				}
				
				/*
				 * Mark the notification as not updated so that events can be sent
				 */
				if(updated) {
					pstmtUpdateNot.setInt(1, nId);
					pstmtUpdateNot.executeUpdate();
				}
			}

		} catch (Exception e) {
			String msg = e.getMessage();
			if(msg == null) {
				msg = "";
			}
			msg += " - " + notificationName;
			if(!duplicateLogEntry(sId + "notification" + LogManager.NOTIFICATION_ERROR + msg)) {
				lm.writeLog(sd, sId, "notification", LogManager.NOTIFICATION_ERROR, msg, 0, serverName);
			}
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			try {if (pstmtNotifications != null) {pstmtNotifications.close();}} catch (SQLException e) {}
			try {if (pstmtMatches != null) {pstmtMatches.close();}} catch (SQLException e) {}
			try {if (pstmtTriggered != null) {pstmtTriggered.close();}} catch (SQLException e) {}
			try {if (pstmtUpdateNot != null) {pstmtUpdateNot.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Return true if this error has already been reported today
	 */
	private boolean duplicateLogEntry(String entry) {
		entry = entry + Calendar.getInstance().get(Calendar.DAY_OF_YEAR);
		boolean dup = true;
		if(autoErrorCheck.get(entry) == null) {
			autoErrorCheck.put(entry, entry);
			dup = false;
		} 
		return dup;
	}
	
	/*
	 * Apply Periodic notifications
	 */
	private void applyPeriodicNotifications(Connection sd, Connection cResults, String basePath, String serverName) {
		
		HashMap<Integer, ResourceBundle> locMap = new HashMap<> ();
		
		/*
		 * Get current time
		 */
		String sqlCurrentTime = "select current_time";
		PreparedStatement pstmtCurrentTime = null;
		
		/*
		 * Get the notifications that send a response at fixed periods and where the time is due
		 */
		String sql = "select name,"
				+ "target,"
				+ "notify_details, "
				+ "periodic_time, "
				+ "periodic_period, "
				+ "r_id, "
				+ "p_id "
				+ "from forward "
				+ "where trigger = 'periodic' "
				+ "and enabled "
				+ "and periodic_time > (select last_checked_time from periodic) and periodic_time < ? "
				+ "and ("
				+ "(periodic_period = 'daily') "
				+ "or (periodic_period = 'weekly' and periodic_day_of_week = extract('DOW' from current_date)) "
				+ "or (periodic_period = 'monthly' and periodic_day_of_month = extract('Day' from current_date)) "
				+ "or (periodic_period = 'yearly' and periodic_day_of_month = extract('Day' from current_date) and periodic_month = extract('Month' from current_date)) "
				+ ")";
		PreparedStatement pstmt = null;

		/*
		 * Update last checked time
		 */
		String sqlUpdate = "update periodic set last_checked_time = ?";
		PreparedStatement pstmtUpdate = null;		
		String sqlInsert = "insert into periodic(last_checked_time) values(?)";
		PreparedStatement pstmtInsert = null;
		
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {

			/*
			 * Get the current time
			 */
			Time currentTime = null;
			pstmtCurrentTime = sd.prepareStatement(sqlCurrentTime);
			ResultSet rs = pstmtCurrentTime.executeQuery();
			if(rs.next()) {
				currentTime = rs.getTime("current_time");
			}
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setTime(1, currentTime);
			rs = pstmt.executeQuery();
			
			/*
			 * Get periodic notifications that have a time between the last check and now
			 */
			while(rs.next()) {
				
				String name = rs.getString("name");
				String target = rs.getString("target");
				String notifyDetailsString = rs.getString("notify_details");
				String period = rs.getString("periodic_period");
				int rId = rs.getInt("r_id");
				int pId = rs.getInt("p_id");

				NotifyDetails nd = gson.fromJson(notifyDetailsString, NotifyDetails.class);
					
				int oId = GeneralUtilityMethods.getOrganisationIdForReport(sd, rId);
				log.info("----- Organisation for report is: " + oId);
				if(oId <= 0) {	// If the report is not valid and hence the organisation is not valid then continue
					continue;
				}
						
				ResourceBundle localisation = locMap.get(oId);
				if(localisation == null) {
					Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
					Locale orgLocale = new Locale(organisation.locale);
					try {
						localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
					} catch(Exception e) {
						localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
					}
				}
				
				MessagingManager mm = new MessagingManager(localisation);
				
				SubmissionMessage msg = new SubmissionMessage(
						name,			// title
						0,				// task id
						pId,
						null,			// Survey ident
						null,			// Update ident
						null,			// instance id
						nd.from,
						nd.subject, 
						nd.content,
						nd.attach,
						false,			// include references (not used)
						false,			// launched only (not used)
						0,				// email question (deprecated)
						null,			// email question name
						null,			// email meta
						false,			// email assigned (not used)
						nd.emails,		// Email addresses
						target,
						null,
						"https",		// scheme
						null,			// callback URL
						null,			// remote user
						null,			// remote password
						0,				// PDF template ID
						null,			// Survey case
						null,			// Assign Question
						period,			// Report Period
						rId);
				
				mm.createMessage(sd, oId, NotificationManager.TOPIC_PERIODIC, "", gson.toJson(msg));	
			}
			
			/*
			 * Store the current time as the last checked time
			 */
			pstmtUpdate = sd.prepareStatement(sqlUpdate);
			pstmtUpdate.setTime(1, currentTime);
			int count = pstmtUpdate.executeUpdate();
			if(count < 1) {
				pstmtInsert = sd.prepareStatement(sqlInsert);
				pstmtInsert.setTime(1, currentTime);
				pstmtInsert.executeUpdate();
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtCurrentTime != null) {pstmtCurrentTime.close();}} catch (SQLException e) {}
			try {if (pstmtUpdate != null) {pstmtUpdate.close();}} catch (SQLException e) {}
			try {if (pstmtInsert != null) {pstmtInsert.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Send Mailouts
	 */
	private void sendMailouts(Connection sd, String basePath, 
			String serverName) {

		// Sql to get mailouts
		String sql = "select "
				+ "mp.id, "
				+ "p.o_id, "
				+ "m.survey_ident, "
				+ "m.multiple_submit,"
				+ "p.id as p_id, "
				+ "ppl.email, "
				+ "ppl.name, "
				+ "m.content, "
				+ "m.subject,"
				+ "mp.initial_data, "
				+ "mp.link, "
				+ "m.name as campaign_name, "
				+ "m.anonymous "
				+ "from mailout_people mp, mailout m, people ppl, survey s, project p "
				+ "where mp.m_id = m.id "
				+ "and mp.p_id = ppl.id "
				+ "and m.survey_ident = s.ident "
				+ "and s.p_id = p.id "
				+ "and mp.status = '" + MailoutManager.STATUS_PENDING + "' "
				+ "and mp.processed is null ";
		PreparedStatement pstmt = null;
		
		// SQL to record a mailout being sent
		String sqlSent = "update mailout_people set processed = now(), link = ? where id = ?";
		PreparedStatement pstmtSent = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmtSent = sd.prepareStatement(sqlSent);
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			HashMap<String, ResourceBundle> locMap = new HashMap<> ();

			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				
				log.info("----- Sending mailout");
				int id = rs.getInt("id");
				int oId = rs.getInt("o_id");
				String surveyIdent = rs.getString("survey_ident");
				int pId = rs.getInt("p_id");
				String email = rs.getString("email");	
				String name = rs.getString("name");
				String content = rs.getString("content");
				String subject = rs.getString("subject");
				String initialData = rs.getString("initial_data");
				String link = rs.getString("link");
				boolean single = !rs.getBoolean("multiple_submit");
				String campaignName = rs.getString("campaign_name");
				boolean anonymousCampaign = rs.getBoolean("anonymous");
				
				ResourceBundle localisation = locMap.get(surveyIdent);
				
				if(link == null) { 
					// Create an action to complete the mailed out form if a link does not already exist
					ActionManager am = new ActionManager(localisation, "UTC");
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
					
					link = am.getLink(sd, action, oId, action.single);
				}
				
				// Add user name to content
				log.info("Add username to content: " + name);
				String messageLink = link;
				if(content == null) {
					content = "Mailout";
				} else {
					if(name != null) {
						content = content.replaceAll("\\$\\{name\\}", name);
					}
					String url = "https://" + serverName + "/webForm" + link;
					if(content.contains("${url}")) {					
						content = content.replaceAll("\\$\\{url\\}", url);
						messageLink = null;	// Default link replaced
					} 
				}
				
				// Send the Mailout Message
				log.info("Create send message");
				MailoutMessage msg = new MailoutMessage(
						id,
						surveyIdent,
						pId,
						"from",
						subject, 
						content,
						email,
						"email",
						"user",
						"https",
						serverName,
						basePath,
						messageLink);
				
				if(localisation == null) {
					Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
					Locale orgLocale = new Locale(organisation.locale);
					try {
						localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
					} catch(Exception e) {
						localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
					}
					locMap.put(surveyIdent, localisation);
				}
				MessagingManager mm = new MessagingManager(localisation);
				mm.createMessage(sd, oId, NotificationManager.TOPIC_MAILOUT, "", gson.toJson(msg));
				
				// record the sending of the notification
				pstmtSent.setString(1, "https://" + serverName + "/webForm" + link);
				pstmtSent.setInt(2, id);
				log.info("Record sending of message: " + pstmtSent.toString());
				pstmtSent.executeUpdate();
				
			}
			sd.setAutoCommit(true);

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtSent != null) {pstmtSent.close();}} catch (SQLException e) {}
			
		}
	}
	
	private void expireTemporaryUsers(ResourceBundle localisation, Connection sd) throws SQLException {
		
		int interval = 30;	// Expire after 30 days
		String sql = "select ident, action_details, o_id from users "
				+ "where temporary "
				+ "and single_submission "
				+ "and (created < now() - interval '" + interval + " days') "
				+ "limit 100";  // Apply progressively incase a large number expire simultaneously
		
		PreparedStatement pstmt = null;
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		MailoutManager mm = new MailoutManager(localisation);
		UserManager um = new UserManager(localisation);
		TaskManager tm = new TaskManager(localisation, null);
		
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String userIdent = rs.getString("ident");
				Action action = GeneralUtilityMethods.getAction(sd, gson, rs.getString("action_details"));
				int oId = rs.getInt("o_id");
				
				// Record the expiry of this action
				if(action.assignmentId > 0) {
					// Assignment
					tm.setTaskStatusCancelled(sd, action.assignmentId);
				} else if(action.mailoutPersonId > 0) {
					// Mailout
					mm.setMailoutStatus(sd, action.mailoutPersonId, 
							MailoutManager.STATUS_EXPIRED, null);
				}
				
				um.deleteSingleSubmissionTemporaryUser(sd, userIdent, UserManager.STATUS_EXPIRED);
				String modIdent = action.email != null ? action.email : userIdent;			
				lm.writeLogOrganisation(sd, oId, modIdent, LogManager.EXPIRED, localisation.getString("msg_expired")
						+ ": " + userIdent, 0);
				
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}
	
	private boolean rebuildLinkageTable(Connection sd) throws SQLException {
		boolean reload = false;
		
		String sql = "select rebuild_link_cache from server";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				reload = rs.getBoolean(1);
			}
		} finally {
			if(pstmt != null) try{pstmt.close();} catch(Exception e){}
		}
		return reload;
	}

	private boolean rebuildLinkageTableComplete(Connection sd) throws SQLException {
		boolean reload = false;
		
		String sql = "update server set rebuild_link_cache = false";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try{pstmt.close();} catch(Exception e){}
		}
		return reload;
	}

	private boolean isValidPeriod(String period) {
		boolean valid = false;
		
		if(period != null) {
			String[] comp = period.split(" ");
			if(comp.length > 1) {
				try {
					int iValue = Integer.valueOf(comp[0]);
					if(iValue > 0) {
						valid = true;
					}
				} catch(Exception e) {
					
				}
			}
		}
		return valid;
	}
}
