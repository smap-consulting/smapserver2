import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.smap.model.SurveyInstance;
import org.smap.model.SurveyTemplate;
import org.smap.notifications.interfaces.S3AttachmentUpload;
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
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.CaseManagementSettings;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.LinkageItem;
import org.smap.sdal.model.MailoutMessage;
import org.smap.sdal.model.MediaChange;
import org.smap.sdal.model.NotifyDetails;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumn;
import org.smap.server.entities.MissingSurveyException;
import org.smap.server.entities.MissingTemplateException;
import org.smap.server.entities.SubscriberEvent;
import org.smap.server.entities.UploadEvent;
import org.smap.subscribers.SubRelationalDB;
import org.smap.subscribers.Subscriber;
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

	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	DatabaseConnections dbc = new DatabaseConnections();

	private static Logger log =
			Logger.getLogger(Subscriber.class.getName());

	private static LogManager lm = new LogManager();		// Application log

	/**
	 * @param args
	 */
	public void go(String smapId, String basePath, String subscriberType) {

		confFilePath = "./" + smapId;

		// Get the connection details for the meta data database

		JdbcUploadEventManager uem = null;
		
		Survey sdalSurvey = null;
		
		String sqlResultsDB = "update upload_event "
				+ "set results_db_applied = 'true',"
				+ "processed_time = now(),"
				+ "db_status = ?,"
				+ "db_reason = ? "
				+ "where ue_id = ?";
		PreparedStatement pstmtResultsDB = null;
		String serverName = null;

		try {
			GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
			serverName = GeneralUtilityMethods.getSubmissionServer(dbc.sd);

			uem = new JdbcUploadEventManager(dbc.sd);
			pstmtResultsDB = dbc.sd.prepareStatement(sqlResultsDB);

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
			
			Subscriber subscriber = new SubRelationalDB();
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
						log.info("        Survey:" + ue.getSurveyName() + ":" + ue.getId());

						SurveyInstance instance = null;
						SubscriberEvent se = new SubscriberEvent();
						String uploadFile = ue.getFilePath();

						log.info("Upload file: " + uploadFile);
						InputStream is = null;
						InputStream is3 = null;
								
						int oId = 0;
								
						ArrayList<MediaChange> mediaChanges = null;

						try {
							oId = GeneralUtilityMethods.getOrganisationIdForSurvey(dbc.sd, ue.getSurveyId());
							Organisation organisation = GeneralUtilityMethods.getOrganisation(dbc.sd, oId);
							Locale orgLocale = new Locale(organisation.locale);
							ResourceBundle orgLocalisation;
							try {
								orgLocalisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
							} catch(Exception e) {
								orgLocalisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
							}

							// Get the submitted results as an XML document
							try {
								is = new FileInputStream(uploadFile);
							} catch (FileNotFoundException e) {
								// Possibly we are re-trying an upload and the XML file has been archived to S3
								// Retrieve the file and try again
								File f = new File(uploadFile);
								FileUtils.forceMkdir(f.getParentFile());
								S3AttachmentUpload.get(basePath, uploadFile);
								is = new FileInputStream(uploadFile);
							}

							// Convert the file into a survey instance object
							instance = new SurveyInstance(is);
							log.info("UUID:" + instance.getUuid());

							//instance.getTopElement().printIEModel("   ");	// Debug 

							// Get the template for this survey
							String templateName = instance.getTemplateName();
							SurveyTemplate template = new SurveyTemplate(orgLocalisation);

							SurveyManager sm = new SurveyManager(localisation, "UTC");
							sdalSurvey = sm.getSurveyId(dbc.sd, templateName);	// Get the survey from the templateName / ident
										
							template.readDatabase(dbc.sd, dbc.results, templateName, false);					
							template.extendInstance(dbc.sd, instance, true, sdalSurvey);	// Extend the instance with information from the template
							// instance.getTopElement().printIEModel("   ");	// Debug

							// Get attachments from incomplete submissions
							getAttachmentsFromIncompleteSurveys(dbc.sd, ue.getFilePath(), ue.getOrigSurveyIdent(), ue.getIdent(), 
									ue.getInstanceId());

							is3 = new FileInputStream(uploadFile);	// Get an input stream for the file in case the subscriber uses that rather than an Instance object
							mediaChanges = subscriber.upload(instance, 
												is3, 
												ue.getUserName(), 
												ue.getTemporaryUser(),
												ue.getServerName(), 
												ue.getImei(), 
												se,
												confFilePath, 
												ue.getFormStatus(),
												basePath, 
												uploadFile, 
												ue.getUpdateId(),
												ue.getId(),
												ue.getUploadTime(),
												ue.getSurveyNotes(),
												ue.getLocationTrigger(),
												ue.getAuditFilePath(),
												orgLocalisation, 
												sdalSurvey);	// Call the subscriber	

						} catch (FileNotFoundException e) {

							se.setStatus("error");
							se.setReason("Submission File Not Found:" + uploadFile);

						} catch (MissingSurveyException e) {

							se.setStatus("error");
							se.setReason("Results file did not specify a survey template:" + uploadFile);

						} catch (MissingTemplateException e) {

							se.setStatus("error");
							se.setReason("No template named: " + e.getMessage() + " in database");

						} catch (Exception e) {

							e.printStackTrace();
							se.setStatus("error");
							se.setReason(e.getMessage());

						} finally {

							try {
								if(is != null) {is.close();}						
								if(is3 != null) {is3.close();}
								
								// Save the status
								if(se.getStatus() != null) {
					
									pstmtResultsDB.setString(1, se.getStatus());
									pstmtResultsDB.setString(2, se.getReason());
									pstmtResultsDB.setInt(3, ue.getId());
									pstmtResultsDB.executeUpdate();
											
								}	
								
							} catch (Exception e) {

							}
						}

						/*
						 * Perform post processing of the XML file
						 * Send it to S3 if that is enabled
						 * Update any media names with the final media names
						 */
						if(mediaChanges != null && mediaChanges.size() > 0) {
							processMediaChanges(uploadFile, mediaChanges);
						}
						try {
							GeneralUtilityMethods.sendToS3(dbc.sd, basePath, uploadFile, oId, false);
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
						}
								
						/*
						 * Write log entry
						 */
						String status = se.getStatus();
						String reason = se.getReason();
						String topic;
						if(status.equals("error")) {
							if(reason != null && reason .startsWith("Duplicate")) {
								topic = LogManager.DUPLICATE;
							} else {
								topic = LogManager.SUBMISSION_ERROR;
							}
						} else if(ue.getAssignmentId() > 0) {
							topic =  LogManager.SUBMISSION_TASK;
						} else if(ue.getTemporaryUser() || GeneralUtilityMethods.isTemporaryUser(dbc.sd, ue.getUserName())) {	// Note the temporaryUser flag in ue is only set for submissions with an action
							topic = LogManager.SUBMISSION_ANON;
						} else {
							topic = LogManager.SUBMISSION;
						}
						
						lm.writeLog(dbc.sd, ue.getSurveyId(), ue.getUserName(), topic, se.getStatus() + " : " 
									+ (se.getReason() == null ? "" : se.getReason()) + " : " + ue.getImei(), 0, null);
		
					} 
				}
			} 

			/*
			 * Apply any other subscriber type dependent processing
			 */
			if(subscriberType.equals("forward")) {		// Note forward is just another batch process, it no longer forwards surveys to other servers
				
				applyCaseManagementReminders(dbc.sd, dbc.results, basePath, serverName);
				applyPeriodicNotifications(dbc.sd, dbc.results, basePath, serverName);
				
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
							+ "and q.appearance like '%keppel%'";
					
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
							if(appearance.contains(linkMgr.REQUEST_FP_IMAGE) || appearance.contains(linkMgr.REQUEST_FP_ISO_TEMPLATE)) {
							
								
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
			try {if (pstmtResultsDB != null) { pstmtResultsDB.close();}} catch (SQLException e) {}

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
	 * ODK sends large attachments in separate "incomplete" posts
	 * Get these now
	 * Only do it once for all subscribers, so once the attachments from the incomplete posts have been moved
	 *  to the complete submission then the first subscriber to do thi)s will be marked as having processed the
	 *  attachments.  All other subscribers will then ignore incomplete attachments
	 */
	private void getAttachmentsFromIncompleteSurveys(Connection connectionSD, 
			String finalPath, 
			String origIdent, 
			String ident,
			String instanceId) {


		String sql = "select ue.ue_id, ue.file_path from upload_event ue "
				+ "where ue.status = 'success' "
				+ "and ue.orig_survey_ident = ? "
				+ "and ue.ident = ? "
				+ "and ue.incomplete = 'true' "
				+ "and ue.instanceid = ? "
				+ "and not ue.results_db_applied ";

		String sqlUpdate = "update upload_event "
				+ "set db_status = ?,"
				+ "db_reason = ? "
				+ "where ue_id = ?";
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtUpdate = null;
		try {
			pstmt = dbc.sd.prepareStatement(sql);
			pstmt.setString(1, origIdent);
			pstmt.setString(2, ident);
			pstmt.setString(3, instanceId);

			pstmtUpdate = dbc.sd.prepareStatement(sqlUpdate);

			File finalFile = new File(finalPath);
			File finalDirFile = finalFile.getParentFile();
			String finalDir = finalDirFile.getPath();

			log.info("Get incomplete attachments: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				log.info("++++++ Processing incomplete file name is: " + rs.getString(2));

				int ue_id = rs.getInt(1);
				File sourceFile = new File(rs.getString(2));
				File sourceDirFile = sourceFile.getParentFile();

				File files[] = sourceDirFile.listFiles();
				for(int i = 0; i < files.length; i++) {
					log.info("       File: " + files[i].getName());
					String fileName = files[i].getName();
					if(!fileName.endsWith("xml")) {
						log.info("++++++ Moving " + fileName + " to " + finalDir);
						files[i].renameTo(new File(finalDir + "/" + fileName));
					}
				}
				pstmtUpdate.setString(1,"merged");
				pstmtUpdate.setString(2,"Files moved to " + finalDir);
				pstmtUpdate.setInt(3, ue_id);
				pstmtUpdate.executeUpdate();

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
			if(pstmtUpdate != null) try {pstmtUpdate.close();} catch(Exception e) {};
		}

	}

	private boolean putDocument(String index, String type, String key, String doc, String host_name, String user, String password) {
		boolean success = true;

		CloseableHttpClient httpclient = null;
		HttpResponse response = null;
		int responseCode = 0;
		String responseReason = null;
		int port = 9200;

		try {
			HttpHost target = new HttpHost(host_name, port, "http");
			CredentialsProvider credsProvider = new BasicCredentialsProvider();
			credsProvider.setCredentials(
					new AuthScope(target.getHostName(), target.getPort()),
					new UsernamePasswordCredentials(user, password));
			httpclient = HttpClients.custom()
					.setDefaultCredentialsProvider(credsProvider)
					.build();

			String url = "http://" + host_name + ":" + port + "/" + index + "/" + type + "/" + key;
			HttpClientContext localContext = HttpClientContext.create();
			HttpPut req = new HttpPut(URI.create(url));
			
			StringEntity params =new StringEntity(doc,"UTF-8");
	        params.setContentType("application/json");
	        req.addHeader("content-type", "application/json");
	        req.addHeader("Accept-Encoding", "gzip,deflate,sdch");
	        req.setEntity(params);
			
	        log.info("Submitting document: " + url);
			response = httpclient.execute(target, req, localContext);
			responseCode = response.getStatusLine().getStatusCode();
			responseReason = response.getStatusLine().getReasonPhrase(); 
			
			// verify that the response was a 200, 201 or 202.
			// If it wasn't, the submission has failed.
			log.info("	Info: Response code: " + responseCode + " : " + responseReason);
			if (responseCode != HttpStatus.SC_OK && responseCode != HttpStatus.SC_CREATED && responseCode != HttpStatus.SC_ACCEPTED) {      
				log.info("	Error: upload to document server failed: " + responseReason);
				success = false;		
			} 

		} catch (UnsupportedEncodingException e) {
			success = false;
			String msg = "UnsupportedCodingException:" + e.getMessage();
			log.info("        " + msg);
		} catch(ClientProtocolException e) {
			success = false;
			String msg = "ClientProtocolException:" + e.getMessage();
			log.info("        " + msg);
		} catch(IOException e) {
			success = false;
			String msg = "IOException:" + e.getMessage();
			log.info("        " + msg);
		} catch(IllegalArgumentException e) {
			success = false;		
			String msg = "IllegalArgumentException:" + e.getMessage();
			log.info("        " + msg);
		} finally {
			try {
				httpclient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return success;
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
				+ "notify_details "
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
			log.info("Cm alerts: " + pstmt.toString());
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
						log.info("CMS Settings: " + pstmtSettings.toString());
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
						
						log.info(pstmtMatches.toString());
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
									
									SubmissionMessage subMgr = new SubmissionMessage(
											"Case Management",		// TODO title
											0,
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
							lm.writeLog(sd, sId, "alert", LogManager.CASE_MANAGEMENT, e.getMessage(), 0, serverName);
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
				+ "r_id "
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

				NotifyDetails nd = gson.fromJson(notifyDetailsString, NotifyDetails.class);
					
				int oId = GeneralUtilityMethods.getOrganisationIdForReport(sd, rId);
						
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
	
	/*
	 * Add the updated paths to media to the uploaded XML file
	 */
	private void processMediaChanges(String uploadFile, ArrayList<MediaChange> mediaChanges) {
		File xmlFile = new File(uploadFile);
		if(xmlFile.exists()) {
			try {
				String contents = FileUtils.readFileToString(xmlFile, StandardCharsets.UTF_8);
				for(MediaChange mc : mediaChanges) {
					contents = contents.replace(">" + mc.srcName + "<", ">" + mc.dstName + "<");
				}
				FileUtils.writeStringToFile(xmlFile, contents, StandardCharsets.UTF_8);
				// Repeat the loop because we want to ensure the xml file is saved before deleting anything
				for(MediaChange mc : mediaChanges) {
					File srcFile = new File(mc.srcPath);
					if(srcFile.exists()) {
						log.info("Deleting input file: " + srcFile.getAbsolutePath());
						srcFile.delete();
					}
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
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
