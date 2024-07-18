import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.smap.model.SurveyInstance;
import org.smap.model.SurveyTemplate;
import org.smap.notifications.interfaces.S3AttachmentUpload;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SMSInboundManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.model.MediaChange;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SMSDetails;
import org.smap.sdal.model.SubscriberEvent;
import org.smap.sdal.model.Survey;
import org.smap.server.entities.MissingSurveyException;
import org.smap.server.entities.MissingTemplateException;
import org.smap.server.entities.UploadEvent;
import org.smap.subscribers.SubRelationalDB;
import org.smap.subscribers.Subscriber;

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

public class SubmissionProcessor {

	String confFilePath;

	DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
	DocumentBuilder db = null;
 
	private static LogManager lm = new LogManager();		// Application log

	private static Logger log = Logger.getLogger(SubmissionProcessor.class.getName());
	
	private class SubmissionQueueLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		String basePath;
		String queueName;
		boolean incRestore;

		public SubmissionQueueLoop(String basePath, String queueName, boolean incRestore) {
			this.basePath = basePath;
			this.queueName = queueName;
			this.incRestore = incRestore;
		}

		public void run() {

			int delaySecs = 1;

			/*
			 * SQL to write the results to the upload event table
			 */
			String sqlResultsDB = "update upload_event "
					+ "set results_db_applied = 'true',"
					+ "processed_time = now(),"
					+ "queued = false, "
					+ "db_status = ?,"
					+ "db_reason = ?,"
					+ "queue_name = ? "
					+ "where ue_id = ?";
			PreparedStatement pstmtResultsDB = null;

			/*
			 * Dequeue SQL from https://chbussler.medium.com/implementing-queues-in-postgresql-3f6e9ab724fa
			 */
			StringBuilder sql = new StringBuilder("delete "
					+ "from submission_queue q "
					+ "where q.element_identifier = "
					+ "(select q_inner.element_identifier "
					+ "from submission_queue q_inner ");
			if(!incRestore) {
				sql.append("where not restore ");
			}
			sql.append("order by q_inner.time_inserted ASC "
					+ "for update skip locked "
					+ "limit 1) ");
			
			sql.append("returning q.time_inserted, q.ue_id, q.payload");
			PreparedStatement pstmt = null;

			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			Subscriber subscriber = new SubRelationalDB();

			// Default to English though we could get the locales from a server level setting
			Locale locale = new Locale("en");
			ResourceBundle localisation;
			try {
				localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			} catch(Exception e) {
				localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", locale);
			}

			boolean loop = true;
			while(loop) {

				String submissionControl = GeneralUtilityMethods.getSettingFromFile(basePath + "/settings/subscriber");
				if(submissionControl != null && submissionControl.equals("stop")) {
					log.info("---------- Submission Queue Stopped");
					loop = false;
				} else {					
					try {

						// Make sure we have a connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
						GeneralUtilityMethods.getSubmissionServer(dbc.sd);

						pstmt = dbc.sd.prepareStatement(sql.toString());
						pstmtResultsDB = dbc.sd.prepareStatement(sqlResultsDB);

						/*
						 * Dequeue
						 */	
						ResultSet rs = pstmt.executeQuery();
						if(rs.next()) {

							log.info("------ Start ");
							
							// Apply Submission to the database		
							UploadEvent ue = gson.fromJson(rs.getString("payload"), UploadEvent.class);

							log.info("        Retrieved Survey From Queue:" + ue.getSurveyName() + ":" + ue.getId());

							SubscriberEvent se = new SubscriberEvent();
							if(UploadEvent.SMS_TYPE.equals(ue.getType())) {
								// SMS
								System.out.println("------------ Processing SMS message");
								SMSDetails sms = gson.fromJson(ue.getPayload(), SMSDetails.class);
								
								SMSInboundManager sim = new SMSInboundManager();
								sim.processMessage(dbc.sd, dbc.results, sms, se);
							} else {
								// Form
							
								SurveyInstance instance = null;	
								String uploadFile = ue.getFilePath();
	
								log.info("Upload file: " + uploadFile);
								InputStream is = null;
								InputStream is3 = null;
	
								int oId = 0;
	
								ArrayList<MediaChange> mediaChanges = null;
	
								try {
									// Get the organisation locales
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
									Survey sdalSurvey = sm.getSurveyId(dbc.sd, templateName);	// Get the survey from the templateName / ident
	
									template.readDatabase(dbc.sd, dbc.results, templateName, false);					
									template.extendInstance(dbc.sd, instance, true, sdalSurvey);	// Extend the instance with information from the template
									// instance.getTopElement().printIEModel("   ");	// Debug
	
									// Get attachments from incomplete submissions
									getAttachmentsFromIncompleteSurveys(dbc.sd, log, ue.getFilePath(), ue.getOrigSurveyIdent(), ue.getIdent(), 
											ue.getInstanceId());
	
									is3 = new FileInputStream(uploadFile);	// Get an input stream for the file in case the subscriber uses that rather than an Instance object
									mediaChanges = subscriber.upload(log, instance, 
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
	
										
	
									} catch (Exception e) {
	
									}
								}
							
								/*
								 * Perform post processing of the XML file
								 * Send it to S3 if that is enabled
								 * Update any media names with the final media names
								 */
								if(mediaChanges != null && mediaChanges.size() > 0) {
									processMediaChanges(log, uploadFile, mediaChanges);
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
								if(status == null) {
									status = "success";
								}
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

								lm.writeLog(dbc.sd, ue.getSurveyId(), ue.getUserName(), topic, status + " : " 
										+ (reason == null ? "" : reason) + " : " + ue.getImei(), 0, null);
								
							}

							/*
							 * Save the status
							 */
							pstmtResultsDB.setString(1, se.getStatus());
							pstmtResultsDB.setString(2, se.getReason());
							pstmtResultsDB.setString(3, queueName);
							pstmtResultsDB.setInt(4, ue.getId());
							pstmtResultsDB.executeUpdate();	

							log.info("======== Completed");
						} else {
							// Sleep and then go again
							try {
								Thread.sleep(delaySecs * 1000);
							} catch (Exception e) {
								// ignore
							}
						}

					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					} finally {
						try {if (pstmtResultsDB != null) { pstmtResultsDB.close();}} catch (SQLException e) {}
						try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
					}

				}

			}
		}
	}

	/**
	 * @param args
	 */
	public void go(String smapId, String basePath, String queueName, boolean incRestore) {

		confFilePath = "./" + smapId;

		try {

			// Process submissions in queue
			Thread t = new Thread(new SubmissionQueueLoop(basePath, queueName, incRestore));
			t.start();


		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			/*
			 * Do not close connections!  This processor is supposed to run forever
			 */

		}

	}

	/*
	 * FieldTask sends large attachments in separate "incomplete" posts
	 * Get these now
	 * Only do it once for all subscribers, so once the attachments from the incomplete posts have been moved
	 *  to the complete submission then the first subscriber to do this will be marked as having processed the
	 *  attachments.  All other subscribers will then ignore incomplete attachments
	 */
	private void getAttachmentsFromIncompleteSurveys(Connection sd, 
			Logger log,
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
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, origIdent);
			pstmt.setString(2, ident);
			pstmt.setString(3, instanceId);

			pstmtUpdate = sd.prepareStatement(sqlUpdate);

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

	/*
	 * Add the updated paths to media to the uploaded XML file
	 */
	private void processMediaChanges(Logger log, String uploadFile, ArrayList<MediaChange> mediaChanges) {
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
}
