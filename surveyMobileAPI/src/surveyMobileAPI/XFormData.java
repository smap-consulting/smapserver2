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

package surveyMobileAPI;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.legacy.MissingSurveyException;
import org.smap.sdal.legacy.MissingTemplateException;
import org.smap.sdal.legacy.SurveyInstance;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.EmailManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.managers.QuestionManager;
import org.smap.sdal.managers.ResourceManager;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;
import org.smap.server.entities.UploadEvent;

import JdbcManagers.JdbcUploadEventManager;

class SaveDetails {
	String fileName = null;
	String instanceDir = null;
	String filePath = null;
	String auditFilePath = null;
	String origSurveyIdent = null;
	int iosImageCount = 0;
	int iosVideoCount = 0;
}

public class XFormData {

	private static Logger log = Logger.getLogger(XFormData.class.getName());

	LogManager lm = new LogManager(); // Application log

	String serverName = null;

	Authorise a = new Authorise(null, Authorise.ENUM);

	Survey survey = null;
	
	public XFormData() {

	}

	public void loadMultiPartMime(HttpServletRequest request, String user, String updateInstanceId, 
			String deviceId,
			boolean isDynamicUser)
			throws ApplicationException, MissingSurveyException, IOException, FileUploadException,
			MissingTemplateException, AuthorisationException, Exception {

		log.info("loadMultiPartMime()");

		// Use Apache Commons file upload to get the items in the file
		SaveDetails saveDetails = null;
		DiskFileItemFactory factory = new DiskFileItemFactory();
		ServletFileUpload upload = new ServletFileUpload(factory);

		List<FileItem> items = upload.parseRequest(request);
		int assignmentId = 0;
		String surveyNotes = null;
		String locationTrigger = null;

		serverName = request.getServerName();
		SurveyInstance si = null;
		String templateName = null;
		String form_status = request.getHeader("form_status");
		boolean incomplete = false; // Set true if odk has more attachments to send
		boolean superUser = false;
		String auditFilePath = null;

		Connection sd = null;
		Connection cResults = null;
		PreparedStatement pstmtIsRepeating = null;
		ResultSet rsRepeating = null;
		
		PreparedStatement pstmt = null;

		String tz = "UTC";
		
		try {
			sd = SDDataSource.getConnection("surveyMobileAPI-XFormData");
			cResults = ResultsDataSource.getConnection("surveyMobileAPI-XFormData");
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			
			String basePath = GeneralUtilityMethods.getBasePath(request);

			/*
			 * Save the XML submission file
			 */
			Iterator<FileItem> iter = items.iterator();
			String thisInstanceId = null;
			String thisStart = null;
			String thisEnd = null;
			String thisInstanceName = null;
			while (iter.hasNext()) {
				FileItem item = (FileItem) iter.next();
				String name = item.getFieldName();
				if (name.equals("xml_submission_file") || name.equals("xml_submission_data")) { // xml_submission_data is the name used by webForms
					
					si = new SurveyInstance(item.getInputStream());

					// Extend the instance with data available in the template
					// This will get a default location if one exists
					templateName = si.getTemplateName();

					saveDetails = saveToDisk(item, basePath, null, templateName, null, 0, 0);
					log.info("Saved xml_submission file:" + saveDetails.fileName + " (FieldName: " + item.getFieldName()
							+ ")");
					
					SurveyTemplate template = new SurveyTemplate(localisation);
					templateName = template.readDatabase(sd, cResults, templateName, false);  // Update the template name if the survey has been replaced
					
					SurveyManager sm = new SurveyManager(localisation, "UTC");
					survey = sm.getSurveyId(sd, templateName); // Get the survey id from the templateName / key

					template.extendInstance(sd, si, false, survey);

					thisInstanceId = si.getUuid();
					
					/*
					 * Get meta values from the instance
					 */
					
					boolean debug = false;		// debug
					log.info("----------------------------- " + survey.surveyData.displayName + " : " + debug);		// debug
					
					if(debug) {
						log.info("       meta size: " + survey.surveyData.meta.size());
					}
					String topFormPath = "/main/";
					if(survey.surveyData.meta.size() > 0) {
						// New meta items stored with survey
						for(MetaItem mi : survey.surveyData.meta) {
							if(mi.isPreload) {
								if(mi.sourceParam.equals("start")) {
									thisStart = si.getValue(topFormPath + mi.name);
								} else if(mi.sourceParam.equals("end")) {
									thisEnd = si.getValue(topFormPath + mi.name);
								}
							} else {
								if(mi.name.toLowerCase().equals("instancename")) {
									thisInstanceName = si.getValue(topFormPath + "meta/" + mi.name);
								}
							}
						}
					} else {
						// Old style meta items which were questions
						QuestionManager qm = new QuestionManager(localisation);
						ArrayList<Question> questions = qm.getQuestionsInForm(sd, 
								null, 		// Not checking for HRK so no need for results database
								survey.surveyData.id, 
								0,			// Get the top level form (pass zero) 
								false,		// Don't get deleted questions 
								true,		// Get property questions 
								false,		// Don't get HRK 
								0,			// parent form 
								null,		// HRK 
								0,			// Number of languages only required for HRK 
								null, 		// Table name only required for HRK
								basePath, 
								0,			// Organisation Id - only required to get labels 
								null,		// Survey - only required to get labels 
								false		// Don't merge setValues into default
								);		
						
						for(Question q : questions) {
							if(debug) {			// debug
								log.info("xxxxxx question: " + q.name + " : " + q.source_param);
							}
							if(q.isPreload()) {	
								if(debug) {			// debug
									log.info("       preload: " + q.name + " : " + q.source_param);
								}
								if(q.source_param != null && q.source_param.equals("start")) {
									thisStart = si.getValue(topFormPath + q.name);
								} else if(q.source_param != null && q.source_param.equals("end")) {
									thisEnd = si.getValue(topFormPath + q.name);
								} else if(q.name.toLowerCase().equals("instancename")) {
									thisInstanceName = si.getValue(topFormPath + "meta/" + q.name);
								}
							}
						}
						if(debug) {			// debug
							log.info("       ------------------");
							log.info("       end: " + thisEnd);
							log.info("       start: " + thisStart);
							log.info("       instance: " + thisInstanceName);
							log.info("       ------------------");
						}
						
					}

					break; // There is only one XML submission file
				}
			}

			/*
			 * Complete saving to disk Return error if save to disk fails (exception thrown)
			 */
			iter = items.iterator();
			int iosImageCount = 0;
			int iosVideoCount = 0;
			log.info("############################ Saving everything to disk  ######################");
			while (iter.hasNext()) {
				FileItem item = (FileItem) iter.next();
				String fieldName = item.getFieldName();
				String dataUrl = null;

				log.info("==== Item: " + fieldName);
				if (item.isFormField() && !fieldName.equals("xml_submission_data")) {
					// Check to see if this form field indicates the submission is incomplete
					if (fieldName.equals("*isIncomplete*") && item.getString().equals("yes")) {
						log.info("    ++++++ Incomplete Submission");
						incomplete = true;
					} else if ((dataUrl = item.getString()).startsWith("data:")) {
						// File Attachment from web forms
						SaveDetails attachSaveDetails = saveToDisk(item, basePath, saveDetails.instanceDir,
								templateName, dataUrl.substring(dataUrl.indexOf("base64") + 7), iosImageCount,
								iosVideoCount);
						iosImageCount = attachSaveDetails.iosImageCount;
						iosVideoCount = attachSaveDetails.iosVideoCount;
						log.info("Saved webforms attachment:" + attachSaveDetails.fileName + " (FieldName: " + fieldName
								+ ")");
					} else if (fieldName.equals("assignment_id")) {
						log.info("Got assignment id ++++++++++++++++++" + item.getString());
						try {
							assignmentId = Integer.parseInt(item.getString());
						} catch (Exception e) {

						}
					} else if (fieldName.equals("location_trigger")) {
						locationTrigger = item.getString();
					} else if (fieldName.equals("survey_notes")) {
						log.info("Got surveyNotes ++++++++++++++++++" + item.getString());
						surveyNotes = item.getString();
					} else {
						log.info("Warning FormField Ignored, Item:" + item.getFieldName() + ":" + item.getString());
					}
				} else {
					if (!fieldName.equals("xml_submission_file") && !fieldName.equals("xml_submission_data")) {
						SaveDetails attachSaveDetails = saveToDisk(item, basePath, saveDetails.instanceDir,
								templateName, null, iosImageCount, iosVideoCount);
						iosImageCount = attachSaveDetails.iosImageCount;
						iosVideoCount = attachSaveDetails.iosVideoCount;
						if(attachSaveDetails.auditFilePath != null) {
							auditFilePath = attachSaveDetails.auditFilePath;
						}
						log.info("Saved file:" + attachSaveDetails.fileName + " (FieldName: " + fieldName + ")");
					}
				}
			}
			log.info("####################### End of Saving everything to disk ##############################");

			if (survey.getDeleted()) {
				String reason = localisation.getString("submit_deleted");
				reason = reason.replace("%s1", survey.surveyData.displayName);
				if (!GeneralUtilityMethods.hasUploadErrorBeenReported(sd, user, si.getImei(), templateName, reason)) {
					writeUploadError(sd, user, survey, templateName, si, reason);
				}
				throw new ApplicationException("deleted::" + survey.surveyData.displayName);
			}
			
			// Throw an exception if the survey has been blocked from accepting any more submissions
			if (survey.getBlocked()) { 
				String reason = localisation.getString("submit_blocked");
				reason = reason.replace("%s1", survey.surveyData.displayName);
				if (!GeneralUtilityMethods.hasUploadErrorBeenReported(sd, user, si.getImei(), templateName, reason)) {
					writeUploadError(sd, user, survey, templateName, si, reason);
				}

				throw new ApplicationException("blocked::" + survey.surveyData.displayName);
			}
			
			/*
			 * Throw an exception if the submission limit for an organisation has been reached
			 */
			ResourceManager rm = new ResourceManager();
			if(!rm.canUse(sd, survey.surveyData.o_id, LogManager.SUBMISSION)) {
				String reason = localisation.getString("submission_limit");
				if (!GeneralUtilityMethods.hasUploadErrorBeenReported(sd, user, si.getImei(), templateName, reason)) {
					writeUploadError(sd, user, survey, templateName, si, reason);
				}
				EmailManager em = new EmailManager(localisation);
				StringBuilder template = new StringBuilder(localisation.getString("submission_limit_email"));
				em.alertAdministrator(sd, survey.surveyData.o_id, user, localisation, serverName, reason,
						template, LogManager.SUBMISSION, GeneralUtilityMethods.getNextEmailId(sd, null));

				throw new ApplicationException("blocked::" + reason);
			} else {
				rm.recordUsage(sd, survey.surveyData.o_id, survey.surveyData.id, LogManager.SUBMISSION, null, user, 1);
			}

			log.info("###### submitted by: " + user);
			if(assignmentId > 0 || isDynamicUser) {
				superUser = true;		// This was an assigned task, or a dynamic user, do not apply role restrictions
			} else {
				try {
					superUser = GeneralUtilityMethods.isSuperUser(sd, user);
				} catch (Exception e) {
				}
			}
			a.isValidSurvey(sd, user, survey.surveyData.id, false, superUser); // Throw an exception if the user is not authorised
																	// to upload this survey
			
			/*
			 * DeviceId should be included in the survey contents, if it is not there then
			 * attempt to use the deviceId passed as a parameter in the submission
			 */
			String masterDeviceId = si.getImei();
			if (masterDeviceId == null || masterDeviceId.trim().length() == 0
					|| masterDeviceId.equals("deviceid not found")) {
				masterDeviceId = deviceId;
				if (masterDeviceId != null && masterDeviceId.startsWith("android_id")) {
					masterDeviceId = masterDeviceId.substring(11);
				}
			}
			
			// Get the action if it exists
			ActionManager am = new ActionManager(localisation, tz);
			Action action = am.getAction(sd, user);

			// Write the upload event
			UploadEvent ue = new UploadEvent();
			
			if(action != null) {
				ue.setTemporaryUser(true);
			} else {
				ue.setTemporaryUser(false);
			}
			
			if(action != null && action.email != null) {
				if(action.anonymousCampaign) {
					ue.setUserName(action.campaignName);
				} else {
					ue.setUserName(action.email);
				}
			} else {
				ue.setUserName(user);
			}
			ue.setFormStatus(form_status);
			ue.setServerName(serverName);
			ue.setSurveyId(survey.surveyData.id);
			ue.setIdent(templateName);
			ue.setFilePath(saveDetails.filePath);
			ue.setAuditFilePath(auditFilePath);
			ue.setProjectId(survey.getPId());
			ue.setOrganisationId(survey.surveyData.o_id);
			ue.setEnterpriseId(survey.surveyData.e_id);
			ue.setUploadTime(new Date());
			ue.setFileName(saveDetails.fileName);
			ue.setSurveyName(survey.getDisplayName());
			ue.setUpdateId(updateInstanceId);
			ue.setAssignmentId(assignmentId);
			ue.setInstanceId(thisInstanceId);
			ue.setLocation(si.getSurveyGeopoint());
			ue.setImei(masterDeviceId);
			ue.setOrigSurveyIdent(saveDetails.origSurveyIdent);
			ue.setStatus("success"); // Not really needed any more as status is really set in the subscriber event
			ue.setIncomplete(incomplete);
			ue.setLocationTrigger(locationTrigger);
			ue.setSurveyNotes(surveyNotes);
			ue.setStart(thisStart);
			ue.setEnd(thisEnd);
			ue.setScheduledStart(GeneralUtilityMethods.getScheduledStart(sd, assignmentId));
			ue.setInstanceName(thisInstanceName);
			ue.setType(SMSManager.FORM_TYPE);

			JdbcUploadEventManager uem = null;
			try {
				uem = new JdbcUploadEventManager(sd);
				uem.write(ue, false);
			} finally {
				if (uem != null) {
					uem.close();
				}
			}
			
			/*
			 * Process temporary user uploads 
			 * who can only submit one result then delete that temporary user
			 */
			UserManager um = new UserManager(localisation);
			if(action != null) {
				// If this is for a temporary user then process the Action Details
				if(action.single) {
					um.deleteSingleSubmissionTemporaryUser(sd, user, UserManager.STATUS_COMPLETE);
				}
				if(action.mailoutPersonId > 0) {
					MailoutManager mm = new MailoutManager(localisation);
					mm.setMailoutStatus(sd, action.mailoutPersonId, 
							MailoutManager.STATUS_COMPLETE, null);
				}
			}
			
			// If assignment Id is known then set the status now so that
			// when a webform refreshes it will immediately get the updated status
			if(assignmentId > 0) {
				String sql = "update assignments set status = 'submitted', completed_date = now() "
						+ "where id = ? ";
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1,  assignmentId);
				pstmt.executeUpdate();
			}
			
			log.info("userevent: " + user + " : upload results : " + si.getDisplayName());

		} finally {
			try {if (rsRepeating != null) {rsRepeating.close();}} catch (SQLException e) {}
			try {if (pstmtIsRepeating != null) {pstmtIsRepeating.close();}} catch (SQLException e) {}
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {SDDataSource.closeConnection("surveyMobileAPI-XFormData", sd);} catch(Exception e) {}
			try {ResultsDataSource.closeConnection("surveyMobileAPI-XFormData", cResults);}catch(Exception e) {}
		}
	}

	private void writeUploadError(Connection sd, String user, Survey survey, String templateName, SurveyInstance si,
			String reason) throws Exception {
		log.info("Writing upload error");
		UploadEvent ue = new UploadEvent();
		ue.setUserName(user);
		ue.setServerName(serverName);
		ue.setSurveyId(survey.surveyData.id);
		ue.setIdent(templateName);
		ue.setProjectId(survey.getPId());
		ue.setUploadTime(new Date());
		ue.setSurveyName(survey.getDisplayName());
		ue.setLocation(si.getSurveyGeopoint());
		ue.setImei(si.getImei());
		ue.setStatus("error"); // Not really needed any more as status is really set in the subscriber event
		ue.setReason(reason);

		JdbcUploadEventManager uem = null;
		try {
			uem = new JdbcUploadEventManager(sd);
			uem.write(ue, true);
			
		} finally {
			if (uem != null) {
				uem.close();
			}
		}

		lm.writeLog(sd, survey.surveyData.id, user, LogManager.ERROR, reason, 0, null); // Write the application log

	}

	private SaveDetails saveToDisk(FileItem item, String basePath, String instanceDir,
			String templateName, String base64Data, int iosImageCount, int iosVideoCount) throws Exception {

		SaveDetails saveDetails = new SaveDetails();
		saveDetails.iosImageCount = iosImageCount;
		saveDetails.iosVideoCount = iosVideoCount;

		// Set the item name
		log.info("-------------------------------- item name: " + item.getName());
		log.info("-------------------------------- field name: " + item.getFieldName());
		log.info("-------------------------------- base64: " + base64Data);
		String itemName = item.getName();
		if (base64Data != null) {
			itemName = item.getFieldName();
		}
		if (itemName == null) {
			itemName = "none";
		}
		String[] splitFileName = itemName.split("[/]");
		saveDetails.origSurveyIdent = splitFileName[splitFileName.length - 1].replaceAll("[ ]", "\\ ");
		log.info("Save to disk: " + saveDetails.origSurveyIdent);

		/*
		 * Modify file name if it is image.jpg or capturedvideo.MOV ios (version 4 at
		 * least) always sets image file names to image.jpg When sending from webforms
		 * on ios if the file name is image.jpg it the data will be changed to refer to
		 * image_0.jpg, image_1.jpg We need to rename the files to match these names
		 */
		if (saveDetails.origSurveyIdent.equals("image.jpg")) {
			saveDetails.origSurveyIdent = "image_" + saveDetails.iosImageCount + ".jpg";
			saveDetails.iosImageCount++;
		}
		if (saveDetails.origSurveyIdent.equals("capturedvideo.MOV")) {
			saveDetails.origSurveyIdent = "capturedvideo_" + saveDetails.iosVideoCount + ".MOV";
			saveDetails.iosVideoCount++;
		}
		/*
		 * Use UUID's for the instance folder and the name of the instance xml file The
		 * folder and the instance file should have the same name as this is the odk
		 * convention However they need to be globally unique to prevent clashes
		 * resulting from 2 phones submitting the same instance name
		 */
		if (item.getFieldName().equals("xml_submission_file") || item.getFieldName().equals("xml_submission_data")) {
			instanceDir = String.valueOf(UUID.randomUUID()); // Use UUIDs for filenames
			saveDetails.fileName = instanceDir + ".xml";
		} else {
			saveDetails.fileName = saveDetails.origSurveyIdent;
		}

		String surveyPath = basePath + "/uploadedSurveys/" + templateName;
		String instancePath = surveyPath + "/" + instanceDir;
		File folder = new File(surveyPath);
		FileUtils.forceMkdir(folder);
		folder = new File(instancePath);
		FileUtils.forceMkdir(folder);

		saveDetails.filePath = instancePath + "/" + saveDetails.fileName;
		log.info("Saving to:" + saveDetails.filePath);
		
		// set the audit file path if this is an audit file
		if(saveDetails.fileName.equals("audit.csv")) {
			saveDetails.auditFilePath = saveDetails.filePath;
		}
		
		try {
			File savedFile = new File(saveDetails.filePath);
			if (base64Data != null) {
				FileUtils.writeByteArrayToFile(savedFile, Base64.decodeBase64(base64Data));
			} else {
				item.write(savedFile);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}

		saveDetails.instanceDir = instanceDir;
		return saveDetails;
	}

}
