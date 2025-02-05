package org.smap.model;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.ApplicationWarning;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.managers.LanguageCodeManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.XLSTemplateUploadManager;
import org.smap.sdal.model.ChangeElement;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.FormLength;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.Message;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.Survey;
import org.smap.server.utilities.JavaRosaUtilities;
import org.smap.server.utilities.PutXForm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SurveyTemplateManager {
	
	private static Logger log =
			 Logger.getLogger(SurveyTemplateManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	/*
	 * Upload a template in XLSForm format
	 */
	public Response uploadTemplate(HttpServletRequest request) {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		Authorise auth = new Authorise(authorisations, null);
		
		Response response = null;
		String connectionString = "CreateXLSForm-uploadForm";
		
		log.info("upload survey -----------------------");
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();
		String displayName = null;
		int projectId = -1;
		int surveyId = -1;
		String fileName = null;
		String type = null;			// xls or xlsx or xml
		boolean isExcel;
		FileItem fileItem = null;
		String user = request.getRemoteUser();
		String action = null;
		int existingSurveyId = 0;	// The ID of a survey that is being replaced
		String bundleSurveyIdent = null;

		fileItemFactory.setSizeThreshold(5*1024*1024); 
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		Connection sd = SDDataSource.getConnection(connectionString); 
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		ArrayList<ApplicationWarning> warnings = new ArrayList<> ();

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		PreparedStatement pstmtChangeLog = null;
		PreparedStatement pstmtUpdateChangeLog = null;
		
		try {
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			
			boolean superUser = false;
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			
			// Authorise the user
			auth.isAuthorised(sd, request.getRemoteUser());
            
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();
			while(itr.hasNext()) {
				
				FileItem item = (FileItem) itr.next();

				if(item.isFormField()) {
					if(item.getFieldName().equals("templateName")) {
						displayName = item.getString("utf-8");
						if(displayName != null) {
							displayName = displayName.trim();
						}
						log.info("Template: " + displayName);
						
						
					} else if(item.getFieldName().equals("projectId")) {
						projectId = Integer.parseInt(item.getString());
						log.info("Project: " + projectId);
						
						if(projectId < 0) {
							throw new Exception("No project selected");
						} else {
							// Authorise access to project
							auth.isValidProject(sd, request.getRemoteUser(), projectId);
						}
						
					} else if(item.getFieldName().equals("surveyId")) {
						try {
							surveyId = Integer.parseInt(item.getString());	
						} catch (Exception e) {
							
						}
						// Authorise access to existing survey
						// Hack.  Because the client is sending surveyId's instead of idents we need to get the latest
						// survey id or we risk updating an old version
						if(surveyId > 0) {
							surveyId = GeneralUtilityMethods.getLatestSurveyId(sd, surveyId);
							auth.isValidSurvey(sd, request.getRemoteUser(), surveyId, false, superUser);	// Check the user has access to the survey
						}
						
						log.info("Add to bundle: " + surveyId);
						
					} else if(item.getFieldName().equals("action")) {						
						action = item.getString();
						log.info("Action: " + action);
						
					} else {
						log.info("Unknown field name = "+item.getFieldName()+", Value = "+item.getString());
					}
				} else {
					fileItem = (FileItem) item;
				}
			} 
			
			// Get the file type from its extension
			fileName = fileItem.getName();
			if(fileName == null || fileName.trim().length() == 0) {
				throw new ApplicationException(localisation.getString("tu_nfs"));
			} else if(fileName.endsWith(".xlsx")) {
				type = "xlsx";
				isExcel = true;
			} else if(fileName.endsWith(".xls")) {
				type = "xls";
				isExcel = true;
			} else if(fileName.endsWith(".xml")) {
				type = "xml";
				isExcel = false;
			} else {
				throw new ApplicationException(localisation.getString("tu_uft"));
			}
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			Survey existingSurvey = null;
			String basePath = GeneralUtilityMethods.getBasePath(request);
			
			HashMap<String, String> groupForms = null;		// Maps form names to table names - When merging to an existing survey
			HashMap<String, QuestionForm> questionNames = null;	// Maps unabbreviated question names to abbreviated question names
			HashMap<String, String> optionNames = null;		// Maps unabbreviated option names to abbreviated option names
			int existingVersion = 1;						// Make the version of a survey that replaces an existing survey one greater
			boolean merge = false;							// Set true if an existing survey is to be replaced or this survey is to be merged with an existing survey
			
			if(surveyId > 0) {
				
				/*
				 * Either a survey is being replaced, in which case the action is "replace"
				 * or a survey is being added to a bundle in which case the action is "add"
				 */
				
				merge = true;
				bundleSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, surveyId);
				groupForms = sm.getGroupForms(sd, bundleSurveyIdent);
				questionNames = sm.getGroupQuestionsMap(sd, bundleSurveyIdent, null, false);	
				optionNames = sm.getGroupOptions(sd, bundleSurveyIdent);
				
				/*
				 * Check that the bundle of the new / replaced survey does not have roles
				 * If it does only the super user can add a new or replace a survey
				 */
				if(!superUser && GeneralUtilityMethods.bundleHasRoles(sd, request.getRemoteUser(), bundleSurveyIdent)) {
					throw new ApplicationException(localisation.getString("tu_roles"));
				}
			}
			
			if(action == null) {
				action = "add";
			} else if(action.equals("replace")) {
				
				existingSurvey = sm.getById(sd, cResults, user, false, surveyId, 
						false, basePath, null, false, false, false, 
						false, false, null, false, false, superUser, null, 
						false,
						false,		// launched only
						false		// Don't merge set values into default value
						);
				displayName = existingSurvey.surveyData.displayName;
				existingVersion = existingSurvey.surveyData.version;
				existingSurveyId = existingSurvey.surveyData.id;
			}

			// If the survey display name already exists on this server, for this project, then throw an error		

			if(!action.equals("replace") && sm.surveyExists(sd, displayName, projectId)) {
				String msg = localisation.getString("tu_ae");
				msg = msg.replaceAll("%s1", displayName);
				throw new ApplicationException(msg);
			}
			
			Survey s = null;					// Used for XLS uploads (The new way)
			SurveyTemplate model = null;		// Used for XML uploads (The old way), Convert to Survey after loading
			if(isExcel) {
				XLSTemplateUploadManager tum = new XLSTemplateUploadManager(localisation, warnings);
				s = tum.getSurvey(sd, 
						oId, 
						type, 
						fileItem.getInputStream(), 
						displayName,
						projectId,
						questionNames,
						optionNames,
						merge,
						existingVersion);
			} else if(type.equals("xml")) {

				// Parse the form into an object model
				PutXForm loader = new PutXForm(localisation);
				model = loader.put(fileItem.getInputStream(), 
						request.getRemoteUser(),
						basePath);	// Load the XForm into the model
				
				// Set the survey name to the one entered by the user 
				if(displayName != null && displayName.length() != 0) {
					model.getSurvey().setDisplayName(displayName);
				} else {
					throw new Exception("No Survey name");
				}

				// Set the project id to the one entered by the user 
				if(projectId != -1) {
					model.getSurvey().setProjectId(projectId);
				} 
				
			} else {
				throw new ApplicationException("Unsupported file type");
			}
				
			/*
			 * Get information on a survey group if this survey is to be added to one
			 * TODO make XML uploads work with this
			 */
			if(surveyId > 0) {
				if(!action.equals("replace")) {
					s.surveyData.groupSurveyIdent = bundleSurveyIdent;
					
				} else {
					// Set the group survey ident to the same value as the original survey
					s.surveyData.groupSurveyIdent = existingSurvey.surveyData.groupSurveyIdent;
					s.surveyData.publicLink = existingSurvey.surveyData.publicLink;
				}

				/*
				 * Validate that the survey is compatible with any groups that it
				 * has been added to
				 */
				for(Form f : s.surveyData.forms) {
					for(Question q : f.questions) {
						QuestionForm qt = questionNames.get(q.name);
						if(qt != null) {
							if(!qt.reference && !qt.formName.equals(f.name) && GeneralUtilityMethods.isDatabaseQuestion(qt.qType)) {
								String msg = localisation.getString("tu_gq");
								msg = msg.replace("%s1", q.name);
								msg = msg.replace("%s2", f.name);
								msg = msg.replace("%s3", qt.formName);
								throw new ApplicationException(msg);
							}
						}
					}
				}
			} 
			
			/*
			 * Save the survey to the database
			 */
			if(isExcel) {
				s.write(sd, cResults, localisation, request.getRemoteUser(), groupForms, existingSurveyId, oId);
				String msg = null;
				String title = null;
				if(action.equals("replace")) {
					msg = localisation.getString("log_sr");
					title = LogManager.REPLACE;
				} else {
					msg = localisation.getString("log_sc");
					title = LogManager.CREATE;
				}
				
				msg = msg.replace("%s1", s.getDisplayName()).replace("%s2", s.getIdent());
				lm.writeLog(sd, s.getId(), user, title, msg, 0, request.getServerName());
				
				/*
				 * If the survey is being replaced and if it has an existing  geometry called the_geom and there is no
				 * geometry of that name in the new form then create a warning
				 * This warning can be removed in version 21.06
				 */
				if(action.equals("replace")) {
					for(Form f : s.surveyData.forms) {
						
						boolean newTheGeom = false;
						boolean newNonTheGeom = false;
						ArrayList<String> newGeomQuestions = new ArrayList<> ();
						for(Question q : f.questions) {
							if(q.type.equals("geopoint") || q.type.equals("geotrace") || q.type.equals("geoshape") || q.type.equals("geocompound")) {
								if(q.name.equals("the_geom")) {	// keep this
									newTheGeom = true;
								} else {
									newNonTheGeom = true;
									newGeomQuestions.add(q.name);
								}
							}
	
						}
						if(newNonTheGeom && !newTheGeom) {
							if(GeneralUtilityMethods.hasTheGeom(sd, existingSurveyId, f.id)) {		
								String gMsg = localisation.getString("tu_geom");
								gMsg = gMsg.replace("%s1", f.name);
								gMsg = gMsg.replace("%s2", newGeomQuestions.toString());
								warnings.add(new ApplicationWarning(gMsg));
							}
						}
					}	
				}
				
			} else {
				int sId = model.writeDatabase();
				s = sm.getById(sd, cResults, user, false, sId, true, basePath, 
						null,					// instance id 
						false, 					// get results
						false, 					// generate dummy values
						true, 					// get property questions
						false, 					// get soft deleted
						false, 					// get HRK
						"internal", 			// Get External options
						false, 					// get change history
						true, 					// get roles
						superUser, 
						null	,				// geom format
						false,					// Include child surveys
						false,					// launched only
						false		// Don't merge set values into default value
						);
			}
			
			/*
			 * Validate the survey:
			 * 	 using the JavaRosa API
			 *    Ensure that it fits within the limit of 1,600 columns
			 */
			boolean valid = true;
			String errMsg = null;
			try {
				JavaRosaUtilities.javaRosaSurveyValidation(localisation, s.surveyData.id, request.getRemoteUser(), tz, request);
			} catch (Exception e) {
								
				// Error! Delete the survey we just created
				log.log(Level.SEVERE, e.getMessage(), e);
				valid = false;
					
				errMsg = e.getMessage();			
				if(errMsg != null && errMsg.startsWith("no <translation>s defined")) {
					errMsg = localisation.getString("tu_nl");
				}					
				
			}
			if(valid) {
				ArrayList<FormLength> formLength = GeneralUtilityMethods.getFormLengths(sd, s.surveyData.id);
				for(FormLength fl : formLength) {
					if(fl.isTooLong()) {
						valid = false;
						errMsg = localisation.getString("tu_tmq");
						errMsg = errMsg.replaceAll("%s1", String.valueOf(fl.questionCount));
						errMsg = errMsg.replaceAll("%s2", fl.name);
						errMsg = errMsg.replaceAll("%s3", String.valueOf(FormLength.MAX_FORM_LENGTH));
						errMsg = errMsg.replaceAll("%s4", fl.lastQuestionName);
						break;	// Only show one form that is too long - This error should very rarely be encountered!
					}
				}
			}
			
			if(!valid) {
				sm.delete(sd, 
						cResults, 
						s.surveyData.id, 
						true,		// hard
						false,		// Do not delete the data 
						user, 
						basePath,
						"no",		// Do not delete the tables
						0);		// New Survey Id for replacement 
				throw new ApplicationException(errMsg);	// report the error
			}
					
			if(action.equals("replace")) {
				/*
				 * Soft delete the old survey
				 * Set task groups to use the new survey
				 */
				sm.delete(sd, 
						cResults, 
						surveyId, 
						false,		// set soft 
						false,		// Do not delete the data 
						user, 
						basePath,
						"no",		// Do not delete the tables
						s.surveyData.id		   // New Survey Id for replacement 
					);		
			}
			
			/*
			 * Save the FormXLS file so that it can be retrieved from the change history
			 */
			String surveyIdent = (action.equals("replace")) ? existingSurvey.getIdent() : s.surveyData.ident;
			String changeFileName = UUID.randomUUID().toString() + "." + type;
			String fileFolder = basePath + "/templates/survey/" + surveyIdent;
			String filePath = fileFolder +"/" + changeFileName; 
			String fileUrl = "/surveyKPI/file/" + changeFileName + "/change_survey/" + surveyIdent + "?name=" + fileName;
			
			// Create the folder if it does not exist
			File folder = new File(fileFolder);
			FileUtils.forceMkdir(folder);
			
			// Save file
			File savedFile = new File(filePath);
			fileItem.write(savedFile); 
			
			/*
			 * Copy the change history from the old survey to the new one that is replacing it
			 */
			int newVersion = existingVersion;
			if(action.equals("replace")) {
				newVersion++;
				String sqlUpdateChangeLog = "insert into survey_change "
						+ "(s_id, version, changes, user_id, apply_results, visible, updated_time) "
						+ "select "
						+ s.surveyData.id
						+ ",version, changes, user_id, apply_results, visible, updated_time "
						+ "from survey_change where s_id = ? "
						+ "order by version asc";
				pstmtUpdateChangeLog = sd.prepareStatement(sqlUpdateChangeLog);
				pstmtUpdateChangeLog.setInt(1, existingSurveyId);
				pstmtUpdateChangeLog.execute();
			}
			
			/*
			 * Add a new entry to the change history
			 */
			String sqlChangeLog = "insert into survey_change " +
					"(s_id, version, changes, user_id, apply_results, visible, updated_time) " +
					"values(?, ?, ?, ?, 'true', ?, ?)";
			pstmtChangeLog = sd.prepareStatement(sqlChangeLog);
			ChangeItem ci = new ChangeItem();
			ci.fileName = fileItem.getName();
			ci.origSId = s.surveyData.id;
			ci.fileUrl = fileUrl;
			pstmtChangeLog.setInt(1, s.surveyData.id);
			pstmtChangeLog.setInt(2, newVersion);
			pstmtChangeLog.setString(3, gson.toJson(new ChangeElement(ci, "upload_template")));
			pstmtChangeLog.setInt(4, GeneralUtilityMethods.getUserId(sd, user));	
			pstmtChangeLog.setBoolean(5,true);	
			pstmtChangeLog.setTimestamp(6, GeneralUtilityMethods.getTimeStamp());
			pstmtChangeLog.execute();
			
			StringBuilder responseMsg = new StringBuilder("");
			String responseCode = "success";
			if(warnings.size() > 0) {
				for(ApplicationWarning w : warnings) {
					responseMsg.append("<br/> - ").append(w.getMessage());
				}
				responseCode = "warning";
			}
			
			/*
			 * Apply auto translations
			 */
			if(s.surveyData.autoTranslate && s.surveyData.languages.size() > 1) {
				
				LanguageCodeManager lcm = new LanguageCodeManager();
				Language fromLanguage = s.surveyData.languages.get(0);
				if(fromLanguage.code != null && lcm.isSupported(sd, fromLanguage.code, LanguageCodeManager.LT_TRANSLATE)) {
					for(int i = 1; i < s.surveyData.languages.size(); i++) {
						Language toLanguage = s.surveyData.languages.get(i);
						if(toLanguage.code != null && lcm.isSupported(sd, toLanguage.code, LanguageCodeManager.LT_TRANSLATE)) {
							
							String result = sm.translate(sd, request.getRemoteUser(), s.surveyData.id,
									0,	// from language index
									i,	// to language index
									fromLanguage.code,
									toLanguage.code,
									false,	// Do not overwrite
									basePath
									);
							if(result != null) {
								responseCode = "warning";
								responseMsg.append(localisation.getString(result).replace("%s1",  LogManager.TRANSLATE));
							}
						} else {
							responseCode = "warning";
							if(toLanguage.code == null) {
								responseMsg.append(localisation.getString("aws_t_nlc").replace("%s1",  toLanguage.name));
							} else {
								responseMsg.append(localisation.getString("aws_t_ilc").replace("%s1",  toLanguage.code));
							}
						}
					}
				} else {
					responseCode = "warning";
					if(fromLanguage.code == null) {
						responseMsg.append(localisation.getString("aws_t_nlc").replace("%s1",  fromLanguage.name));
					} else {
						responseMsg.append(localisation.getString("aws_t_ilc").replace("%s1",  fromLanguage.code));
					}
				}
				
			}
			
			response = Response.ok(gson.toJson(new Message(responseCode, responseMsg.toString(), displayName))).build();
			
		} catch(AuthorisationException ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			throw ex;
		}catch(ApplicationException ex) {		
			response = Response.ok(gson.toJson(new Message("error", ex.getMessage(), displayName))).build();
		} catch(FileUploadException ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			if(pstmtChangeLog != null) try {pstmtChangeLog.close();} catch (Exception e) {}
			if(pstmtUpdateChangeLog != null) try {pstmtUpdateChangeLog.close();} catch (Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}
		
		return response;
	}
}
