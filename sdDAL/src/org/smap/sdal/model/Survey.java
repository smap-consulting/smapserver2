package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.KeyManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.RoleManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/*
 * Survey Class
 * Used for survey editing
 */
public class Survey {
	
	public SurveyDAO surveyData = new SurveyDAO();	// Store data in a DAO for conversion to JSON
	
	private static Logger log = Logger.getLogger(Survey.class.getName());
	private HtmlSanitise sanitise = new HtmlSanitise();
	
	// Getters
	public int getId() {return surveyData.id;}; 
	public int getPId() {return surveyData.p_id;};
	public String getProjectName() {return surveyData.projectName;}; 
	public String getIdent() {return surveyData.ident;};
	public String getDisplayName() {return surveyData.displayName;}; 
	public boolean getDeleted() { return surveyData.deleted;};
	public boolean getBlocked() { return surveyData.blocked;};
	public boolean hasManifest() { return surveyData.hasManifest;};
	public boolean getHideOnDevice() { 
		return surveyData.hideOnDevice;
	};
	public boolean getSearchLocalData() { 
		return surveyData.searchLocalData;
	};
	public boolean getReadOnlySurvey() { 
		return surveyData.readOnlySurvey;
	};
	
	public Form getFirstForm() {
		Form form = null;
		
		for(int i = 0; i < surveyData.forms.size(); i++) {
			Form f = surveyData.forms.get(i);
			if(f.parentform == 0) {
				form = f;
				break;
			}
		}
		return form;
	}
	
	public Form getSubForm(Form form, Question q) {
		Form subForm = null;

		for(int i = 0; i < surveyData.forms.size(); i++) {
			Form f = surveyData.forms.get(i);
			if(f.parentform == form.id && f.parentQuestion == q.id) {
				subForm = f;
				break;
			}
		}
		return subForm;
	}
	
	public Form getSubFormQId(Form form, int qId) {
		Form subForm = null;

		for(int i = 0; i < surveyData.forms.size(); i++) {
			Form f = surveyData.forms.get(i);
			if(f.parentform == form.id && f.parentQuestion == qId) {
				subForm = f;
				break;
			}
		}
		return subForm;
	}
	
	public int getFormIdx(int formId) {
		int idx = -1;
		for(int i = 0; i < surveyData.forms.size(); i++) {
			Form f = surveyData.forms.get(i);
			if(f.id == formId) {
				idx = i;
				break;
			}
		}
		return idx;
	}
	
	// Get a name for the survey instance
	public String getInstanceName() {
		String instanceName = "survey";
		
		ArrayList<Result> results = surveyData.instance.results.get(0);
		
		for(Result r : results) {
			if(r.name.toLowerCase().equals("instancename")) {	
				if(r.value != null && r.value.trim().length() != 0) {
					instanceName = r.value;		
				}
				break;
			}
		}
		return instanceName;
	}
	
	// Get a name for the survey hrk
	public InstanceMeta getInstanceMeta() {
		InstanceMeta im = new InstanceMeta();
		im.surveyname = surveyData.displayName;

		if(surveyData.instance.results.size() > 0) {
			ArrayList<Result> results = surveyData.instance.results.get(0);
			for(Result r : results) {
				if(r.name.toLowerCase().equals("_hrk")) {	
					if(r.value != null && r.value.trim().length() != 0) {
						im.hrk = r.value;		
					}
				} else if(r.name.toLowerCase().equals("instancename")) {	
					if(r.value != null && r.value.trim().length() != 0) {
						im.instancename = r.value;		
					}
				} else if(r.name.toLowerCase().equals("user")) {	
					if(r.value != null && r.value.trim().length() != 0) {
						im.username = r.value;		
					}
				} else if(r.name.toLowerCase().equals("_device")) {	
					if(r.value != null && r.value.trim().length() != 0) {
						im.device = r.value;		
					}
				} else if(r.name.toLowerCase().equals("_assigned")) {	
					if(r.value != null && r.value.trim().length() != 0) {
						im.assigned = r.value;		
					}
				}
			}
		}
		return im;
	}
	
	// Setters
	public void setId(int v) { surveyData.id = v;};
	public void setIdent(String v) { surveyData.ident = v;};
	public void setDisplayName(String v) { surveyData.displayName = v;};
	public void setDeleted(boolean v) { surveyData.deleted = v;};
	public void setBlocked(boolean v) { surveyData.blocked = v;};
	public void setHasManifest(boolean v) { surveyData.hasManifest = v;};
	public void setVersion(int v) { surveyData.version = v;};
	public void setLoadedFromXLS(boolean v) { surveyData.loadedFromXLS = v;};
	public void setProjectName(String v) { surveyData.projectName = v;};
	public void setProjectId(int v) { surveyData.p_id = v;};
	public void setHideOnDevice(boolean v) { surveyData.hideOnDevice = v;};
	public void setSearchLocalData(boolean v) { surveyData.searchLocalData = v;};
	
	/*
	 * Write a survey to the database
	 * If this survey is to be attached to a group survey then
	 *   1. Get a list of form names and from ids to be used when available
	 *   2. Forms will only be created if they do not already exist
	 *   2. questions and choices will only be created if they do not already exist in the form
	 */
	public void write(Connection sd, Connection cRel, ResourceBundle localisation, 
			String userIdent, HashMap<String, String> groupForms, int existingSurveyId, int oId) throws Exception {
		
		try {
			log.info("Set autocommit false");
			sd.setAutoCommit(false);
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			
			writeSurvey(sd, cRel, localisation, gson, userIdent, oId);
			GeneralUtilityMethods.setLanguages(sd, surveyData.id, surveyData.languages);
			writeLists(sd, gson);
			writeStyles(sd, gson);
			writeForms(sd, localisation, groupForms, existingSurveyId);	
			updateForms(sd);		// Set parent form id and parent question id for forms
			writeRoles(sd, localisation, gson, userIdent);
			
			// If this survey has been added on top of existing tables then mark columns published if they already exist
			GeneralUtilityMethods.setPublished(sd, cRel, surveyData.id);
			
			// Notify devices
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, surveyData.id, 0);
			// Update the form dependencies so that when new results are received it is simple to identify the impacted forms			
			GeneralUtilityMethods.updateFormDependencies(sd, surveyData.id);
			
			if(!sd.getAutoCommit()) {	// auto commit may have been reenabled when updating roles
				sd.commit();
			}
			
		} catch (Exception e) {
			try {sd.rollback();} catch (Exception ex) {}
			throw e;
		} finally {
			log.info("Set autocommit true");
			try {sd.setAutoCommit(true);} catch (Exception e) {}
		}
	}
	
	/*
	 * Private methods that support writing to the survey to the database
	 * 1. Write the survey definition
	 */
	private void writeSurvey(Connection sd, Connection cResults, ResourceBundle localisation, Gson gson, String userIdent, int oId) throws Exception {
		
		String sql = "insert into survey ("
				+ "s_id, "
				+ "last_updated_time, "
				+ "display_name, "
				+ "p_id, "
				+ "def_lang, "
				+ "class,"
				+ "ident,"
				+ "version,"
				+ "manifest,"
				+ "instance_name,"
				+ "loaded_from_xls,"
				+ "meta,"
				+ "task_file,"
				+ "group_survey_ident,"
				+ "hrk,"
				+ "key_policy,"
				+ "created,"
				+ "public_link,"
				+ "pulldata,"
				+ "hide_on_device,"
				+ "search_local_data,"
				+ "data_survey,"
				+ "oversight_survey,"
				+ "read_only_survey,"
				+ "my_reference_data,"
				+ "timing_data,"
				+ "audit_location_data,"
				+ "track_changes,"
				+ "auto_translate,"
				+ "default_logo,"
				+ "compress_pdf) "
				+ "values (nextval('s_seq'), now(), ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";		
		PreparedStatement pstmt = null;
		
		String sqlUpdate = "update survey set "
				+ "ident = ?, "
				+ "group_survey_ident = ? "
				+ "where s_id = ?;";
		PreparedStatement pstmtUpdate = null;

		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			
			pstmt.setString(1, HtmlSanitise.checkCleanName(surveyData.displayName, localisation));		
			pstmt.setInt(2, surveyData.p_id);				
			pstmt.setString(3, surveyData.def_lang);
			pstmt.setString(4, surveyData.surveyClass);	
			pstmt.setString(5, surveyData.ident);
			pstmt.setInt(6, surveyData.version);			
			pstmt.setString(7, surveyData.manifest);
			pstmt.setString(8, surveyData.instanceNameDefn);
			pstmt.setBoolean(9, surveyData.loadedFromXLS);
			pstmt.setString(10, gson.toJson(surveyData.meta));
			pstmt.setBoolean(11, surveyData.task_file);
			pstmt.setString(12, surveyData.groupSurveyIdent);
			pstmt.setString(13, surveyData.uk.key);			// Obsolete - Keys no longer required per survey
			pstmt.setString(14, surveyData.uk.key_policy);		// Obsolete - Keys no longer required per survey
			pstmt.setString(15, surveyData.publicLink);
			String pd = null;
			if(surveyData.pulldata != null) {
				pd = gson.toJson(surveyData.pulldata);
			}
			pstmt.setString(16, pd);
			pstmt.setBoolean(17, surveyData.hideOnDevice);
			pstmt.setBoolean(18, surveyData.searchLocalData);
			pstmt.setBoolean(19, surveyData.dataSurvey);
			pstmt.setBoolean(20, surveyData.oversightSurvey);
			pstmt.setBoolean(21, surveyData.readOnlySurvey);
			pstmt.setBoolean(22, surveyData.myReferenceData);
			pstmt.setBoolean(23, surveyData.timing_data);
			pstmt.setBoolean(24, surveyData.audit_location_data);
			pstmt.setBoolean(25, surveyData.track_changes);
			pstmt.setBoolean(26, surveyData.autoTranslate);
			pstmt.setString(27, surveyData.default_logo);
			pstmt.setBoolean(28, surveyData.compress_pdf);
			pstmt.executeUpdate();
			
			// If an ident was not provided then assign a new ident based on the survey id
			if(surveyData.ident == null || surveyData.ident.trim().length() == 0) {
				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					surveyData.id = rs.getInt(1);
				
					surveyData.ident = "s" + surveyData.p_id + "_" + surveyData.id;
					if(surveyData.groupSurveyIdent == null) {
						surveyData.groupSurveyIdent = surveyData.ident;
					}
					
					pstmtUpdate = sd.prepareStatement(sqlUpdate);
					pstmtUpdate.setString(1, surveyData.ident);
					pstmtUpdate.setString(2, surveyData.groupSurveyIdent);
					pstmtUpdate.setInt(3, surveyData.id);
					pstmtUpdate.executeUpdate();
				}
			}
			
			// Write the key details
			KeyManager km = new KeyManager(localisation);
			km.update(sd, surveyData.groupSurveyIdent, surveyData.uk.key, surveyData.uk.key_policy, userIdent, oId, false);	// Do not override existing key when called from XLS upload
			if(surveyData.uk.key != null && surveyData.uk.key.trim().length() > 0) {
				String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, surveyData.groupSurveyIdent);
				km.updateExistingData(sd, cResults, surveyData.uk.key, surveyData.groupSurveyIdent, tableName, 0);
			}
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Write the lists
	 * Then get the list id to be used by the question
	 */
	private void writeLists(Connection sd, Gson gson) throws SQLException {
		
		String sql = "insert into listname (s_id, name) values(?, ?);";
		PreparedStatement pstmt = null;
		
		String sqlOption = "insert into option ("
				+ "o_id, "
				+ "seq, "
				+ "ovalue,"
				+ "cascade_filters, "
				+ "externalfile, "
				+ "column_name, "
				+ "display_name, "
				+ "l_id,"
				+ "published) "
				+ "values (nextval('o_seq'), ?, ?, ?, ?, ?, ?, ?, ?);";
		PreparedStatement pstmtOption = null;
		
		String sqlUpdateOption = "update option set label_id = ? where o_id = ?";
		PreparedStatement pstmtUpdateOption = null;
		
		PreparedStatement pstmtSetLabels = null;
		String sqlSetLabels = "insert into translation (s_id, language, text_id, type, value, external) " +
				"values (?, ?, ?, ?, ?, ?)";
		
		try {
			// Creating the option list
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, surveyData.id);
			
			// Inserting an option
			pstmtOption = sd.prepareStatement(sqlOption, Statement.RETURN_GENERATED_KEYS);
			
			// Setting the label ID
			pstmtUpdateOption = sd.prepareStatement(sqlUpdateOption);
			
			// Setting the labels
			pstmtSetLabels = sd.prepareStatement(sqlSetLabels);
			pstmtSetLabels.setInt(1, surveyData.id);
			
			for(String listname : surveyData.optionLists.keySet()) {
				
				OptionList ol = surveyData.optionLists.get(listname);
				
				// 1. Create the list and get the list id
				pstmt.setString(2, listname);
				pstmt.executeUpdate();				
				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					ol.id = rs.getInt(1);
				}
				
				// 2. Insert each option with this list id
				int idx = 0;
				for(Option o : ol.options) {
					String transId = null;
					pstmtOption.setInt(1, idx++);
					pstmtOption.setString(2, o.value);
					pstmtOption.setString(3, gson.toJson(o.cascade_filters));
					pstmtOption.setBoolean(4, false);
					pstmtOption.setString(5, o.columnName);
					pstmtOption.setString(6, o.display_name);
					pstmtOption.setInt(7, ol.id);
					pstmtOption.setBoolean(8, o.published);
					pstmtOption.executeUpdate();
					
					
					rs = pstmtOption.getGeneratedKeys();
					if(rs.next()) {
						o.id = rs.getInt(1);
		
						transId = "option_" +  o.id;
						pstmtUpdateOption.setString(1, transId  + ":label");
						pstmtUpdateOption.setInt(2, o.id);
						pstmtUpdateOption.executeUpdate();
						
						// Write the labels
						UtilityMethodsEmail.setLabels(sd, surveyData.id, transId, o.labels, pstmtSetLabels, o.externalFile, sanitise);
					}
					
				}
			}
			
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmtOption != null) {try {pstmtOption.close();} catch(Exception e) {}}
			if(pstmtUpdateOption != null) {try {pstmtUpdateOption.close();} catch(Exception e) {}}
			if(pstmtSetLabels != null) {try {pstmtSetLabels.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Write the styles
	 * Then get the style id to be used by the question
	 */
	private void writeStyles(Connection sd, Gson gson) throws SQLException {
		
		String sql = "insert into style (s_id, name,style) values(?, ?, ?);";
		PreparedStatement pstmt = null;
		
		try {
			// Creating the option list
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, surveyData.id);
			
			for(String stylename : surveyData.styleLists.keySet()) {
				
				StyleList sl = surveyData.styleLists.get(stylename);
				
				// 1. Create the style and get the style id
				pstmt.setString(2, stylename);
				pstmt.setString(3, gson.toJson(sl.markup));
				pstmt.executeUpdate();				
				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					sl.id = rs.getInt(1);
				}
	
			}		
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * 2. Write the forms
	 * This creates an initial entry for a form and then gets the resultant form ID
	 */
	private void writeForms(Connection sd, ResourceBundle localisation, HashMap<String, String> groupForms, int existingSurveyId) throws Exception {
		
		String sql = "insert into form ("
				+ "f_id, "
				+ "s_id, "
				+ "name, "
				+ "table_name,"
				+ "reference,"
				+ "merge,"
				+ "replace,"
				+ "append) "
				+ "values (nextval('f_seq'), ?, ?, ?, ?, ?, ?, ?);";
		PreparedStatement pstmt = null;
		
		PreparedStatement pstmtSetLabels = null;
		String sqlSetLabels = "insert into translation (s_id, language, text_id, type, value, external) " +
				"values (?, ?, ?, ?, ?, ?)";
		
		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
			pstmt.setInt(1, surveyData.id);		// Survey Id
		
			pstmtSetLabels = sd.prepareStatement(sqlSetLabels);
			pstmtSetLabels.setInt(1, surveyData.id);
			
			for(Form f : surveyData.forms) {
				
				String formName = null;
				String cleanName = null;
				if(f.reference) {
					formName = f.referenceName;
				} else {
					formName = f.name;				
				}	
				cleanName = GeneralUtilityMethods.cleanName(formName, true, false, false);
				
				String tableName = null;
				if(groupForms != null) {
					tableName = groupForms.get(formName);
				}
				
				if(tableName == null) {
					tableName = "s" + surveyData.id + "_" + cleanName;		
				}
				
				pstmt.setString(2, f.name);
				pstmt.setString(3, tableName);
				pstmt.setBoolean(4, f.reference);
				pstmt.setBoolean(5, f.merge);
				pstmt.setBoolean(6, f.replace);
				pstmt.setBoolean(7, f.append);
				pstmt.executeUpdate();

				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					f.id = rs.getInt(1);
				}
				
				// Write Form questions
				int idx = 0;
				for(Question q : f.questions) {
					if(existingSurveyId > 0 && q.type != null && q.type.equals("select")) {
						// If replacing a survey then set the compress flag to the same value as the existing select question
						q.compressed = getExistingCompressedFlag(sd, tableName,existingSurveyId, q.name);
					}
					writeQuestion(sd, localisation, q, f.id, idx++, pstmtSetLabels);
					GeneralUtilityMethods.writeAutoUpdateQuestion(sd, surveyData.id, q.id, GeneralUtilityMethods.convertParametersToString(q.paramArray), false);
				}
				
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmtSetLabels != null) {try {pstmtSetLabels.close();} catch(Exception e) {}}
		}	
	}
	
	/*
	 * If replacing a form we don't want to change an uncompressed select to a compressed select
	 */
	private boolean getExistingCompressedFlag(Connection sd, String tableName, int existingSurveyId, String qName) throws SQLException {
		boolean compressed = true;
		String sql = "select compressed, qtype from question where qName = ? and f_id = "
				+ "(select f_id from form where s_id = ? and table_name = ? and not reference)";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, qName);
			pstmt.setInt(2,  existingSurveyId);
			pstmt.setString(3,  tableName);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {				
				String qType = rs.getString(2);
				if(qType != null && qType.equals("select")) {
					compressed = rs.getBoolean(1);
				} else {
					compressed = true;		// default to true if we are not updating an existing uncompressed select
				}
			}	
		} finally {
			if(pstmt != null) {try {pstmt.close();}catch(Exception e) {}}
		}
		return compressed;
	}
	
	/*
	 * Update the forms with
	 *  parent form
	 *  parent question
	 */
	private void updateForms(Connection sd) throws SQLException {
		
		String sql = "update form set "
				+ "parentform = ?, "
				+ "parentquestion = ?, "
				+ "repeats = ? "
				+ "where f_id = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
		
			for(Form f : surveyData.forms) {
				if(f.parentFormIndex >= 0) {
					Form parentForm = surveyData.forms.get(f.parentFormIndex);	
					Question parentQuestion = parentForm.questions.get(f.parentQuestionIndex);
					pstmt.setInt(1, parentForm.id);
					pstmt.setInt(2,  parentQuestion.id);
					pstmt.setString(3, parentQuestion.repeatCount);
					pstmt.setInt(4,  f.id);
				
					pstmt.executeUpdate();
				}
			}

		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}	
	}

	/*
	 * 2. Write the roles
	 */
	private void writeRoles(Connection sd, ResourceBundle localisation, Gson gson, String userIdent) throws Exception {
		
		String sqlGetRole = "select id from role "
				+ "where o_id = ? "
				+ "and name = ?";
		PreparedStatement pstmtGetRole = null;
		
		String sqlAssociateSurvey = "insert into survey_role (survey_ident, r_id, column_filter, row_filter, "
				+ "enabled) "
				+ "values (?, ?, ?, ?, 'true')";
		PreparedStatement pstmtAssociateSurvey = null;
		
		try {
			RoleManager rm = new RoleManager(localisation);
			
			pstmtGetRole = sd.prepareStatement(sqlGetRole);		
			pstmtGetRole.setInt(1, surveyData.o_id);
			
			HashMap <String, String> roleNames = new HashMap<>();
			for(String h : surveyData.roles.keySet()) {
				Role r = surveyData.roles.get(h);
				int rId;
			
				/*
				 * Check for duplicate role names
				 */
				if(roleNames.get(r.name) != null) {
					String msg = localisation.getString("tu_dr");
					msg = msg.replace("%s1", r.name);
					log.info("Error: " + msg);
					throw new Exception(msg); 
				} else {
					roleNames.put(r.name,  r.name);
				}
				
				/*
				 * Get existing role or create new role
				 */
				pstmtGetRole.setString(2, r.name);
				
				ResultSet rs = pstmtGetRole.executeQuery();
				if(rs.next()) {
					rId = rs.getInt(1);
				} else {
					// Create a new role
					r.desc = localisation.getString("tu_cb");
					r.desc = r.desc.replace("%s1", surveyData.displayName);
					rId = rm.createRole(sd, r, surveyData.o_id, userIdent, false);
				}
				
				// Add the column filter
				if(r.column_filter_ref != null) {
					for(RoleColumnFilterRef ref : r.column_filter_ref) {
						Question q = surveyData.forms.get(ref.formIndex).questions.get(ref.questionIndex);
						if(q != null) {
							RoleColumnFilter rcf = new RoleColumnFilter(q.id);
							r.column_filter.add(rcf);
						}		
					}
				}
				
				// Sort the column filters in order of increasing id to improve speed of matching with questions
				if(r.column_filter != null) {
					r.column_filter.sort(null);
				}
				
				// Associate the survey to the roles
				pstmtAssociateSurvey = sd.prepareStatement(sqlAssociateSurvey);
				pstmtAssociateSurvey.setString(1, surveyData.ident);
				pstmtAssociateSurvey.setInt(2, rId);
				pstmtAssociateSurvey.setString(3, gson.toJson(r.column_filter));
				pstmtAssociateSurvey.setString(4, r.row_filter);
				
				log.info("Associate survey to roles: " + pstmtAssociateSurvey.toString());
				pstmtAssociateSurvey.executeUpdate();
			
			}
		} finally {
			if(pstmtGetRole != null) {try {pstmtGetRole.close();} catch(Exception e) {}}
			if(pstmtAssociateSurvey != null) {try {pstmtAssociateSurvey.close();} catch(Exception e) {}}
		}	
	}
	
	/*
	 * 3. Write a Question
	 */
	private void writeQuestion(Connection sd, ResourceBundle localisation, Question q, int f_id, int seq, PreparedStatement pstmtSetLabels) throws Exception {
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		PreparedStatement pstmt = null;
		String sql = "insert into question ("
				+ "q_id, "
				+ "f_id, "
				+ "seq, "
				+ "qname, "
				+ "qtype, "
				+ "qtext_id, "
				+ "defaultanswer, "
				+ "infotext_id,"
				+ "visible,"
				+ "source,"
				+ "source_param,"
				+ "readonly,"
				+ "readonly_expression,"
				+ "mandatory,"
				+ "relevant,"
				+ "calculate,"
				+ "qconstraint,"
				+ "constraint_msg,"
				+ "required_expression,"
				+ "appearance,"
				+ "parameters,"
				+ "nodeset,"
				+ "nodeset_value,"
				+ "nodeset_label,"
				+ "column_name,"
				+ "published,"
				+ "l_id,"
				+ "autoplay,"
				+ "accuracy,"
				+ "dataType,"
				+ "compressed,"
				+ "display_name,"
				+ "intent,"
				+ "style_id,"
				+ "server_calculate,"
				+ "set_value,"
				+ "flash,"
				+ "trigger"
				+ ") "
				+ "values (nextval('q_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?"
					+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
					+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		try {
			
			// label reference
			String transId = null;
			String labelId = null;
			for(Label l : q.labels) {
				if(l.hasLabels()) {
					transId = f_id + "_question_" + q.columnName;
					labelId = transId + ":label";
					break;
				}
			}			 
			
			// Hint reference
			String infotextId = null;
			for(Label l : q.labels) {
				if(l.hint != null && !l.hint.isEmpty()) {
					infotextId = transId + ":hint";
					break;
				}
			}	
			
			// Set list id
			q.l_id = 0;	
			if(q.list_name != null && !q.list_name.startsWith("${")) {
				OptionList ol = surveyData.optionLists.get(q.list_name);
				if(ol == null) {
					throw new Exception("List name " + q.list_name + " not found");
				}
				q.l_id = ol.id;
			}
			
			// Set style id
			q.style_id = 0;	
			if(q.style_list != null) {
				StyleList sl = surveyData.styleLists.get(q.style_list);
				if(sl == null) {
					String msg = localisation.getString("msg_style_nf");
					msg = msg.replace("%s1", q.style_list);
					throw new Exception(msg);
				}
				q.style_id = sl.id;
			}
			
			// Set name
			String name = q.name;
			if(q.type.equals("end group")) {
				name += "_groupEnd";
				infotextId = null;
				transId = null;
			}
			
			/*
			 * Write the data
			 */
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, f_id);
			pstmt.setInt(2, seq);
			pstmt.setString(3, name);
			pstmt.setString(4, q.type);
			pstmt.setString(5, labelId);					
			pstmt.setString(6, q.defaultanswer);
			pstmt.setString(7, infotextId);
			pstmt.setBoolean(8, q.visible);
			pstmt.setString(9, q.source);
			pstmt.setString(10, q.source_param);
			pstmt.setBoolean(11, q.readonly); 
			pstmt.setString(12, q.readonly_expression);
			pstmt.setBoolean(13, q.required);
			pstmt.setString(14, q.relevant);	
			pstmt.setString(15, q.calculation);
			pstmt.setString(16, q.constraint);
			pstmt.setString(17, q.constraint_msg);
			pstmt.setString(18, q.required_expression);
			pstmt.setString(19, q.appearance);
			pstmt.setString(20, GeneralUtilityMethods.convertParametersToString(q.paramArray));
			
			String nodeset = null;
			String nodeset_value = null;
			String nodeset_label = null;
			String cascade_instance = null;
			
			if(q.type.startsWith("select") || q.type.equals("rank")) {
				if(q.list_name != null && q.list_name.startsWith("${")) {
					cascade_instance = q.list_name;
					nodeset = GeneralUtilityMethods.getNodesetForRepeat(q.choice_filter, cascade_instance);
					nodeset_value = cascade_instance;
					nodeset_label = cascade_instance;
				} else {
					cascade_instance = GeneralUtilityMethods.cleanName(q.list_name, true, false, false);
					nodeset = GeneralUtilityMethods.getNodesetFromChoiceFilter(q.choice_filter, cascade_instance);
					nodeset_value = "name";
					nodeset_label = "jr:itext(itextId)";
				}
			}
			
			pstmt.setString(21, nodeset);		
			pstmt.setString(22, nodeset_value);
			pstmt.setString(23, nodeset_label);
			
			pstmt.setString(24,  q.columnName);
			pstmt.setBoolean(25,  false);   				// published		
			pstmt.setInt(26, q.l_id);
			pstmt.setString(27, q.autoplay); 
			pstmt.setString(28, q.accuracy);
			pstmt.setString(29, q.dataType);
			
			if(q.type.equals("select")) {
				pstmt.setBoolean(30, q.compressed);
			} else {
				pstmt.setBoolean(30, true);
			}
			pstmt.setString(31,  sanitise.sanitiseHtml(q.display_name));
			pstmt.setString(32,  q.intent);
			pstmt.setInt(33,  q.style_id);
			
			String serverCalculation = null;
			if(q.server_calculation != null) {
				serverCalculation = gson.toJson(q.server_calculation);
			}
			pstmt.setString(34,  serverCalculation);
			pstmt.setString(35, q.getSetValueArrayAsString(gson));
			pstmt.setInt(36, q.flash);
			pstmt.setString(37, q.trigger);
				
			pstmt.executeUpdate();
			
			ResultSet rs = pstmt.getGeneratedKeys();
			if(rs.next()) {
				q.id = rs.getInt(1);
			}
			
			// Write the labels
			if(transId != null) {
				UtilityMethodsEmail.setLabels(sd, surveyData.id, transId, q.labels, pstmtSetLabels, false, sanitise);
			}
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
}
