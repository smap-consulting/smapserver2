package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/*
 * Survey Class
 * Used for survey editing
 */
public class Survey {
	
	private static Logger log =Logger.getLogger(Survey.class.getName());
	
	public int id;
	public int o_id;
	public int p_id;
	public String pName;
	public String ident;
	public String displayName;
	public String instanceNameDefn;
	public String def_lang;
	public boolean task_file;		// Set true if this data from a file can be pre-loaded into this survey
	public boolean timing_data;		// Set true if timing data is to be collected for this survey
	public String surveyClass;
	public boolean deleted;
	public boolean blocked;
	public boolean hasManifest;
	public ArrayList<Form> forms = new ArrayList<Form> ();
	public HashMap<String, OptionList> optionLists = new HashMap<String, OptionList> ();
	public ArrayList<Language> languages = new ArrayList<Language> (); 
	public ArrayList<ServerSideCalculate> sscList  = new ArrayList<ServerSideCalculate> ();
	public ArrayList<ManifestValue> surveyManifest  = new ArrayList<ManifestValue> ();
	public HashMap<String, Boolean> filters = new HashMap<String, Boolean> ();
	public ArrayList<ChangeLog> changes  = new ArrayList<ChangeLog> ();
	public ArrayList<MetaItem> meta = new ArrayList<> ();
	public Instance instance = new Instance();	// Data from an instance (a submitted survey)
	public String pdfTemplateName;
	public int managed_id;
	public int version;			// Default to 1
	public boolean loadedFromXLS;
	public ArrayList<Pulldata> pulldata;
	public String hrk;
	public String key_policy;
	public ArrayList<LinkedSurvey> linkedSurveys = new ArrayList<LinkedSurvey> ();
	public String basedOn;
	public boolean sharedTable;
	public Timestamp created;
	public boolean exclude_empty;
	public String autoUpdates;
	public String projectName;
	public int pId;
	public boolean projectTasksOnly;

	
	// Getters
	public int getId() {return id;}; 
	public int getPId() {return p_id;};
	public String getPName() {return pName;}; 
	public String getIdent() {return ident;};
	public String getDisplayName() {return displayName;}; 
	public boolean getDeleted() { return deleted;};
	public boolean getBlocked() { return blocked;};
	public boolean hasManifest() { return hasManifest;};
	
	public Form getFirstForm() {
		Form form = null;
		
		for(int i = 0; i < forms.size(); i++) {
			Form f = forms.get(i);
			if(f.parentform == 0) {
				form = f;
				break;
			}
		}
		return form;
	}
	
	public Form getSubForm(Form form, Question q) {
		Form subForm = null;

		for(int i = 0; i < forms.size(); i++) {
			Form f = forms.get(i);
			if(f.parentform == form.id && f.parentQuestion == q.id) {
				subForm = f;
				break;
			}
		}
		return subForm;
	}
	
	public int getFormIdx(int formId) {
		int idx = -1;
		for(int i = 0; i < forms.size(); i++) {
			Form f = forms.get(i);
			if(f.id == formId) {
				idx = i;
				break;
			}
		}
		return idx;
	}
	
	// Get the display name with any HTML reserved characters escaped
	public String getDisplayNameForHTML() {
		return GeneralUtilityMethods.esc(displayName);
	}
	
	// Get a name for the survey instance
	public String getInstanceName() {
		String instanceName = "survey";
		
		ArrayList<Result> results = instance.results.get(0);
		
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
		im.surveyname = displayName;
		
		ArrayList<Result> results = instance.results.get(0);
		
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
			}
		}
		return im;
	}
	
	// Setters
	public void setId(int v) { id = v;};
	public void setPId(int v) { p_id = v;};
	public void setPName(String v) { pName = v;};
	public void setIdent(String v) { ident = v;};
	public void setDisplayName(String v) { displayName = v;};
	public void setDeleted(boolean v) { deleted = v;};
	public void setBlocked(boolean v) { blocked = v;};
	public void setHasManifest(boolean v) { hasManifest = v;};
	public void setManagedId(int v) { managed_id = v;};
	public void setVersion(int v) { version = v;};
	public void setLoadedFromXLS(boolean v) { loadedFromXLS = v;};
	public void setProjectName(String v) { projectName = v;};
	public void setProjectId(int v) { pId = v;};
	public void setProjectTasksOnly(boolean v) { projectTasksOnly = v;};
	
	// Write a new survey to the database
	public void write(Connection sd) throws SQLException {
		log.info("Set autocommit false");
		try {
			sd.setAutoCommit(false);
			
			writeSurvey(sd);
			writeLanguages(sd);
			writeForms(sd);
			
		} finally {
			try {sd.setAutoCommit(true);} catch (Exception e) {}
		}
	}
	
	/*
	 * Private methods that support writing to the survey to the database
	 * 1. Write the survey definition
	 */
	private void writeSurvey(Connection sd) throws SQLException {
		
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
				+ "created) "
				+ "values (nextval('s_seq'), now(), ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, now());";		
		PreparedStatement pstmt = null;
		
		String sqlUpdate = "update survey set "
				+ "ident = ? "
				+ "where s_id = ?;";
		PreparedStatement pstmtUpdate = null;
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();

		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			
			pstmt.setString(1, displayName);		
			pstmt.setInt(2, p_id);				
			pstmt.setString(3, def_lang);		// TODO settings
			pstmt.setString(4, surveyClass);		// TODO settings
			pstmt.setString(5, ident);
			pstmt.setInt(6, version);			
			pstmt.setString(7, null);				// TODO manifest
			pstmt.setString(8, instanceNameDefn);	// TODO settings
			pstmt.setBoolean(9, loadedFromXLS);
			pstmt.setString(10, gson.toJson(meta));
			pstmt.executeUpdate();
						
			ResultSet rs = pstmt.getGeneratedKeys();
			if(rs.next()) {
				id = rs.getInt(1);
				
				if(ident == null || ident.trim().length() == 0) {
					String surveyName = "s" + p_id + "_" + id;
					ident = surveyName;
					
					pstmtUpdate = sd.prepareStatement(sqlUpdate);
					pstmtUpdate.setString(1, ident);
					pstmtUpdate.setInt(2, id);
					pstmtUpdate.executeUpdate();
				}
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Write to the languages table
	 */
	private void writeLanguages(Connection sd) throws SQLException {
		GeneralUtilityMethods.setLanguages(sd, id, languages);
	}
	
	/*
	 * 2. Write the forms
	 * This creates an initial entry for a form and then gets the resultant form ID
	 * Later once the questions have been written this form definition will be updated with 
	 *  parent form
	 *  parent question
	 *  repeats
	 */
	private void writeForms(Connection sd) throws SQLException {
		
		String sql = "insert into form ("
				+ "f_id, "
				+ "s_id, "
				+ "name, "
				+ "table_name) "
				+ "values (nextval('f_seq'), ?, ?, ?);";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
			pstmt.setInt(1, id);		// Survey Id
		
			for(Form f : forms) {
				pstmt.setString(2, f.name);
				pstmt.setString(3, "s" + id + "_" + GeneralUtilityMethods.cleanName(f.name, true, false, false));
				pstmt.executeUpdate();

				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					f.id = rs.getInt(1);
				}
				
				if(f.parentFormIndex == 0) {
					// TODO write automatic preloads
				}
				
				// Write Form questions
				int idx = 0;
				for(Question q : f.questions) {
					writeQuestion(sd, q, f.id, idx++);
				}
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}	
	}
	

	/*
	 * 3. Write a Question
	 */
	private void writeQuestion(Connection sd, Question q, int f_id, int seq) throws SQLException {
		
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
				+ "mandatory,"
				+ "relevant,"
				+ "calculate,"
				+ "chartdata,"
				+ "qconstraint,"
				+ "constraint_msg,"
				+ "required_msg,"
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
				+ "dataType"
				+ ") "
				+ "values (nextval('q_seq'), ?, ?, ?, ?, ?, ?, ?, ?"
					+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
					+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
		
		try {
			
			// Derive some values
			if(q.columnName == null) {
				q.columnName = GeneralUtilityMethods.cleanName(q.name, true, true, true);
			}
			
			String transId = f_id + "_question_" + q.columnName;
			String labelId = transId + ":label";
			String infotextId = null;
			for(Label l : q.labels) {
				if(l.hint != null && l.hint.trim().length() > 0) {
					infotextId = transId + ":hint";
				}
			}
			
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, f_id);
			pstmt.setInt(2, seq);
			pstmt.setString(3, q.name);
			pstmt.setString(4, q.type);					// TODO question type
			pstmt.setString(5, labelId);					
			pstmt.setString(6, "");						// default answer
			pstmt.setString(7, infotextId);
			pstmt.setBoolean(8, q.visible);				// TODO visibility
			pstmt.setString(9, q.source);
			pstmt.setString(10, q.source_param);
			pstmt.setBoolean(11, q.readonly); 			// TODO read only
			pstmt.setBoolean(12, q.required);			// TODO required
			pstmt.setString(13, q.relevant);				// TODO
			pstmt.setString(14, q.calculation);			// TODO
			pstmt.setString(15, q.chartdata);			// TODO
			pstmt.setString(16, q.constraint);			// TODO
			pstmt.setString(17, q.constraint_msg);		// TODO
			pstmt.setString(18, q.required_msg);			// TODO
			pstmt.setString(19, q.appearance);			// TODO
			pstmt.setString(20, q.parameters);			// TODO
			pstmt.setString(21, q.nodeset);				// TODO
			pstmt.setString(22, "");						// TODO Nodeset value
			pstmt.setString(23, "");						// TODO Nodeset label
			pstmt.setString(24,  q.columnName);
			pstmt.setBoolean(25,  false);    			// false
			pstmt.setInt(26, q.l_id);					// TODO
			pstmt.setString(27, q.autoplay);  			// TODO
			pstmt.setString(28, q.accuracy);  			// TODO
			pstmt.setString(29, q.dataType);

			pstmt.executeUpdate();
			
			// Write the labels
			UtilityMethodsEmail.setLabels(sd, id, transId, q.labels, "");
			
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
}
