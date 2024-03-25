package org.smap.sdal.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

public class SurveyDAO {
	public int id;
	public int e_id;
	public int o_id;
	public int p_id;
	public String ident;
	public String displayName;
	public String instanceNameDefn;
	public String def_lang;
	public boolean task_file;				// Set true if this data from a file can be pre-loaded into this survey
	public boolean timing_data;				// Set true if timing data is to be collected for this survey
	public boolean audit_location_data;		// Set true if location is to be recorded for each question
	public boolean myReferenceData;			// Set true if an enumerator can only get reference data they submitted from this survey
	public boolean track_changes;			// Set true if every change to a question is to be tracked
	public String surveyClass;
	public boolean deleted;
	public boolean blocked;
	public String manifest;
	public boolean hasManifest;
	public boolean autoTranslate;			// Only used on upload from XLS to do auto translation
	public ArrayList<Form> forms = new ArrayList<Form> ();
	public HashMap<String, OptionList> optionLists = new HashMap<> ();
	public HashMap<String, StyleList> styleLists = new HashMap<> ();
	public HashMap<String, ServerCalculation> serverCalculations  = new HashMap<> ();
	public ArrayList<Language> languages = new ArrayList<Language> (); 
	public ArrayList<ManifestValue> surveyManifest  = new ArrayList<> ();
	public HashMap<String, Boolean> filters = new HashMap<> ();
	public ArrayList<ChangeLog> changes  = new ArrayList<> ();
	public ArrayList<MetaItem> meta = new ArrayList<> ();
	public HashMap<String, Role> roles = new HashMap<> ();
	public InstanceResults instance = new InstanceResults();	// Data from an instance (a submitted survey)
	public String pdfTemplateName;
	public String default_logo;
	public int version;			// Default to 1
	public boolean loadedFromXLS;
	public ArrayList<Pulldata> pulldata;
	public UniqueKey uk = new UniqueKey();		 // Key details here
	public String basedOn;
	public Timestamp created;
	public boolean exclude_empty;
	public boolean compress_pdf;
	public String projectName;
	public boolean hideOnDevice;		// Replaces projectTasksOnly
	public boolean searchLocalData;
	public boolean dataSurvey = true;
	public boolean oversightSurvey = true;
	public boolean readOnlySurvey = false;
	public String groupSurveyIdent;
	public String groupSurveyDetails;
	public String publicLink;
	
	public SurveyLinks links;
}
