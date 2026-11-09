package org.smap.sdal.legacy;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.ManifestInfo;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Search;
import org.smap.sdal.model.SetValue;

public class SurveyTemplate {
	
	private static Logger log =
			 Logger.getLogger(SurveyTemplate.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	// The model data
	int surveyId;
	private HashMap<String, Question> questions = new HashMap<>();
	private HashMap<String, String> questionPaths = new HashMap<>();
	private HashMap<String, ArrayList<SetValue>> triggers = new HashMap<> ();
	private HashMap<String, Option> options = new HashMap<>();
	private HashMap<String, Option> cascade_options = new HashMap<>();
	private ArrayList<CascadeInstance> cascadeInstances = new ArrayList<> ();
	private HashMap<String, Form> forms = new HashMap<String, Form>();
	private HashMap<String, HashMap<String, HashMap<String, Translation>>> translations = new HashMap<>();
	private Vector<Translation> dummyTranslations = new Vector<>();
	private HashMap<String, String> defaults = new HashMap<>();
	private Survey survey = null;
	private String user;			// The user that created this template
	private String basePath;		// Where the files are located
	private Project project = null;
	private String firstFormRef = null;
	private String firstFormName = null;
	private String xFormFormName = null;
	private int nextOptionSeq = 0;
	private int nextQuestionSeq = 0;
	private int MAX_COLUMNS = 1600 - 20;		// Max number of columns in Postgres is 1600, allow for automcatically generated columns

	private ResourceBundle localisation;
	/*
	 * Constructor
	 */
	public SurveyTemplate(ResourceBundle l) {
		localisation = l;
	}

	public HashMap<String, HashMap<String, HashMap<String, Translation>>> getTranslations() {
		return translations;
	}
	
	
	
	
	public String getDefaultLanguage() {
		if(survey != null) {
			return survey.getDefLang();
		} else {
			return null;
		}
	}
	
	public void setSurveyClass(String v) {
		survey.setSurveyClass(v);
	}
	
	public String getSurveyClass() {
		return survey.getSurveyClass();
	}
	
	public String getHrk() {
		return survey.getHrk();
	}
	
	public HashMap<String, String> getQuestionPaths() {
		return questionPaths;
	}
	
	public HashMap<String, ArrayList<SetValue>> getTriggers() {
		return triggers;
	}
	
	
	
	public boolean hasCascade() {
		if(cascadeInstances.size() == 0) {
			return false;
		} else {
			return true;
		}
	}
	
	// Return the list of cascade options that match the passed in cascade instance id
	public ArrayList <Option> getCascadeOptionList(String cascadeInstance) {
		ArrayList <Option> opts = new ArrayList <Option> ();
		Collection<Option> c = null;
		Iterator<Option> itr = null;

		c = cascade_options.values();
		itr = c.iterator();
		while (itr.hasNext()) {
			Option o = itr.next();
			if(o.getListName().equals(cascadeInstance)) {
				opts.add(o);
			}
		}
		return opts;
	}
	
	// Return the list of cascade options that match the passed in cascade instance id
	public ArrayList <CascadeInstance> getCascadeInstances() {
		return cascadeInstances;
	}
	
	
	public void setUser(String v) {
		user = v;
	}
	
	public void setBasePath(String v) {
		basePath = v;
	}
	
	
	public String getBasePath() {
		return basePath;
	}

	/*
	 * Method to get the survey object
	 * 
	 * @return Survey
	 */
	public Survey getSurvey() {
		return survey;
	}
	
	public Project getProject() {
		return project;
	}
	

	/*
	 * Method to create a new form
	 * 
	 * @param formRef the reference for the form to be created
	 * @param formName The name of the form
	 */
	public void createForm(String formRef, String formName) {
		Form f = new Form();
		f.setName(formName);
		//f.setPath(formRef);     rmpath
		forms.put(formRef, f);
	}

	/*
	 * Method to get an existing form
	 * 
	 * @param formRef the reference for the form to be retrieved
	 * 
	 * @return Form
	 */
	public Form getForm(String formRef) {
		return forms.get(formRef);
	}
	
	public List <Form> getAllForms() {
		return new ArrayList<Form>(forms.values());
	}

	public Form getFormById(int f_id) {
		Form f = null;
		ArrayList<Form> formList = new ArrayList<Form>(forms.values());
		
		for(Form form : formList) {
			if(form.getId() == f_id) {
				f = form;
				break;
			}
		}
		return f;
	}
	
    /*
     * Get a sub-form, if it exists, for the designated form and question else return null
     * 
     * @param parentForm the potential parent form
     * @param parentQuestion the potential parent question
     * @return the sub form
     */
	public Form getSubForm(Form parentForm, Question parentQuestion) {
		Form subForm = null;
		List <Form> forms = getAllForms();
		for(Form f : forms) {
			if(f.getParentForm() == parentForm.getId() && f.getParentQuestionId() == parentQuestion.getId()) {
				subForm = f;
				break;   			
			}
		}
		return subForm;
	}
	
	public String getTableName(String formName) {
		String table = null;
		List<Form> formList = new ArrayList<Form>(forms.values());
		for(Form f : formList) {
			if(f.getName().equals(formName)) {
				table = f.getTableName();
				break;
			}		
		}
		return table;
	}

	
	

	/*
	 * Method to get the reference for the first form
	 * 
	 * @return String the reference for the form to be set
	 */
	public String getFirstFormRef() {
		return firstFormRef;
	}
	
	
	public String getXFormFormName() {
		return xFormFormName;
	}

	
	
	
	public String getDefault(String ref) {
		return defaults.get(ref);
	}
	

	/*
	 * Method to get an existing question
	 * 
	 * @param questionRef the reference for the question to be retrieved
	 * 
	 * @return Question
	 */
	public Question getQuestion(String questionRef) {
		return questions.get(questionRef);
	}
	

	
	
	
	

	/*
	 * Method to get an existing option
	 * 
	 * @param optoionRef the reference for the question to be retrieved
	 * 
	 * @return Option
	 */
	public Option getOption(String optionRef) {
		return options.get(optionRef);
	}
	
	// Get an option from the cascade option bucket

	
	
	
	

	

	/*
	 * Method to read the survey template from the database when passed the key of the survey
	 * 
	 * @param surveyIdent the ident of the survey
	 */
	public String readDatabase(Connection sd, Connection cResults, String surveyIdent, boolean embedExternalSearch) throws Exception {

		JdbcSurveyManager sm = null;
		
		String newSurveyIdent = surveyIdent;
		
		try {
			// Locate the survey object
			//SurveyManager surveys = new SurveyManager(pc);
			//survey = surveys.getByIdent(surveyIdent);
	
			sm = new JdbcSurveyManager(sd, localisation);
			
			survey = sm.getByIdent(surveyIdent);
			
			if(survey != null) {
				readDatabase(survey, sd, cResults, embedExternalSearch);	// Get the rest of the survey
				newSurveyIdent = survey.getIdent();
			} else {
				log.fine("Error: Survey Template not found: " + surveyIdent);
				throw new MissingTemplateException("Error: Survey Template not found: " + surveyIdent);
			}
		} finally {
			if(sm != null) {sm.close();}
		}
		
		return newSurveyIdent;

	}
	
	/*
	 * Method to read the survey template from the database when passed the survey id
	 * 
	 * @param surveyId the primary key of the survey
	 */
	public void readDatabase(int surveyId, boolean embedExternalSearch) throws Exception {

		Connection sd = org.smap.sdal.Utilities.SDDataSource.getConnection("SurveyTemplate-Read Database");
		Connection cResults = org.smap.sdal.Utilities.ResultsDataSource.getConnection("SurveyTemplate-Read Database");
		JdbcSurveyManager sm = null;
		
		try {
			
			sm = new JdbcSurveyManager(sd, localisation);
			
			survey = sm.getById(surveyId);
	
			if(survey != null) {
				readDatabase(survey, sd, cResults, embedExternalSearch);	// Get the rest of the survey
			} else {
				log.fine("Error: Survey Template not found: " + surveyId);
				throw new MissingTemplateException("Error: Survey Template not found: " + surveyId);
			}
			
		} finally {
			if(sm != null) {sm.close();}
			org.smap.sdal.Utilities.SDDataSource.closeConnection("SurveyTemplate-Read Database", sd);
			org.smap.sdal.Utilities.ResultsDataSource.closeConnection("SurveyTemplate-Read Database", cResults);
		}

	}
	
	
	/*
	 * Method to read the remaining survey template from the database once the 
	 *  survey to be read has been identified
	 */
	private void readDatabase(Survey survey, 
			Connection sd, 
			Connection cResults, 
			boolean embedExternalSearch) throws Exception {

		int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, survey.getId());
		
		JdbcProjectManager pm = null;
		JdbcFormManager fm = null;
		JdbcQuestionManager qm = null;
		JdbcOptionManager om = null;
		JdbcTranslationManager tm = null;
		
		try {
			/*
			 * Add preloads to the questionPaths hashmap so they can be referenced
			 */
			ArrayList<MetaItem> preloads = survey.getMeta();
			if(preloads != null) {
				for(MetaItem mi : preloads) {
					if(mi.isPreload) {
						questionPaths.put(mi.name, "/main/" + mi.name);
					}
				}
			}
			
			pm = new JdbcProjectManager(sd);
			project = pm.getById(survey.getProjectId());
			
			/*
			 * Get the forms
			 */	
			fm = new JdbcFormManager(sd);
			//List <Form> formList = fPersist.getBySurvey(survey);
			List <Form> formList = fm.getBySurveyId(survey.getId());
			
			/*
			 * Get questions
			 */
			qm = new JdbcQuestionManager(sd);
			List <Question> qList = qm.getBySurveyId(survey.getId(), formList);
			
			/*
			 * Set the path of each form based using
			 *  1. The forms position in the tree of forms
			 *  2. the relative paths from its parent question (set when questions are read in)
			 */
			for(int i= 0; i < formList.size(); i++) {
				String ref = formList.get(i).getPath(formList);
				if(ref != null) {
					forms.put(ref, formList.get(i));
				}
				if(formList.get(i).getParentForm() == 0) {
					firstFormRef = ref;
				}
			}
			
			/*
			 * Post processing of question list
			 */
			for(int i= 0; i < qList.size(); i++) {
				Question q = qList.get(i);
				int f_id = q.getFormId();
				Form f = getFormById(f_id);
				String formRef = null;
				if(f != null) {
					formRef = getFormById(f_id).getPath(formList);
				} else {
					log.fine("Form not found for f_id = " + f_id);
				}
				
				q.setFormRef(formRef);
				String qRef = q.getPath();
				
				/*
				 * Get the options
				 */
				if(qRef != null) {
					
					questions.put(qRef, q);
					String qName = q.getName();
					if(qName.equals("the_geom")) {
						questionPaths.put(qName + f_id, qRef);		// temporary support for multiple the_geoms
					} else {
						questionPaths.put(qName, qRef);
					}
					
					//boolean cascade = false;
					String listName = q.getListName();
					String cascadeName = null;
					String label = q.getNodesetLabel();
					String nodeset = null;
					boolean isExternal = false;
					try {
						nodeset = UtilityMethods.getNodeset(false, 
								true, 
								null, 
								false,
								q.getNodeset(),
								q.getAppearance(false, null),
								q.getFormId(),
								q.getName(),
								false	// Not converted to XPath name hence relativePath is ignored
								);
						isExternal = GeneralUtilityMethods.isAppearanceExternalFile(q.getAppearance(true, getQuestionPaths()));
					} catch (Exception e) {
						
					}
					
					// Do not add questions in reference form into a cascade instance
					if(!f.getReference() && listName != null && label != null && (!isExternal || embedExternalSearch)) { 
						
						/*
						 * If this survey was loaded from xlsForm then the list name will not be the same as the
						 * cascade instance name
						 * Hence get the cascade instanceName
						 * (Note this code is probably not required anymore, list name should always equal the cascade name)
						 */
						if(nodeset != null) {
							int idx1 = nodeset.indexOf('(');
							int idx2 = nodeset.indexOf(')', idx1 + 1);
							cascadeName = nodeset.substring(idx1 + 2, idx2 - 1);
						} else {
							cascadeName = listName;
						}
						
						CascadeInstance ci = new CascadeInstance();
						ci.name = cascadeName;
						ci.valueKey = q.getNodesetValue();
						
						if(label.startsWith("jr:itext")) {
							// Text id reference, set the label_id
							int idx1 = label.indexOf('(');
							int idx2 = label.indexOf(')', idx1 + 1);
							ci.labelKey = label.substring(idx1 + 1, idx2);
						} else {
							ci.labelKey = label;
						}
	
						// Cascade options are shared, check that this instance has not been added already by another question
						
						if(!cascadeInstanceLoaded(cascadeName)) {
							cascadeInstances.add(ci);
							
							/*
							 * Get options for this question
							 */
							om = new JdbcOptionManager(sd);
							List <Option> oList = null;
							
							boolean includeExternal = false;
							if(embedExternalSearch) {
								includeExternal = GeneralUtilityMethods.listHasExternalChoices(sd, survey.getId(), q.getListId());
								if(includeExternal) {
									oList = getExternalList(sd, cResults, oId, survey.getId(), q, cascadeName, qList);
									// Append any additional fixed choices included in the survey template
									List <Option> internalList = om.getByListId(q.getListId());
									int idx = oList.size() + 1;
									for(Option o : internalList) {
										try {
											Integer ix = Integer.parseInt(o.getColumnName());
											if(ix != null) {
												o.setLabel(UtilityMethodsEmail.getSingleLabel(sd,
														survey.getId(),
														survey.getDefLang(),	// This is not currently multi language
														q.getListId(),
														o.getValue(),false));
												o.setLabelId(null);
												o.setExternalFile(true);
												o.setSeq(idx++);
												oList.add(o);
											}
										} catch (Exception e) {
											// We only want choices that have an integer column name
										}
									}
								}
							}				
							if(!includeExternal) {
								oList = om.getByListId(q.getListId());
							}
							
							for(int j= 0; j < oList.size(); j++) {
								Option o = oList.get(j);
								o.setQuestionRef(qRef);
								String oRef = qRef;
								if(o.getId() > 0) {
									oRef += "/" + o.getId();
								} else {
									// Option from external won't have an id
									oRef += "/" + UUID.randomUUID().toString();
								}
								o.setListName(cascadeName);
								cascade_options.put(oRef, o);
							}	
						} 
					}
			
				}
				
				/*
				 * Get triggers
				 */
				String trigger = q.getTrigger();
				if(trigger != null && !trigger.trim().equals("")) {
					String triggerQuestion = GeneralUtilityMethods.getNameFromXlsName(trigger);
					String triggerValue = q.getCalculate(false, questionPaths, getXFormFormName());
					String triggerRef = q.getName(); // This question is going to be trigger by the question identified by trigger
					
					ArrayList<SetValue> targets = triggers.get(triggerQuestion);
					if(targets == null) {
						targets = new ArrayList<SetValue> ();
						triggers.put(triggerQuestion, targets);
					}
					targets.add(new SetValue(SetValue.TRIGGER, triggerValue, triggerRef));
				}
			}	
			
			/*
			 * Get translations
			 */
			tm = new JdbcTranslationManager(sd);
			List <Translation> tList = tm.getBySurveyId(survey.getId(), embedExternalSearch);
			for(Translation t : tList) {
				HashMap<String, HashMap<String, Translation>> languageMap = translations.get(t.getLanguage());
				if(languageMap == null) {
					languageMap = new HashMap<String, HashMap <String, Translation>> ();
					translations.put(t.getLanguage(), languageMap);
				}
				
				String type = t.getType();
				String textId = t.getTextId();
				if(type.equals("guidance")) {
					textId = textId.replace("guidance_hint", "hint");	// Store hint with guidance hint
				}
				HashMap<String, Translation> types = languageMap.get(textId);
				if(types == null) {
					types = new HashMap<String, Translation> ();
					languageMap.put(textId, types);
				}
				
				types.put(type, t);			
			}
		} finally {
			if(pm != null) {pm.close();}
			if(fm != null) {fm.close();}
			if(qm != null) {qm.close();}
			if(om != null) {om.close();}
			if(tm != null) {tm.close();}
		}
	}
	
	
	/*
	 * Method to check to see whether the cascade instance has already been loaded
	 */
	public boolean cascadeInstanceLoaded(String cascadeInstanceId) {
		boolean loaded = false;
		

		
		for(int i = 0; i < cascadeInstances.size(); i++) {
		
			CascadeInstance ci = cascadeInstances.get(i);
			if(ci.name.equals(cascadeInstanceId)) {
				loaded = true;
				break;
			}
		}

		return loaded;
	}
	
	
	
	
	/*
	 * Method to extend a survey instance with information from the template
	 *  useExternalChoices is set true when processing results
	 *      Use the options as shown on the form
	 *  useExternalChoices is set false when getting an XForm
	 *      Return the dummy choice that points to the external file columns
	 */
	public void extendInstance(Connection sd, SurveyInstance instance, boolean useExternalChoices, org.smap.sdal.model.Survey sdalSurvey) throws Exception {
		List<Form> formList  = getAllForms(); 
		
		// Set the display name
		instance.setDisplayName(survey.getDisplayName());
		/*
		 * Extend the forms
		 */
		for(Form f : formList) {
			instance.setForm(f.getPath(formList), f.getTableName(), f.getType(), f.getReference());
			List <Question> questionList = f.getQuestions(sd, f.getPath(formList));
			extendQuestions(sd, instance, questionList, f.getPath(formList), useExternalChoices);
			if(!f.hasParent()) {
				extendMeta(sdalSurvey.surveyData.meta, instance);
			}
		}
	}
	
	private void extendMeta(ArrayList<MetaItem> meta, SurveyInstance instance) {
		
		instance.setQuestion("/main/meta", "begin group", "meta", false, null, null, false, null, null);
		for(MetaItem mq : meta) {
			String questionPath = null;
			if(mq.name.contains("instanceID") || mq.name.contains("instanceName") || mq.name.contains("audit")) {
				questionPath = "/main/meta/" + mq.name;
			} else {
				questionPath = "/main/" + mq.name;
			}
			instance.setQuestion(questionPath, mq.type, mq.name, false, mq.columnName, mq.dataType, false, null, null);
			
			// Use geopoint meta questions to set survey location if it has not already been set
			if(mq.type.equals("geopoint") && (instance.getSurveyGeopoint() == null || instance.getSurveyGeopoint().trim().equals(""))) {
				log.fine("+++++++ setting for: " + questionPath);
				instance.setOverallLocation(questionPath);
			}
		}
	}
	
	
	public void extendQuestions(
			Connection sd,
			SurveyInstance instance, 
			List <Question> questionList, 
			String formPath,
			boolean useExternalChoices) throws Exception {
		
		for(Question q : questionList) {
			
			String questionPath = null;
			if(q.getName().equals("the_geom")) {				
				questionPath = questionPaths.get(q.getName() + q.getFormId());	// Temp support for multiple the_geom
			} else {
				questionPath = questionPaths.get(q.getName());
			}
			
			// Set the question type for "begin group" questions
			if(q.getType() != null && q.getType().equals("begin group")) {
				
				instance.setQuestion(questionPath, q.getType(), q.getName(), q.getPhoneOnly(), q.getColumnName(false), 
						q.getDataType(), q.isCompressed(), q.getAppearance(false, null),
						q.getParameters());
				
			}
			
			if(q.getSource() != null) {
				// Extend any other questions that have a source (ie not meta data)
				
				instance.setQuestion(questionPath, q.getType(), q.getName(), 
						q.getPhoneOnly(), q.getColumnName(false), 
						q.getDataType(),
						q.isCompressed(),
						q.getAppearance(false, null),
						q.getParameters());
				
				// Set the overall survey location to the last geopoint type found in the survey				
				if(q.getType().equals("geopoint") || q.getType().equals("geoshape") || q.getType().equals("geotrace") || q.getType().equals("geocompound")) {
					//instance.setOverallLocation(referencePath);
					instance.setOverallLocation(questionPath);
				}
				
				if(q.getType().equals("select")) {
					// Add the options to this multi choice question
					Collection <Option> optionList = null;
					if(useExternalChoices) {
						optionList = q.getValidChoices(sd);
					} else {
						optionList = q.getChoices(sd);
					}

					for(Option o : optionList) {
						
						// This value must be populated for multi select questions

						String optionColumn = q.getColumnName(false) + "__" + o.getColumnName();												
						instance.setOption(questionPath, o.getValue(), o.getValue(), o.getSeq(), optionColumn);
					}
				}
			}
		}
	}
	
	/*
	 * Get external options for the question
	 */
	private ArrayList<Option> getExternalList(
			Connection sd, 
			Connection cResults,
			int oId,
			int sId,
			Question q,
			String listName,
			List <Question> qList) {
		
		ArrayList<Option> options = new ArrayList<Option> ();
		try {
			ArrayList<KeyValueSimp> filters = getFiltersForChoiceList(listName, qList);
			
			/*
			 * Get the external file name here as it may not have been set yet
			 * This is required because we are using two methods to read the survey from the database SurveyTemplate and SurveyManager
			 */
			ManifestInfo mi = GeneralUtilityMethods.addManifestFromAppearance(q.getAppearance(false, questionPaths), null);
			String externalFilename = mi.filename;
	
			ArrayList<org.smap.sdal.model.Option> oList = GeneralUtilityMethods.getExternalChoices(
					sd,
					cResults,
					localisation,
					user,
					oId,
					survey.getId(), q.getId(), null, survey.getIdent(), "UTC", filters, externalFilename,
					true);	// Embedding external choices into the webform DOM - apply the connection filter and record cap
			
			int idx = 0;
			if(oList != null) {
				for(org.smap.sdal.model.Option o : oList) {
					Option oLegacy = new Option();
					oLegacy.setId(o.id);
					oLegacy.setValue(o.value);
					if(o.labels.size() > 0) {
						oLegacy.setLabel(o.labels.get(0).text);
					}
					oLegacy.setSeq(idx++);
					oLegacy.setExternalFile(true);
					if(o.cascade_filters != null) {
						for(String k : o.cascade_filters.keySet()) {
							oLegacy.addCascadeKeyValue(k, o.cascade_filters.get(k));
						}
						oLegacy.setCascadeFilters();
					}
					
					options.add(oLegacy);
				}
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
		return options;
	}
	
	ArrayList<KeyValueSimp> getFiltersForChoiceList(String listName, List <Question> qList) throws Exception {
		
		ArrayList<KeyValueSimp> filters = new ArrayList<KeyValueSimp>();
		for(int i= 0; i < qList.size(); i++) {
			Question q = qList.get(i);
			if(q.getListName() != null && q.getListName().toLowerCase().equals(listName)) {
					Search search = GeneralUtilityMethods.getSearchFiltersFromAppearance(q.getAppearance(false, questionPaths));
					filters.addAll(search.filters);
			}
		}
		return filters;
	}
	
}
