package org.smap.model;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.ManifestInfo;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Search;
import org.smap.sdal.model.SetValue;
import org.smap.server.entities.Form;
import org.smap.server.entities.MissingTemplateException;
import org.smap.server.entities.Option;
import org.smap.server.entities.Project;
import org.smap.server.entities.Question;
import org.smap.server.entities.Survey;
import org.smap.server.entities.Translation;
import org.smap.server.utilities.UtilityMethods;

import JdbcManagers.JdbcFormManager;
import JdbcManagers.JdbcOptionManager;
import JdbcManagers.JdbcProjectManager;
import JdbcManagers.JdbcQuestionManager;
import JdbcManagers.JdbcSurveyManager;
import JdbcManagers.JdbcTranslationManager;

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
	
	public void resetNextQuestionSeq() {
		nextQuestionSeq = 0;
	}
	
	public int getNextQuestionSeq() {
		nextQuestionSeq++;
		return nextQuestionSeq-1;
	}
	
	public void setDefaultLanguage(String v) {
		survey.setDefLang(v);
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
	
	public void setNextOptionSeq(int seq) {
		nextOptionSeq = seq;
	}
	
	public int getNextOptionSeq() {
		return nextOptionSeq;
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
	
	/*
	 * Method to create a new unnamed survey
	 * 
	 * @param surveyName the name of the survey
	 */
	public void createSurvey() {
		survey = new Survey();
	}
	
	public void setUser(String v) {
		user = v;
	}
	
	public void setBasePath(String v) {
		basePath = v;
	}
	
	public String getUser() {
		return user;
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
	 * Method to set the reference for the first form
	 * 
	 * @param formRef the reference for the form to be set
	 */
	public void setFirstFormRef(String formRef) {
		firstFormRef = formRef;
	}
	
	/*
	 * Method to set the reference for the first form used in an Xform import
	 * This is converted to "main" by smap
	 * 
	 * @param formRef the reference for the form to be set
	 */
	public void setXFormFormName(String formName) {
		xFormFormName = formName;
	}
	
	public void setFirstFormName(String formName) {
		firstFormName = formName;
	}

	/*
	 * Method to get the reference for the first form
	 * 
	 * @return String the reference for the form to be set
	 */
	public String getFirstFormRef() {
		return firstFormRef;
	}
	
	public String getFirstFormName() {
		return firstFormName;
	}
	
	public String getXFormFormName() {
		return xFormFormName;
	}

	public void addIText(String lCode, String id, String type, String value) {
	
		HashMap <String, HashMap<String, Translation>> l = translations.get(lCode);
		
		if (l == null) {	// Create new language
			l = new HashMap <String, HashMap<String, Translation>> ();
			translations.put(lCode, l);
		}
		
		HashMap <String, Translation> types = l.get(id);	// Get the types for this string identifier
		if(types == null) {	// Create a new Hashmap of translation types
			types = new HashMap<String, Translation> ();
			l.put(id, types);
		}
		
		if(value.startsWith("jr://")) {
			int idx = value.lastIndexOf('/');
			if(idx > -1) {
				value = value.substring(idx + 1);
			}
		}
		Translation t = new Translation();
		t.setLanguage(lCode);
		t.setTextId(id);
		t.setType(type);
		t.setValue(value);
		types.put(type, t);
		
	}
	
	public void addDummyTranslation(String id, String value) {
		
		Translation t = new Translation();
		t.setTextId(id);
		t.setValue(value);
		dummyTranslations.add(t);
	}
	
	public void addDefault(String ref, String value) {
		defaults.put(ref, value);
	}
	
	public String getDefault(String ref) {
		return defaults.get(ref);
	}
	
	/*
	 * Method to create a new question
	 * 
	 * @param questionRef the reference for the question to be created
	 * 
	 * @param questionName The name of the question
	 */
	public void createQuestion(String questionRef, String questionName) {
		Question q = new Question();
		q.setName(questionName);
		//q.setReference(questionRef);
		q.setPath(questionRef);		// Store path when loaded from xForm
		q.setSeq(-1);	// Default to -1 until actual sequence is known
		questions.put(questionRef, q);
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
	 * Method to delete a question
	 * 
	 * @param questionRef the reference for the question to be deleted
	 * 
	 */
	public void removeQuestion(String questionRef) {
		questions.remove(questionRef);
	}

	/*
	 * Method to create a new option
	 * 
	 * @param optionRef the reference for the option to be created
	 * @param label The label of the option
	 */
	public void createOption(String optionRef, String questionRef) {
		Option o = new Option();
		o.setQuestionRef(questionRef);
		options.put(optionRef, o);
	}
	
	/*
	 * Method to create a cascade option
	 *  These options have a different reference than other options and hence
	 *  are stored in a different hashmap, however their attributes are more or less 
	 *  the same as standard options and they are stored in an Option object
	 */
	public void createCascadeOption(String optionRef, String cascadeInstanceId) {
		Option o = new Option();
		o.setListName(cascadeInstanceId);
		cascade_options.put(optionRef, o);
	}
	
	/*
	 * Create real options for this question by copying the cascade options that match the passed in instance id
	 */
	public void createCascadeOptions(String cascadeInstanceId, String questionRef) {
		Collection<Option> c = null;
		Iterator<Option> itr = null;

		c = cascade_options.values();
		itr = c.iterator();
		while (itr.hasNext()) {
			Option o = itr.next();
			if(o.getListName().equals(cascadeInstanceId)) {
				Option oNew = new Option(o);
				oNew.setQuestionRef(questionRef);
				String optionRef = questionRef + "/" + oNew.getSeq();
				options.put(optionRef, oNew);
			}
		}
	}
	
	/*
	 * Set the value on all cascade options that match the passed in instance id and the value key
	 */
	public void setCascadeValue(String questionRef, String valueKey) {
		Collection<Option> c = null;
		Iterator<Option> itr = null;

		// Cascade option has already been copied to options list
		c = options.values();		
		itr = c.iterator();
		while (itr.hasNext()) {
			Option o = itr.next();
			if(o.getQuestionRef().equals(questionRef)) {
				HashMap<String, String> kv_pairs = o.getCascadeKeyValues();
				String value = kv_pairs.get(valueKey);
				o.setValue(value);
				kv_pairs.remove(valueKey);	// Remove this kv pair as its been identified as a value
			}
		}
	}
	
	/*
	 * Set the label_id on all cascade options that match the passed in instance id and the label key
	 */
	public void setCascadeLabel(String questionRef, String labelKey) {
		Collection<Option> c = null;
		Iterator<Option> itr = null;

		boolean itext = false;
		if(labelKey.startsWith("jr:itext")); {
			// Text id reference, set the label_id
			itext = true;
			int idx1 = labelKey.indexOf('(');
			int idx2 = labelKey.indexOf(')', idx1 + 1);
			labelKey = labelKey.substring(idx1 + 1, idx2);
		}

		c = options.values();
		itr = c.iterator();
		while (itr.hasNext()) {
			Option o = itr.next();
			if(o.getQuestionRef().equals(questionRef)) {
				HashMap<String, String> kv_pairs = o.getCascadeKeyValues();
				String label = kv_pairs.get(labelKey);

				if(itext) {
					o.setLabelId(label);
				} else {
					o.setLabel(label);
				}
				kv_pairs.remove(labelKey);	// Remove this kv pair as its been identified as a label
			}
		}
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
	public Option getCascadeOption(String optionRef) {
		return cascade_options.get(optionRef);
	}

	/*
	 * Method to print out the model for debug purposes
	 */
	public void printModel() throws Exception {

		Collection c = null;
		Iterator itr = null;

		// Survey
		System.out.println("Survey: " + survey.getDisplayName());
		System.out.println("     Id: " + survey.getId());

		// Forms
		c = forms.values();
		itr = c.iterator();
		while (itr.hasNext()) {
			Form f = (Form) itr.next();
			System.out.println("Form: " + f.getId());
			System.out.println("	Name: " + f.getName());
			System.out.println("	Parent Ref: " + f.getParentFormRef());
			if(f.getParentForm() != 0) {
				System.out.println("	Parent Form: " + f.getParentForm());
			} else {
				System.out.println("	Parent Form: None");
			}
			System.out.println("	Parent Question: " + f.getParentQuestionId());
		}

		// Questions
		c = questions.values();
		itr = c.iterator();
		while (itr.hasNext()) {
			Question q = (Question) itr.next();
			System.out.println("Question: " + q.getName());
			System.out.println("	Form: " + q.getFormRef());
			System.out.println("	Type: " + q.getType());
			System.out.println("	ReadOnly: " + q.isReadOnly());
			System.out.println("	Mandatory: " + q.isMandatory());
			System.out.println("	Default: " + q.getDefaultAnswer());
			System.out.println("	QuestionId: " + q.getQTextId());
			System.out.println("	Relevance: " + q.getRelevant(true, questionPaths, getXFormFormName()));
			System.out.println("	Question Sequence: " + q.getSeq());
		}
		
		// Cascade Options
		c = cascade_options.values();
		itr = c.iterator();
		while (itr.hasNext()) {
			Option o = (Option) itr.next();
			System.out.println("Cascade Option: ");
			System.out.println("	Instance: " + o.getListName());
			System.out.println("	Question: " + o.getQuestionRef());
			System.out.println("	Value: " + o.getValue());
			System.out.println("	Label: " + o.getLabel());
			System.out.println("	Label Id: " + o.getLabelId());
			System.out.println("	Seq: " + o.getSeq());
		}
	}

	/*
	 * Method to count total questions per form and  table columns per form
	 * Total table columns are limited by Postgres to 1,600
	 */
	private class FormDesc {
		int questions = 0;
		int options = 0;
	}
	
	public ArrayList<String> multipleGeoms() {
		HashMap <String, FormDesc> forms = new HashMap <String, FormDesc> ();
		ArrayList<String> badForms = new ArrayList<String> ();
		
		List<Question> questionList = new ArrayList<Question>(questions.values());
		for (Question q : questionList) {
			Form f = getForm(q.getFormRef());
			String fName = null;
			if(f != null) {
				fName = f.getName();
			} else {
				fName = "_topLevelForm";
			}
			FormDesc fd = forms.get(fName);
			if(fd == null) {
				fd = new FormDesc();
				forms.put(fName, fd);					
			}
			
			// Count the questions ignoring select multiple as the number of columns created by a select multiple is
			//  determined by the number of its columns
			String qType = q.getType();
			if(!qType.equals("select") && !qType.equals("end group") && !qType.equals("begin group")) {
				fd.questions++;
			}
			
		}
		
		// Each select multiple creates a column for every options, count these
		/* Select multiple questions are stored in a single column now
		List<Option> optionList = new ArrayList<Option>(options.values());
		for (Option o : optionList) {

			Question q = questions.get(o.getQuestionRef());
			Form f = getForm(q.getFormRef());
			String fName = null;
			if(f != null) {
				fName = f.getName();
			} else {
				fName = "_topLevelForm";
			}
			FormDesc fd = forms.get(fName);
			if(fd == null) {
				fd = new FormDesc();
				forms.put(fName, fd);					
			}
			if(q != null) {
				String qType = q.getType();
				if(qType != null && qType.equals("select")) {
					fd.options++;
				}
			} 
			
		}
		*/
	
		Iterator itr = forms.keySet().iterator();
		while(itr.hasNext()) {
			String k = itr.next().toString();
			FormDesc fd = forms.get(k);
			if((fd.questions + fd.options) > MAX_COLUMNS) {
				badForms.add("Error: There are " + fd.questions + " questions and " + fd.options + 
						" multiple choice options in form:" + k +
						". Each of these create a data base column (in total " + (fd.questions + fd.options) + ")" +
						" and there is a maximum of " + MAX_COLUMNS +
						" allowed. You will need to put some questions in a sub form. " +
						"Refer to http://blog.smap.com.au/limits-number-questions-per-form/ for details.");
			}

		}
		return badForms;
	}
	
	/*
	 * Method to check for duplicate names in a survey
	 */
	public ArrayList <String> duplicateNames() {
		// Hashmap of form names each of which contains a hashmap of question in that form
		//HashMap <String, HashMap<String, String>> forms = new HashMap <String, HashMap<String, String>> ();
		
		HashMap<String, String> questionMap = new HashMap<String, String> ();
		ArrayList<String> badNames = new ArrayList<String> ();
		
		List<Question> questionList = new ArrayList<Question>(questions.values());
		for (Question q : questionList) {
			String qName = q.getName();

			Form f = getForm(q.getFormRef());
			String fName = null;
			if(f != null) {
				fName = f.getName();
			} else {
				fName = "_topLevelForm";
			}

			String existingQuestion = questionMap.get(qName.trim().toLowerCase());
			if(existingQuestion != null) {
				badNames.add(qName + " in forms(" + fName + "," + existingQuestion + ")");
				log.info("Duplicate Question:" + qName + " in form:" + fName);
			} else {
				questionMap.put(qName.trim().toLowerCase(), fName);
			}
		}
		
		return badNames;
	}
	
	/*
	 * Method to check for:
	 *  	a) read only mandatory questions
	 *  	b) constraints that don't have a "."
	 */
	public ArrayList <String> manReadQuestions() throws Exception {
		
		ArrayList<String> badNames = new ArrayList<String> ();
		
		List<Question> questionList = new ArrayList<Question>(questions.values());
		for (Question q : questionList) {
			String qName = q.getName();
			String qType = q.getType();
			boolean man = q.isMandatory();
			boolean ro = q.isReadOnly() || (qType != null && qType.equals("note"));
			String relevance = q.getRelevant(true, questionPaths, getXFormFormName());
			String constraint = q.getConstraint(true, questionPaths, getXFormFormName());
		
			// Check for mandatory and readonly
			if(man && ro && relevance == null) {
				log.info("check man read: " + qName + " : " + man + " : " + ro + " : " + relevance);
				String roMsg = "Question '" + qName + "' is mandatory, read only and has nothing in the 'relevance' column - remove the 'yes' in the required column" ;
				badNames.add(roMsg);
			}
			
			// Check for constraints without dots
			if(constraint !=null && !constraint.contains(".") && !constraint.contains("false()")) {
				log.info("check constraint: " + qName + " : " + constraint);
				String roMsg = "Constraint '" + constraint + "' for question " + qName + " must refer to the answer using a '.' (dot)";
				badNames.add(roMsg);
			}
					
		}
		
		return badNames;
	}
	

	
	/*
	 * Method to write the model to the database
	 */
	public int writeDatabase() throws Exception {
		
		Connection sd = org.smap.sdal.Utilities.SDDataSource.getConnection("SurveyTemplate-Write Database");
		log.info("Set autocommit false");
		sd.setAutoCommit(false);
		
		JdbcSurveyManager sm = null;
		JdbcFormManager fm = null;
		JdbcQuestionManager qm = null;
		JdbcOptionManager om = null;
		JdbcTranslationManager tm = null;
		
		try {
			Collection c = null;
			Iterator itr = null;
	
			if(forms.values().size() == 0) {
				throw new Exception("No forms in this survey");
			}
			sm = new JdbcSurveyManager(sd, localisation);
			log.info("Persisting survey");
			sm.write(survey);
	
			/*
			 * Forms 1. Create record for each form and get its primary key
			 */
			List<Form> formList = new ArrayList<Form>(forms.values());
			fm = new JdbcFormManager(sd);
			for(Form f : formList) {
				f.setSurveyId(survey.getId());
				fm.write(f);
			}
			
			/*
			 * Questions.
			 */
			List<Question> questionList = new ArrayList<Question>(questions.values());
			
			/*
			 * Hack
			 * Add four preload questions that arn't currently supported by odkBuild editor
			 * These questions will be added to the top level form as the form_ref is null
			 */
			boolean alreadyHas_device = false;
			boolean alreadyHas_start = false;
			boolean alreadyHas_end = false;
			boolean alreadyHas_instanceid = false;
			boolean alreadyHas_instancename = false;
			boolean alreadyHas_task_key = false;
			for (Question q : questionList) {
				if(q.getSourceParam() != null) {
					if(q.getSourceParam().equals("deviceid")) {
						alreadyHas_device = true;
					} else if(q.getSourceParam().equals("start")) {
						alreadyHas_start = true;
					} else if(q.getSourceParam().equals("end")) {
						alreadyHas_end = true;
					} 
				}
	
				if(q.getName().trim().toLowerCase().equals("_instanceid") ||
						q.getName().trim().equals("instanceID")) {
						alreadyHas_instanceid = true;
				} else if(q.getName().equals("_task_key")) {
						alreadyHas_task_key = true;
				} else if(q.getName().toLowerCase().equals("instancename")) {
						alreadyHas_instancename = true;
						q.setSource("user");	// Always add instance name as a column in the results database even if not initially set in client
				}
	
			}
	
			if(!alreadyHas_device) {
				Question q = new Question();	// Device id
				q.setName("_device");
				q.setSeq(-3);
				q.setVisible(false);
				q.setSource("property");
				q.setSourceParam("deviceid");
				questionList.add(q);
			}
			
			if(!alreadyHas_start) {
				Question q = new Question();	// Start time
				q.setName("_start");
				q.setSeq(-2);
				q.setVisible(false);
				q.setSource("timestamp");
				q.setSourceParam("start");
				q.setType("dateTime");
				questionList.add(q);
			}
			
			if(!alreadyHas_end) {
				Question q = new Question();	// End time
				q.setName("_end");
				q.setSeq(-1);
				q.setVisible(false);
				q.setSource("timestamp");
				q.setSourceParam("end");
				q.setType("dateTime");
				questionList.add(q);
			}
			
			if(!alreadyHas_instanceid) {
				Question q = new Question();	// Instance Id
				q.setName("_instanceid");
				q.setSeq(-1);
				q.setVisible(false);
				q.setSource("user");
				q.setCalculate("concat('uuid:', uuid())");
				q.setReadOnly(true);
				q.setType("string");
				questionList.add(q);
			}
			
			if(!alreadyHas_instancename) {
				Question q = new Question();	// Instance Name
				q.setName("instanceName");
				q.setPath("/main/meta/instanceName");	
				q.setSeq(-1);
				q.setVisible(false);
				q.setSource("user");
				q.setCalculate("");
				q.setReadOnly(true);
				q.setType("string");
				questionList.add(q);
			}
			
			if(!alreadyHas_task_key) {
				Question q = new Question();	// Task Key
				q.setName("_task_key");
				q.setSeq(-1);
				q.setVisible(false);
				q.setSource("tasks");
				q.setType("string");
				questionList.add(q);
			}
			
			/*
			 * Set the sequence number of any questions that have the default sequence "-1" but
			 * need to be placed within a non repeat group
			 */
			for (Question q : questionList) {
				if(q.getSeq() == -1) {
					String ref = q.getPath();
					//String ref = questionPaths.get(q.getName());
					String group = UtilityMethods.getGroupFromPath(ref);
					if(group != null) {
						for(Question q2 : questionList) {
							
							//String q2Path = questionPaths.get(q2.getName());
							String q2Path = q2.getPath();
							if(q2Path != null) {
								if(q2Path.endsWith(group)) {
									q.setSeq(q2.getSeq() + 1);
		
								}
							}
						}
					}
				}
			}
			
			/*
			 * Sort the list by sequence
			 */
			java.util.Collections.sort(questionList, new Comparator<Question>() {
				@Override
				public int compare(Question object1, Question object2) {
					if (object1.getSeq() < object2.getSeq())
						return -1;
					else if (object1.getSeq() == object2.getSeq())
						return 0;
					else
						return 1;
				}
			});
			
			qm  = new JdbcQuestionManager(sd);
			for (Question q : questionList) {
				Form f = getForm(q.getFormRef());
				if(f == null) {
					
					/*
					 * Try and get the formRef from the question path
					 */
					String qPath = q.getPath();
					if(qPath != null) {
						String formRef = getFormFromQuestionPath(qPath);
						f = getForm(formRef);
					}
					
					if(f == null) {
						// Still no form then allocate this question to the top level form
						f = getForm(firstFormRef);
						//q.setPath(f.getPath() + "/" + q.getName());   path change - no longer save path
					}
	
	
				}
	
				if(f != null) {
					q.setFormId(f.getId());
					q.setSeq(f.qSeq++);
					q.setListId(sd, survey.getId());
					if(!q.isRepeatCount()) {
						qm.write(q, getXFormFormName());
					}
				} 			
			}
	
			/*
			 * Forms 2. Update the form record with parent form and question keys
			 */
			for(Form f : formList) {
				if (f.getParentFormRef() != null) {
					f.setParentForm(forms.get(f.getParentFormRef()).getId());
					f.setParentQuestionId(questions.get(f.getParentQuestionRef()).getId());
				}
				if(f.getRepeatsRef() != null) {		// Set the repeat count from the dummy calculation question
					String rRef = f.getRepeatsRef().trim();
					Question qRef = questions.get(rRef);
					f.setRepeats(qRef.getCalculate(false, null, getXFormFormName()));
				}
				fm.update(f);
			}
	
			/*
			 * Persist the options
			 */
			
			// Sort options by sequence number
			List<Option> optionList = new ArrayList<Option>(options.values());
			java.util.Collections.sort(optionList, new Comparator<Option>() {
				@Override
				public int compare(Option object1, Option object2) {
					if (object1.getSeq() < object2.getSeq())
						return -1;
					else if (object1.getSeq() == object2.getSeq())
						return 0;
					else
						return 1;
				}
			});
			
			om = new JdbcOptionManager(sd);
			for (Option o : optionList) {
				Question q = getQuestion(o.getQuestionRef()); // Get the question id
				o.setListId(q.getListId());
				o.setSeq(q.oSeq++);
				o.setCascadeFilters();	// Set the filter value based on the key value pairs
				om.write(o);
			}
			
			// Write the translation objects
			Set<String> langs = translations.keySet();
			String[] languages = (String[]) langs.toArray(new String[0]);
			int sId = survey.getId();
			tm = new JdbcTranslationManager(sd);
			if(languages.length > 0) {
				for(int langIndex = 0; langIndex < languages.length; langIndex++) {
					HashMap<?, HashMap<String, Translation>> aLanguageTranslation = translations.get(languages[langIndex]);	// A single language
					Collection<HashMap<String, Translation>> l = aLanguageTranslation.values();
					
					tm.persistBatch(sId, l);
					//tPersist.persistBatch(l, survey);
					
					for(int i = 0; i < dummyTranslations.size(); i++) {
						Translation trans = dummyTranslations.elementAt(i);
						tm.write(sId, languages[langIndex], trans.getTextId(), "none", trans.getValue());
					}
				}
			} else {
				// No languages specified, just create the dummy elements with a language of "default"
				for(int i = 0; i < dummyTranslations.size(); i++) {
					Translation trans = dummyTranslations.elementAt(i);
					tm.write(sId, "default", trans.getTextId(), "none", trans.getValue());
					
				}
			}
			
			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);
			// Update the form dependencies so that when new results are received it is simple to identify the impacted forms			
			GeneralUtilityMethods.updateFormDependencies(sd, sId);
			
			String msg = localisation.getString("log_sc_xml");
			msg = msg.replace("%s1", survey.getDisplayName()).replace("%s2", survey.getIdent());
			lm.writeLog(sd, sId, user, LogManager.CREATE, msg, 0, null);
			
			sd.commit();
		} catch (Exception e) {
			try{sd.rollback();} catch(Exception ex) {}
			throw e;
		} finally {
			
			log.info("Set autocommit true");
			sd.setAutoCommit(true);
			if(sm != null) {sm.close();};
			if(fm != null) {fm.close();};
			if(qm != null) {qm.close();};
			if(om != null) {om.close();};
			if(tm != null) {tm.close();};
			
			org.smap.sdal.Utilities.SDDataSource.closeConnection("SurveyTemplate-Write Database", sd);
		}
		
		return survey.getId();

	}

	/*
	 * Get the form name for the question path by finding the longest form that
	 *  can be the base of the question path
	 *  The path to the question cannot be used as this may include a group
	 */
	private String getFormFromQuestionPath(String qPath) {

		List<String> kList = new ArrayList<String>(forms.keySet());
		String longestPath = null;
		int maxLength = -1;
		for(String formPath : kList) {
			String exPath;
			if(formPath.endsWith("/")) {
				exPath = formPath;
			} else {
				exPath= formPath + "/";    // Make sure path does not match part of a question name
			}
			if(qPath.startsWith(exPath)) {
				int l = formPath.length();
				if(l > maxLength) {
					maxLength = l;
					longestPath = formPath;
				}
			}
		}

		return longestPath;
		
		/*
		String formPath = null;
		int idx = qPath.lastIndexOf('/');
		if(idx > 0) {
			formPath = qPath.substring(0, idx);
		}
		return formPath;
		*/
	}
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
				log.info("Error: Survey Template not found: " + surveyIdent);
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
				log.info("Error: Survey Template not found: " + surveyId);
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
					log.info("Form not found for f_id = " + f_id);
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
	 * Method to check to see whether the cascade option has already been loaded
	 * Compare against the value as pyxform can create two sets of options one
	 *  in an instance and the other not which differ in label id's but are otherwise duplicates
	 */
	public String cascadeOptionLoaded(String listName, String labelId, String value) {
		String existingRef = null;
		
		Set<String> keys = cascade_options.keySet();
		for (String ref : keys) {
			Option o = cascade_options.get(ref);
			if(o.getListName().equals(listName) && o.getValue().equals(value)) {
				existingRef = ref;
				break;
			}
		}
		return existingRef;
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
	 * Add a survey level manifest such as a csv file from an appearance attribute
	 */
	public void addManifestFromAppearance(String appearance) {
		
		ManifestInfo mi = GeneralUtilityMethods.addManifestFromAppearance(appearance, survey.getManifest());
		survey.setManifest(mi.manifest);
	}
	
	/*
	 * Add a survey level manifest such as a csv file from a calculate attribute
	 */
	public void addManifestFromCalculate(String calculate) {
		
		ManifestInfo mi = GeneralUtilityMethods.addManifestFromCalculate(calculate, survey.getManifest());
		survey.setManifest(mi.manifest);
			
	}
	
	/*
	 * Add a survey level name from the calculate (if its a instanceName question)
	 */
	public void addSurveyInstanceNameFromCalculate(String calculate, String questionRef) {
		
		if(questionRef.toLowerCase().trim().equals("/main/meta/instancename")) {
			survey.setInstanceName(calculate);
		}
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
				log.info("+++++++ setting for: " + questionPath);
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
					survey.getId(), q.getId(), null, survey.getIdent(), "UTC", filters, externalFilename);
			
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
