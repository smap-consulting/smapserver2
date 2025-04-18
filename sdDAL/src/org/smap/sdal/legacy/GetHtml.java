package org.smap.sdal.legacy;

import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.constants.SmapQuestionTypes;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.NodesetFormDetails;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.SetValue;
import org.smap.sdal.model.Survey;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/*
 * Return an HTML5 section built from a survey defined in the database
 * 
 * Translations are stored as a hierarchy of values from language down to element type (as below)
 *   languages (HashMap)-> translation id's (HashMap)-> translation types (HashMap) -> translation element (String)
 */
public class GetHtml {

	Survey survey = null;
	int languageIndex = 0;
	HashMap<String, String> paths = new HashMap<>(); // Keep paths out of the survey model and instead store them here
	HashMap<String, String> formRefs = new HashMap<>();		// Mapping between question name and form ref used for nodesets that reference repeats
	Document outputDoc = null;
	private boolean gInTableList = false;
	private HashMap<String, Integer> gRecordCounts = null;
	private String gUrlPrefix;

	private static Logger log = Logger.getLogger(GetHtml.class.getName());
	
	private HtmlSanitise sanitise = new HtmlSanitise();
	
	private static  String FILE_MIME="text/plain,application/pdf,application/vnd.ms-excel,application/msword,text/richtext,application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/zip,application/x-zip,application/x-zip-compressed" ;

	private ResourceBundle localisation;
	
	public GetHtml(ResourceBundle l) {
		localisation = l;
	}
	/*
	 * Get the Html as a string
	 */
	public String get(HttpServletRequest request, int sId, boolean superUser, String userIdent, 
			HashMap<String, Integer> recordCounts, 
			boolean temporaryUser,
			boolean forDevice) throws SQLException, Exception {
		
		gRecordCounts = recordCounts;
		gUrlPrefix = GeneralUtilityMethods.getUrlPrefix(request);
		
		String response = null;
		String connectionString = "Get Html";

		// Get the base path
		String basePath = GeneralUtilityMethods.getBasePath(request);
		Connection sd = SDDataSource.getConnection(connectionString);
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		SurveyManager sm = new SurveyManager(localisation, "UTC");

		try {

			survey = sm.getById(sd, cResults, userIdent, 
					temporaryUser, 	
					sId, true, basePath, null, false, false, true, false,
					false, "real", false, false, superUser, null,
					false,		// Do not include child surveys - presumably never required for a web form
					false,		// launched only
					false		// Don't merge set value into default values
					);

			if(survey == null) {
				throw new Exception("Survey not available - Check to see if it has been deleted or Security Roles applied");
			}
			
			log.info("Getting survey as Html-------------------------------");
			// Create a new XML Document
			DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
			DocumentBuilder b = dbf.newDocumentBuilder();
			outputDoc = b.newDocument();

			Writer outWriter = new StringWriter();
			Result outStream = new StreamResult(outWriter);

			Element parent;
			parent = populateRoot();
			createForm(sd, parent, true);

			// Write the survey to a string and return it to the calling program
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transformer.setOutputProperty(OutputKeys.METHOD, "html");

			DOMSource source = new DOMSource(outputDoc);
			transformer.transform(source, outStream);

			response = outWriter.toString();

		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;

	}

	/*
	 * Create the root element
	 * 
	 * @param outputDoc
	 */
	public Element populateRoot() {

		Element rootElement = outputDoc.createElement("root");
		outputDoc.appendChild(rootElement);

		return rootElement;
	}

	public void createForm(Connection sd, Element parent, boolean useNodesets) throws Exception {

		Element bodyElement = outputDoc.createElement("form");
		bodyElement.setAttribute("novalidate", "novalidate");
		bodyElement.setAttribute("autocomplete", "off");
		bodyElement.setAttribute("class",
				"or clearfix" + (survey.surveyData.surveyClass != null ? (" " + survey.surveyData.surveyClass) : ""));
		bodyElement.setAttribute("dir", "ltr");
		bodyElement.setAttribute("id", survey.getIdent());

		populateForm(sd, bodyElement);
		parent.appendChild(bodyElement);
	}

	private void populateForm(Connection sd, Element parent) throws Exception {

		// logo
		Element bodyElement = outputDoc.createElement("section");
		bodyElement.setAttribute("class", "form-logo");
		bodyElement.setTextContent(" "); // Set a dummy value a enketo does not understand empty sections
		parent.appendChild(bodyElement);

		// title
		bodyElement = outputDoc.createElement("h3");
		bodyElement.setAttribute("id", "form-title");
		bodyElement.setAttribute("dir", "auto");
		bodyElement.setTextContent(sanitise.sanitiseHtml(survey.getDisplayName()));
		parent.appendChild(bodyElement);

		// Languages
		bodyElement = outputDoc.createElement("select");
		bodyElement.setAttribute("id", "form-languages");
		if (survey.surveyData.languages == null || survey.surveyData.languages.size() <= 1) {
			bodyElement.setAttribute("style", "display:none;");
		}
		bodyElement.setAttribute("data-default-lang", survey.surveyData.def_lang);
		populateLanguageChoices(bodyElement);
		parent.appendChild(bodyElement);

		/*
		 * Add preloads to the questionPaths hashmap so they can be referenced
		 */
		ArrayList<MetaItem> preloads = survey.surveyData.meta;
		for(MetaItem mi : preloads) {
			if(mi.isPreload) {
				paths.put(mi.name, "/main/" + mi.name);
			}
		}
		
		// Questions
		for (Form form : survey.surveyData.forms) {
			if (form.parentform == 0) { // Start with top level form
				log.info("Adding questions from: " + form.name);
				addPaths(form, "/");
				processQuestions(sd, parent, form);
				form.questions.addAll(form.generatedCalculations);	// Add calculations generated during processing of the questions
				processPreloads(parent, form);
				processCalculations(parent, form);
				break;
			}
		}

	}

	private void populateLanguageChoices(Element parent) {
		Element bodyElement = null;
		int idx = 0;
		for (Language lang : survey.surveyData.languages) {
			bodyElement = outputDoc.createElement("option");
			bodyElement.setAttribute("value", lang.name);
			
			boolean rtl = lang.rtl;
			// For backward compatability if the language code is null guess the rtl from the language name
			if(lang.code == null) {
				rtl = GeneralUtilityMethods.isRtl(lang.name);
			}
			if(rtl) {
				bodyElement.setAttribute("data-dir", "rtl");
			}
			bodyElement.setTextContent(sanitise.sanitiseHtml(GeneralUtilityMethods.getLanguageName(lang.name)));
			parent.appendChild(bodyElement);

			// Save the index of the default language
			if (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang)) {
				languageIndex = idx;
			}
			idx++;
		}
	}

	/*
	 * Associate question names with their paths
	 */
	private void addPaths(Form form, String pathStem) {

		Stack<String> pathStack = new Stack<>(); // Store the paths as we go in and out of groups

		pathStem = pathStem + form.name + "/";
		String formPath = pathStem;

		for (Question q : form.questions) {

			paths.put(getRefName(q.name, form), pathStem + q.name); // Save the path
			formRefs.put(q.name, formPath);

			if (q.type.equals("end group")) {

				pathStem = pathStack.pop();

			} else if (q.type.equals("begin group")) {

				pathStack.push(pathStem);
				pathStem = pathStem + q.name + "/";

			} else if (q.type.equals("begin repeat")) {

				for (Form subForm : survey.surveyData.forms) {
					if (subForm.parentQuestion == q.id) { // continue with next form
						addPaths(subForm, pathStem);
						break;
					}
				}

			}
		}

	}

	/*
	 * Process the main block of questions Skip over: - preloads - meta group
	 */
	private void processQuestions(Connection sd, Element parent, Form form) throws Exception {

		Element bodyElement = null;
		Element currentParent = parent;
		Stack<Element> elementStack = new Stack<>(); // Store the elements for non repeat groups

		for (Question q : form.questions) {

			// Append _pull to pulldata sources in calculations
			q.calculation = processPulldataSuffix(q.calculation);
			
			// Add a marker if this is a table list group
			if (q.type.equals("begin group")) {
				if (q.isTableList) {
					gInTableList = true;
				} else {
					String appearance = q.appearance;
					if (appearance != null && appearance.contains("table-list")) {
						q.isTableList = true;
						gInTableList = true;
						q.appearance = appearance.replace("table-list", "field-list");
					}
				}
			} else if (q.type.equals("end group")) {
				gInTableList = false;
			}

			if (!q.inMeta && !q.name.equals("meta_groupEnd") && !q.isPreload() 
					&& !q.type.equals("calculate")		// Calculates are processed separately from questions for webforms
					&& !q.type.equals("server_calculate")
					&& !q.type.equals("conversation")
					&& !q.type.equals("chart")) {	// Charts not supported in webforms
				
				if(q.type.equals("pdf_field") && q.source == null) {
					continue;
				}
				
				if (q.type.equals("end group")) {

					currentParent = elementStack.pop();

				} else if (q.type.equals("begin group")) {

					elementStack.push(currentParent);
					currentParent = addGroupWrapper(currentParent, q, false, form);

					// Add a dummy instance element for the table list labels if this is a table
					// list question
					if (q.isTableList) {

						// Get one of the select questions to provide the labels
						Question qLabel = getTableListLabelQuestion(q, form);

						bodyElement = outputDoc.createElement("fieldset");
						bodyElement.setAttribute("class", "question simple-select or-appearance-label");
						Element extraFieldsetElement = outputDoc.createElement("fieldset");
						bodyElement.appendChild(extraFieldsetElement);

						addSelectContents(sd, extraFieldsetElement, qLabel, form, true, false);
						currentParent.appendChild(bodyElement);
					}

				} else if (q.type.equals("begin repeat")) {

					elementStack.push(currentParent);
					currentParent = addGroupWrapper(currentParent, q, true, form);

					addRepeat(sd, currentParent, q, form);

					// repeat into
					Element repeatInfo = outputDoc.createElement("div");
					repeatInfo.setAttribute("class", "or-repeat-info");
					repeatInfo.setAttribute("data-name", paths.get(getRefName(q.name, form)));
					if (q.calculation != null && q.calculation.trim().length() > 0) {
						repeatInfo.setAttribute("data-repeat-count", paths.get(getRefName(q.name, form)) + "_count");
						if(q.appearance == null || !q.appearance.contains("extendable")) {
							repeatInfo.setAttribute("data-repeat-fixed", "true()");		// Do not add if initial extend set by repeat count but can be extended
						}
					} 
					currentParent.appendChild(repeatInfo);

					// Exit the group
					currentParent = elementStack.pop();

				} else {
					processStandardQuestion(sd, form, q, currentParent, false);
				}
			}
		}

	}
	
	/*
	 * Convert a standard question (not a group or repeat) into HTML
	 */
	private void processStandardQuestion(Connection sd, Form form, Question q, Element currentParent, boolean hideLabels) throws Exception {
		
		Element bodyElement;
		
		if (q.isSelect()) {

			if (gInTableList) {
				if (q.appearance == null) {
					q.appearance = "";
				}
				if (!q.appearance.contains("field-list")) {
					q.appearance = q.appearance.trim();
					if (q.appearance.length() > 0) {
						q.appearance += " ";
					}
					q.appearance += "list-nolabel";
				}
			}

			/*
			 * Create fieldSet or Label depending on the attributes
			 */
			if (minSelect(q.appearance) && !q.type.equals("rank")) {
				bodyElement = outputDoc.createElement("label");
				setQuestionClass(q, bodyElement);

				if (hasNodeset(sd, q, form)) {
					if(q.appearance.contains("minimal") && !q.appearance.contains("autocomplete") || q.type.equals("select")) {
						addMinimalContents(sd, bodyElement, q, form, false, hideLabels);		// Not auto complete
					} else {
						addMinimalContents(sd, bodyElement, q, form, true, hideLabels);		// For autocomplete
					}
				} else {
					addMinimalSelectContents(bodyElement, q, form, hideLabels);
				}
				currentParent.appendChild(bodyElement);

			} else {
				bodyElement = outputDoc.createElement("fieldset");
				currentParent.appendChild(bodyElement);
				
				setQuestionClass(q, bodyElement);

				Element extraFieldsetElement = outputDoc.createElement("fieldset");
				bodyElement.appendChild(extraFieldsetElement);
				
				addSelectContents(sd, extraFieldsetElement, q, form, false, hideLabels);
				
				// Add constraint message at end to the outer fieldset
				addConstraintMsg(q.constraint_msg, null, bodyElement, 0);
				
				if(q.appearance.contains("literacy") && q.flash > 0) {
					bodyElement.setAttribute("data-flash", String.valueOf(q.flash));
				}

			}

		} else if(q.type.equals("acknowledge") || q.type.equals("trigger")) {

			// trigger questions
			bodyElement = outputDoc.createElement("fieldset");
			currentParent.appendChild(bodyElement);
			setQuestionClass(q, bodyElement);
			
			// inner fieldSet
			Element fieldset = outputDoc.createElement("fieldset");
			bodyElement.appendChild(fieldset);
			
			// legend
			Element legendElement = outputDoc.createElement("legend");
			fieldset.appendChild(legendElement);
			
			// Label
			if(!hideLabels) {
				addLabels(legendElement, q, form);
			}
			
			// control
			Element controlElement = outputDoc.createElement("div");
			fieldset.appendChild(controlElement);
			controlElement.setAttribute("class", "option-wrapper");
			
			// Control label
			Element controlLabel = outputDoc.createElement("label");
			controlElement.appendChild(controlLabel);
			
			// input
			//addLabelContents(controlLabel, q, form);
			Element input = outputDoc.createElement("input");
			controlLabel.appendChild(input);
			input.setAttribute("value", "OK");
			input.setAttribute("type", "radio");
			input.setAttribute("name", paths.get(getRefName(q.name, form)));
			input.setAttribute("data-name", paths.get(getRefName(q.name, form)));
			input.setAttribute("data-type-xml", "string");
			if (q.readonly) {
				input.setAttribute("readonly", "readonly");
			}
			if (q.isRequired()) {
				input.setAttribute("data-required", UtilityMethods.getCompoundExpression(q.required, 
						q.required_expression,
						form.id, q.name, paths));
			}
			// relevant
			if (q.relevant != null && q.relevant.trim().length() > 0) {
				input.setAttribute("data-relevant",
						UtilityMethods.convertAllxlsNames(q.relevant, false, paths, form.id, true, q.name, false));
			}
			if (q.constraint != null && q.constraint.trim().length() > 0) {
				input.setAttribute("data-constraint",
						UtilityMethods.convertAllxlsNames(q.constraint, false, paths, form.id, true, q.name, false));
			}
	
			// Dynamic Default
			if (q.calculation != null && q.calculation.trim().length() > 0) {
				bodyElement.setAttribute("data-calculate", UtilityMethods.convertAllxlsNames(q.calculation, false, paths, form.id, true, q.name, false));
			}
			
			// option label
			Element option_label = outputDoc.createElement("span");
			controlLabel.appendChild(option_label);
			option_label.setAttribute("class", "option-label active");
			option_label.setTextContent("OK");
			
		} else {

			// Non select question
	
			
			/*
			 * If this is an image question with a get_image() function in its calculate then add a calculation
			 * TODO
			 */
			if(q.type.equals(SmapQuestionTypes.IMAGE) && q.calculation != null && q.calculation.contains("get_image(")) {
				// Add calculation
				Question qc = new Question();
				qc.name = "_gic_" + q.name;
				qc.type = SmapQuestionTypes.CALCULATE;
				qc.labels = q.labels;
				qc.calculation = getImageCalculation(q);	// removes get_image from q
				
				form.generatedCalculations.add(qc);	// Add the generated calculation question to the form for processing
				String formPath = paths.get(form.name);
				paths.put(getRefName(qc.name, form), formPath + q.name); // Save the path
				formRefs.put(q.name, formPath);
			}
			
			/*
			 * Add the question
			 */
			bodyElement = outputDoc.createElement("label");
			if(q.type.equals(SmapQuestionTypes.PARENT_FORM) || q.type.equals(SmapQuestionTypes.CHILD_FORM)) {
				bodyElement.setAttribute("title", survey.getDisplayName());
			}
			setQuestionClass(q, bodyElement);
			addLabelContents(bodyElement, q, form, hideLabels);
			currentParent.appendChild(bodyElement);

		}

	}

	/*
	 * Get the calculation required for a generated calculate to get an image
	 */
	private String getImageCalculation(Question q) {
		String newCalc = q.calculation.replaceAll("get_image\\(", "get_media\\(");
		StringBuilder cb = new StringBuilder("");
		int idx = newCalc.indexOf("get_media", 0);
		while(idx > 0) {
			idx = newCalc.indexOf(")",idx);
			if(idx > 0) {
				cb.append(newCalc.substring(0, idx));
				cb.append(",'/main/q3'");
				newCalc = newCalc.substring(idx);
				idx = newCalc.indexOf("get_media",0);
			} else {
				cb.append(newCalc);
			}
		}
		cb.append(newCalc);
		
		q.calculation = null;
		System.out.println(cb.toString());
		return cb.toString();
		// "if(string-length(${q1}) > 0, get_media(${q1}, '/main/q3'), '')";
	}
	
	/*
	 * Question classes
	 */
	private void setQuestionClass(Question q, Element elem) {

		StringBuffer classVal = new StringBuffer("");

		if (q.type.equals("note")) {
			classVal.append("note question non-select");
		} else if (q.type.equals("begin group") || q.type.equals("begin repeat")) {
			if (hasLabel(q)) {
				classVal.append("or-group");
			} else {
				classVal.append("or-group-data");
			}
		} else if (q.type.equals("trigger") || q.type.equals("acknowledge")) {
			classVal.append("question single-select trigger");

		} else {
			classVal.append("question");
			if (!q.isSelect()) {
				classVal.append(" non-select");
			} else if(q.type.equals("rank") || (!q.appearance.contains("likert") && !minSelect(q.appearance) && !q.appearance.contains("compact"))) {
				classVal.append(" simple-select");
			}
		}
		
		if (q.type.equals(SmapQuestionTypes.PARENT_FORM) || q.type.equals(SmapQuestionTypes.CHILD_FORM)) {
			classVal.append(" or-appearance-form");
		}

		// Mark the question as a branch if it has a relevance
		if (q.relevant != null && q.relevant.trim().length() > 0) {
			classVal.append(" or-branch pre-init");
		}

		// Add appearances
		if(q.appearance != null) {
			
			String[] appList = q.appearance.split(" ");
			boolean inSearch = false;
			int brackets = 0;
			
			boolean horizontal = false;
			for (int i = 0; i < appList.length; i++) {
				if (appList[i] != null && appList[i].trim().length() > 0) {
					
					String appItem = appList[i].toLowerCase().trim();
					
					if(appItem.equals("horizontal")) {
						horizontal = true;
					}
					
					if(appItem.startsWith("search(") || appItem.startsWith("lookup_choices(")) {
						inSearch = true;
						brackets = 0;
					}
					
					if(!inSearch) {
						classVal.append(" or-appearance-");
						classVal.append(appItem);
					} else {
						brackets = brackets + countChars(appItem, '(') - countChars(appItem, ')');
						if(brackets == 0) {
							inSearch = false;
						}
					}
				}
			}
			if(horizontal) {
				classVal.append(" or-appearance-columns");
			}
		}
		elem.setAttribute("class", classVal.toString());
	}

	/*
	 * Process the main block of questions Preloads are only in the top level form
	 */
	private void processPreloads(Element parent, Form form) {

		Element preloadLabel = null;
		Element preloadInput = null;
		Element bodyElement = outputDoc.createElement("fieldset");
		bodyElement.setAttribute("style", "display:none;");
		bodyElement.setAttribute("id", "or-preload-items");
		
		if(survey.surveyData.meta != null) {
			for(MetaItem mi : survey.surveyData.meta) {
				if(mi.isPreload) {
					preloadLabel = outputDoc.createElement("label");
					preloadLabel.setAttribute("class", "calculation non-select");
					bodyElement.appendChild(preloadLabel);

					preloadInput = outputDoc.createElement("input");
					preloadInput.setAttribute("type", "hidden");
					preloadInput.setAttribute("name", "/main/" + mi.name);
					preloadInput.setAttribute("data-preload", mi.dataType);
					preloadInput.setAttribute("data-preload-params", mi.sourceParam);
					preloadInput.setAttribute("data-type-xml", mi.type);
					preloadLabel.appendChild(preloadInput);
				}
			}
		}
		// Legacy preloads in questions
		for (Question q : form.questions) {

			if (q.isPreload() && !q.inMeta) {
				preloadLabel = outputDoc.createElement("label");
				preloadLabel.setAttribute("class", "calculation non-select");
				bodyElement.appendChild(preloadLabel);

				preloadInput = outputDoc.createElement("input");
				preloadInput.setAttribute("type", "hidden");
				preloadInput.setAttribute("name", paths.get(getRefName(q.name, form)));
				preloadInput.setAttribute("data-preload", q.source);
				preloadInput.setAttribute("data-preload-params", q.source_param);
				preloadInput.setAttribute("data-type-xml", getXmlType(q));
				preloadLabel.appendChild(preloadInput);
			}
		}
		parent.appendChild(bodyElement);

	}

	/*
	 * Process Calculations
	 */
	private void processCalculations(Element parent, Form form) throws Exception {

		Element bodyElement = outputDoc.createElement("fieldset");
		bodyElement.setAttribute("style", "display:none;");
		bodyElement.setAttribute("id", "or-calculated-items");

		if(form.parentform == 0) {
			// instanceID
			Element calculationLabel = outputDoc.createElement("label");
			calculationLabel.setAttribute("class", "calculation non-select");
			bodyElement.appendChild(calculationLabel);
			Element calculationInput = outputDoc.createElement("input");
			calculationInput.setAttribute("type", "hidden");
			calculationInput.setAttribute("name", "/main/meta/instanceID");			
			calculationInput.setAttribute("data-type-xml", "string");
			calculationLabel.appendChild(calculationInput);
			
			// instanceName
			if(survey.surveyData.instanceNameDefn != null && survey.surveyData.instanceNameDefn.trim().length() > 0) { 
				calculationLabel = outputDoc.createElement("label");
				calculationLabel.setAttribute("class", "calculation non-select");
				bodyElement.appendChild(calculationLabel);
				calculationInput = outputDoc.createElement("input");
				calculationInput.setAttribute("type", "hidden");
				calculationInput.setAttribute("name", "/main/meta/instanceName");			
				calculationInput.setAttribute("data-type-xml", "string");
				calculationInput.setAttribute("data-calculate",
						" " + UtilityMethods.convertAllxlsNames(survey.surveyData.instanceNameDefn, false, paths, form.id, true, "instanceName", false) + " ");
				calculationLabel.appendChild(calculationInput);
			}
		}
		
		processCalculationQuestions(bodyElement, form);
		
		parent.appendChild(bodyElement);
		
	}
	
	/*
	 * Process the questions in a form to get calculation elements
	 */
	private void processCalculationQuestions(Element bodyElement, Form form) throws DOMException, Exception {
		
		Element calculationLabel = null;
		Element calculationInput = null;
		
		for (Question q : form.questions) {
			
			String calculation = null;
			if (q.name.equals("instanceName")) {
				continue;		// Legacy instance name included as a question
				//calculation = survey.instanceNameDefn;
			} 
			
			if (q.type.equals("begin repeat")) {
				for (Form subForm : survey.surveyData.forms) {
					if (subForm.parentQuestion == q.id) { // continue with next form
						if(subForm.reference) {
							calculation = "0";
							if(gRecordCounts != null) {
								Integer c = gRecordCounts.get(subForm.name);
								if(c != null) {
									calculation = String.valueOf(c);
								}
							}
						} else {
							calculation = q.calculation;
						}
						break;
					}
				}
			} else {
				calculation = q.calculation;
			}

			if (calculation != null && calculation.trim().length() > 0 &&(q.type.equals("calculate") || q.type.equals("begin repeat"))) {

				calculationLabel = outputDoc.createElement("label");
				calculationLabel.setAttribute("class", "calculation non-select");
				bodyElement.appendChild(calculationLabel);

				calculationInput = outputDoc.createElement("input");
				calculationInput.setAttribute("type", "hidden");

				if (q.type.equals("begin repeat")) {
					calculationInput.setAttribute("name", paths.get(getRefName(q.name, form)) + "_count");
				} else {
					calculationInput.setAttribute("name", paths.get(getRefName(q.name, form)));
				}

				calculationInput.setAttribute("data-calculate",
						" " + UtilityMethods.convertAllxlsNames(calculation, false, paths, form.id, true, q.name, false) + " ");

				calculationInput.setAttribute("data-type-xml", "string"); // Always use string for calculate
				calculationLabel.appendChild(calculationInput);
			}
			
			/*
			 * Add an additional calculation for lookup_choices
			 */
			if(q.appearance != null && q.appearance.contains("lookup_choices(")) {
				calculationLabel = outputDoc.createElement("label");
				calculationLabel.setAttribute("class", "calculation non-select");
				bodyElement.appendChild(calculationLabel);

				calculationInput = outputDoc.createElement("input");
				calculationInput.setAttribute("type", "hidden");

				calculationInput.setAttribute("name", paths.get(getRefName(q.name, form)) + "__dynamic"
						+ (q.type.equals("select") ? "_mult" : ""));
				String fn = GeneralUtilityMethods.extractFn("lookup_choices", q.appearance);
				
				if(fn != null && fn.length() > 1) {
					// Add additional parameters required for webforms
					fn = fn.trim();
					fn = fn.substring(0, fn.length() - 1);
					fn += ",'" + q.name + "')";
					fn = GeneralUtilityMethods.escapeSingleQuotesInFn(fn);
				
					calculationInput.setAttribute("data-calculate"," " + UtilityMethods.convertAllxlsNames(fn, false, paths, form.id, true, q.name, false) + " ");

					calculationInput.setAttribute("data-type-xml", "string"); // Always use string for calculate
					calculationLabel.appendChild(calculationInput);
				}
			}
			
			// Process calculations in repeats
			if (q.type.equals("begin repeat")) {
				// Process sub form
				for (Form subForm : survey.surveyData.forms) {
					if (subForm.parentQuestion == q.id) { // continue with next form
						processCalculationQuestions(bodyElement, subForm);
						break;
					}
				}
			}

		}
	}

	/*
	 * Add the contents of a select that does not have nodesets - minimal -
	 * autocomplete - search
	 */
	private void addMinimalSelectContents(Element parent, Question q, Form form, boolean hideLabels) throws Exception {

		// Add labels
		if(!hideLabels) {
			addLabels(parent, q, form);
		}

		Element textElement = null;
		// Add input / select
		if (q.type.equals("select")) {
			textElement = outputDoc.createElement("select");
			textElement.setAttribute("multiple", "multiple");
		} else {
			textElement = outputDoc.createElement("input");
			textElement.setAttribute("type", "text");
			textElement.setAttribute("data-name", paths.get(getRefName(q.name, form)));
			textElement.setAttribute("list", getListName(q.list_name));
			if (q.calculation != null && q.calculation.trim().length() > 0) {
				textElement.setAttribute("data-calculate", UtilityMethods.convertAllxlsNames(q.calculation, false, paths, form.id, true, q.name, false));
			}
			
		}
		parent.appendChild(textElement);
		textElement.setAttribute("name", paths.get(getRefName(q.name, form)));
		textElement.setAttribute("data-type-xml", q.type);

		if (q.relevant != null && q.relevant.trim().length() > 0) {
			textElement.setAttribute("data-relevant",
					UtilityMethods.convertAllxlsNames(q.relevant, false, paths, form.id, true, q.name, false));
		}
		if(q.constraint != null && q.constraint.length() > 0) {
			textElement.setAttribute("data-constraint", UtilityMethods.convertAllxlsNames(q.constraint,false, paths, form.id, true, q.name, false));
		}		
		if (q.readonly) {
			textElement.setAttribute("readonly", "readonly");
		}
		if (q.isRequired()) {
			textElement.setAttribute("data-required", UtilityMethods.getCompoundExpression(q.required, 
					q.required_expression,
					form.id, q.name, paths));
		}
		// Dynamic Default
		if (q.calculation != null && q.calculation.trim().length() > 0) {
			textElement.setAttribute("data-calculate", UtilityMethods.convertAllxlsNames(q.calculation, false, paths, form.id, true, q.name, false));
		}
		
		
		// Add data list
		if (q.type.equals("select1")) {
			Element dlElement = outputDoc.createElement("datalist");
			textElement.appendChild(dlElement);
			dlElement.setAttribute("id", getListName(q.list_name));
			addDataList(dlElement, q, form);
		} else {
			addDataList(textElement, q, form);
		}

		// Option translations section
		Element otElement = outputDoc.createElement("span");
		parent.appendChild(otElement);
		otElement.setAttribute("class", "or-option-translations");
		otElement.setAttribute("style", "display:none;");
		addOptionTranslations(otElement, q, form);

	}
	
	/*
	 * Add the contents of a select that has nodesets - minimal 
	 */
	private void addMinimalContents(Connection sd, Element parent, Question q, Form form, boolean autoComplete, boolean hideLabels) throws Exception {

	
		// Add labels
		if(!hideLabels) {
			addLabels(parent, q, form);
		}

		// Add search
		Element selectElement = null;
		if(autoComplete) {
		 selectElement = outputDoc.createElement("input");
		} else {
			selectElement = outputDoc.createElement("select");
		}
		parent.appendChild(selectElement);
		selectElement.setAttribute("name", paths.get(getRefName(q.name, form)));
		selectElement.setAttribute("data-name", paths.get(getRefName(q.name, form)));
		if (q.type.equals("select")) {
			selectElement.setAttribute("multiple", "multiple");
		}
		selectElement.setAttribute("data-type-xml", q.type);
		if(autoComplete) {
			selectElement.setAttribute("type", "text");
			selectElement.setAttribute("list", getListName(paths.get(getRefName(q.name, form))));
		}
		if (q.relevant != null && q.relevant.trim().length() > 0) {
			selectElement.setAttribute("data-relevant",
					UtilityMethods.convertAllxlsNames(q.relevant, false, paths, form.id, true, q.name, false));
		}
		if (q.constraint != null && q.constraint.trim().length() > 0) {
			selectElement.setAttribute("data-constraint",
					UtilityMethods.convertAllxlsNames(q.constraint, false, paths, form.id, true, q.name, false));
		}
		if (q.isRequired()) {
			selectElement.setAttribute("data-required", UtilityMethods.getCompoundExpression(q.required, 
					q.required_expression,
					form.id, q.name, paths));
		}
		if (q.calculation != null && q.calculation.trim().length() > 0) {
			selectElement.setAttribute("data-calculate", UtilityMethods.convertAllxlsNames(q.calculation, false, paths, form.id, true, q.name, false));
		}
		if(q.readonly) {
			selectElement.setAttribute("readonly", "readonly");
		}
		
		// Itemset template option
		if(!autoComplete) {
			Element templateOption = outputDoc.createElement("option");
			selectElement.appendChild(templateOption);
			templateOption.setAttribute("class", "itemset-template");
			templateOption.setAttribute("value", "");
			templateOption.setAttribute("data-items-path", getNodeset(q, form));
			templateOption.setTextContent("...");
		}
		
		// Data List
		if(autoComplete) {
			Element dlElement = outputDoc.createElement("datalist");
			parent.appendChild(dlElement);
			dlElement.setAttribute("id", getListName(paths.get(getRefName(q.name, form))));
			Element dlOption = outputDoc.createElement("option");
			dlElement.appendChild(dlOption);
			dlOption.setAttribute("class", "itemset-template");
			dlOption.setAttribute("value", "");
			dlOption.setAttribute("data-items-path", getNodeset(q, form));
		}
		
		
		// Option translations section
		// <span class="or-option-translations" style="display:none;">
		Element otElement = outputDoc.createElement("span");
		parent.appendChild(otElement);
		otElement.setAttribute("class", "or-option-translations");
		otElement.setAttribute("style", "display:none;");

		// Itemset labels
		Element optionElement = outputDoc.createElement("span");
		parent.appendChild(optionElement);
		optionElement.setAttribute("class", "itemset-labels");
		if(q.nodeset.startsWith("${")) {
		   	addRepeatNodesetRefs(optionElement, q.nodeset, formRefs, paths, form.id);
		} else {
			optionElement.setAttribute("data-value-ref", "name");
			if(q.external_choices) {
				optionElement.setAttribute("data-label-type", "label");
				optionElement.setAttribute("data-label-ref", "label");
			} else {
				optionElement.setAttribute("data-label-type", "itext");
				optionElement.setAttribute("data-label-ref", "itextId");
			}
		}
		

		addMinimalOptionLabels(sd, optionElement, q, form);

	}

	/*
	 * Add refs to a repeat nodeset
	 */
	private void addRepeatNodesetRefs(Element optionElement, String nodeset, HashMap<String, String> formRefs, HashMap<String, String> paths, int formId) throws Exception {
		int idx = nodeset.indexOf('[');
    	if(idx > 0) {
			String repQuestionXLS = nodeset.substring(0, idx).trim();
			
			NodesetFormDetails formDetails = UtilityMethods.getFormDetails(null, formRefs, repQuestionXLS, paths, formId);
			String repQuestionName = UtilityMethods.convertAllxlsNames(repQuestionXLS, false, paths,  
					formId, true, formDetails.formName, true);
			
			optionElement.setAttribute("data-value-ref", repQuestionName);
			optionElement.setAttribute("data-label-ref", repQuestionName);
    	}
	}
	/*
	 * Get a list name from a path
	 */
	private String getListName(String in) {
		String out = in.replaceAll("[/_]", ""); 
		out = out.replaceAll("\\.", "x");
		return out;
	}
	/*
	 * Add the contents of a select
	 * 
	 */
	private void addSelectContents(Connection sd, Element parent, Question q, Form form, boolean tableList, boolean hideLabels)
			throws Exception {

		// legend
		Element bodyElement = outputDoc.createElement("legend");
		parent.appendChild(bodyElement);
		if (!tableList && !hideLabels) {
			addLabels(bodyElement, q, form);
		}

		// Input element for rank
		if(q.type.equals("rank")) {
			Element inputElement = outputDoc.createElement("input");
			inputElement.setAttribute("class", "rank hide");
			inputElement.setAttribute("type", "error");
			inputElement.setAttribute("name", paths.get(getRefName(q.name, form)));
			inputElement.setAttribute("data-type-xml", "rank");
			if (q.calculation != null && q.calculation.trim().length() > 0) {
				inputElement.setAttribute("data-calculate", UtilityMethods.convertAllxlsNames(q.calculation, false, paths, form.id, true, q.name, false));
			}
			if (q.isRequired()) {
				inputElement.setAttribute("data-required", UtilityMethods.getCompoundExpression(q.required, 
						q.required_expression,
						form.id, q.name, paths));
			}
			if (q.relevant != null && q.relevant.trim().length() > 0) {
				inputElement.setAttribute("data-relevant",
						UtilityMethods.convertAllxlsNames(q.relevant, false, paths, form.id, true, q.name, false));
			}
			if (q.constraint != null && q.constraint.trim().length() > 0) {
				inputElement.setAttribute("data-constraint",
						UtilityMethods.convertAllxlsNames(q.constraint, false, paths, form.id, true, q.name, false));
			}
			
			parent.appendChild(inputElement);
		}
		// Option wrapper
		Element optionWrapperElement = outputDoc.createElement("div");
		parent.appendChild(optionWrapperElement);		
		if(q.type.equals("rank")) {
			optionWrapperElement.setAttribute("class", "option-wrapper widget rank-widget rank-widget--empty");
			optionWrapperElement.setAttribute("aria-dropeffect", "move");
		} else {      
			optionWrapperElement.setAttribute("class", getDynamicOptionsClass(q));
		}

		// options
		addOptions(sd, optionWrapperElement, q, form, tableList);
	}
	
	private String getDynamicOptionsClass(Question q) {
		StringBuilder cValue = new StringBuilder("option-wrapper");
		
		if(q.appearance != null && q.appearance.contains("lookup_choices")) {
			cValue.append(" dchoice_").append(q.name);
		}
		return cValue.toString(); 
	}

	/*
	 * Add the contents of a label
	 */
	private void addLabelContents(Element parent, Question q, Form form, boolean hideLabels) throws Exception {

		StringBuilder classValue = new StringBuilder();
		
		// span
		if(!hideLabels) {
			addLabels(parent, q, form);
		}

		/*
		 * Input
		 */
		Element bodyElement;
		if(q.appearance == null) {
			q.appearance = "";
		}
		String rp = GeneralUtilityMethods.getSurveyParameter("rows", q.paramArray);
		
		if(q.type.equals("string") && (q.appearance.contains("multiline") || rp != null)) {
			bodyElement = outputDoc.createElement("textarea");
			if(rp != null) {
				bodyElement.setAttribute("rows", rp);
			} else {
				bodyElement.setAttribute("rows", "5");
			}
		} else {
			bodyElement = outputDoc.createElement("input");
		} 
		bodyElement.setAttribute("type", getInputType(q));
		bodyElement.setAttribute("name", paths.get(getRefName(q.name, form)));
		bodyElement.setAttribute("data-type-xml", getXmlType(q));
		
		// media specific
		if (q.type.equals("image")) {
			bodyElement.setAttribute("accept", "image/*");
			if(q.appearance.contains("new")) {
				bodyElement.setAttribute("capture", "camera");
			}
		} else if (q.type.equals("audio")) {
			bodyElement.setAttribute("accept", "audio/*");
		} else if (q.type.equals("video")) {
			bodyElement.setAttribute("accept", "video/*");
		} else if (q.type.equals("file")) {
			bodyElement.setAttribute("accept", FILE_MIME);
		}

		// note and read only specific
		if (q.type.equals("note") || q.readonly) {
			bodyElement.setAttribute("readonly", "readonly");
		}
		
		if(q.type.equals("int")) {
			bodyElement.setAttribute("maxlength", "9");
		}
		
		// range specific
		if (q.type.equals("range")) {
			bodyElement.setAttribute("min", GeneralUtilityMethods.getSurveyParameter("start", q.paramArray));
			bodyElement.setAttribute("max", GeneralUtilityMethods.getSurveyParameter("end", q.paramArray));
			bodyElement.setAttribute("step", GeneralUtilityMethods.getSurveyParameter("step", q.paramArray));
			classValue.append("hide");
		}

		// Required - note allow required on read only questions to support form level
		// validation trick
		if (q.isRequired() && !q.type.startsWith("select")) {
			bodyElement.setAttribute("data-required", UtilityMethods.getCompoundExpression(q.required, 
					q.required_expression,
					form.id, q.name, paths));
		}

		// decimal
		if (q.type.equals("decimal")) {
			bodyElement.setAttribute("step", "any");
		}
		
		if(q.type.equals(SmapQuestionTypes.PARENT_FORM) || q.type.equals(SmapQuestionTypes.CHILD_FORM)) {

			// Add a calculation containing the URL
			bodyElement.setAttribute("data-calculate",
					UtilityMethods.convertAllxlsNames(getFormLaunchURL(q), false, paths, form.id, true, q.name, false));
		}

		// constraint
		if (q.constraint != null && q.constraint.trim().length() > 0) {
			bodyElement.setAttribute("data-constraint",
					UtilityMethods.convertAllxlsNames(q.constraint, false, paths, form.id, true, q.name, false));
		}

		// relevant
		if (q.relevant != null && q.relevant.trim().length() > 0) {
			bodyElement.setAttribute("data-relevant",
					UtilityMethods.convertAllxlsNames(q.relevant, false, paths, form.id, true, q.name, false));
		}
		
		// Add dynamic defaults
		if(q.calculation != null && q.calculation.length() > 0) {
			bodyElement.setAttribute("data-calculate", UtilityMethods.convertAllxlsNames(q.calculation, false, paths, form.id, true, q.name, false));
		}
		
		/*
		 * Add setvalue
		 */
		if(q.setValues != null && q.setValues.size() > 0) {
			SetValue sv = q.setValues.get(0);		// Can only handle one?
		
			bodyElement.setAttribute("data-event", sv.event);
			bodyElement.setAttribute("data-setvalue", UtilityMethods.convertAllxlsNames(sv.value, false, paths, form.id, true, q.name, false));
			if(classValue.length() > 0) {
				classValue.append(" ");
			}
			classValue.append("action form-control-action ref-target ");
			classValue.append(sv.event);
		}
		
		if(classValue.length() > 0) {
			bodyElement.setAttribute("class",  classValue.toString());
		}
		
		parent.appendChild(bodyElement);
		if (q.type.equals("image")) {
			parent.appendChild(createDynamicInput());	// Add a dummy value for dynamic defaults
		}
		
	}
	
	private Element createDynamicInput() {
		Element e = outputDoc.createElement("input");
		e.setAttribute("class", "dynamic-input ignore hide");
		e.setAttribute("type", "text");
		return e;
	}

	private void addOptions(Connection sd, Element parent, Question q, Form form, boolean tableList) throws Exception {

		boolean isLikert = false;
		if(q.appearance != null && q.appearance.contains("likert")) {
			isLikert = true;
		}
		
		// Itemset Template
		if (hasNodeset(sd, q, form)) {
			Element labelElement = outputDoc.createElement("label");
			parent.appendChild(labelElement);
			labelElement.setAttribute("class", "itemset-template");
			if(isLikert) {
				labelElement.setAttribute("style", "display:none;");
			}
			labelElement.setAttribute("data-items-path", getNodeset(q, form));

			Element inputElement = outputDoc.createElement("input");
			labelElement.appendChild(inputElement);
			inputElement.setAttribute("type", getInputType(q));
			if (!tableList) {
				inputElement.setAttribute("name", paths.get(getRefName(q.name, form)));
				inputElement.setAttribute("data-name", paths.get(getRefName(q.name, form)));
			} else {
				// create dummy name for table list
				inputElement.setAttribute("name", paths.get(getRefName(q.name, form)) + "_table_list_labels");
				inputElement.setAttribute("data-name", paths.get(getRefName(q.name, form)) + "_table_list_labels");
			}
			inputElement.setAttribute("data-type-xml", getXmlType(q));
			inputElement.setAttribute("value", "");
			if (q.relevant != null && q.relevant.trim().length() > 0) {
				inputElement.setAttribute("data-relevant",
						UtilityMethods.convertAllxlsNames(q.relevant, false, paths, form.id, true, q.name, false));
			}
			if(q.constraint != null && q.constraint.length() > 0) {
				inputElement.setAttribute("data-constraint", UtilityMethods.convertAllxlsNames(q.constraint, false, paths, form.id, true, q.name, false));
			}

			if(q.isRequired()) {
				inputElement.setAttribute("data-required", UtilityMethods.getCompoundExpression(q.required, 
						q.required_expression,
						form.id, q.name, paths));
			}
			if (q.readonly) {
				inputElement.setAttribute("readonly", "readonly");
			}
			if (q.calculation != null && q.calculation.trim().length() > 0) {
				inputElement.setAttribute("data-calculate", UtilityMethods.convertAllxlsNames(q.calculation, false, paths, form.id, true, q.name, false));
			}

			// Itemset labels
			Element optionElement = outputDoc.createElement("span");
			parent.appendChild(optionElement);
			optionElement.setAttribute("class", "itemset-labels");
			if(q.nodeset.startsWith("${")) {
				addRepeatNodesetRefs(optionElement, q.nodeset, formRefs, paths, form.id);
			} else {

				optionElement.setAttribute("data-value-ref", "name");
				optionElement.setAttribute("data-label-type", "itext");
				optionElement.setAttribute("data-label-ref", "itextId");

				addOptionLabels(sd, optionElement, q, form, tableList);
			}

		} else {
			addOptionLabels(sd, parent, q, form, tableList);
		}

	}

	private void addOptionLabels(Connection sd, Element parent, Question q, Form form, boolean tableList) throws Exception {

		boolean hasNodeset = hasNodeset(sd, q, form);
		Element labelElement = null;

		OptionList optionList = survey.surveyData.optionLists.get(q.list_name);
		if(optionList != null) {
			ArrayList<Option> options = optionList.options;
			for (Option o : options) {
				if (!hasNodeset) {
					labelElement = outputDoc.createElement("label");
					parent.appendChild(labelElement);
	
					if (!tableList) {
						Element inputElement = outputDoc.createElement("input");
						labelElement.appendChild(inputElement);
						inputElement.setAttribute("type", getInputType(q));
						if(!q.type.equals("rank")) {
							inputElement.setAttribute("name", paths.get(getRefName(q.name, form)));
						} else {
							inputElement.setAttribute("class", "ignore");
						}
						inputElement.setAttribute("value", o.value);
						//inputElement.setAttribute("data-type-xml", q.type);   // Not used with simple select multiple
						if(q.constraint != null && q.constraint.length() > 0) { 
							// inputElement.setAttribute("data-constraint", q.constraint);
							log.info("XXXXXXXXXXXXXXXXXX wants to set constraint on attribute for question: " + q.name + " : " + q.fId);
						}
					}
	
				}
				int idx = 0;
				Element bodyElement = null;
				for (Language lang : survey.surveyData.languages) {
					bodyElement = outputDoc.createElement("span");
					if (hasNodeset) {
						parent.appendChild(bodyElement);
					} else {
						labelElement.appendChild(bodyElement);
					}
					bodyElement.setAttribute("lang", lang.name);
					bodyElement.setAttribute("class",
							"option-label" + (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));
					bodyElement.setAttribute("data-itext-id", o.text_id);
	
					String label = o.labels.get(idx).text;
					try {
						label = UtilityMethods.convertAllxlsNames(o.labels.get(idx).text, true, paths, form.id, true, o.value, false);
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					}
					bodyElement.setTextContent(sanitise.sanitiseHtml(label));
					
					if(labelElement != null) {
						addMedia(labelElement, o.labels.get(idx), lang, o.text_id);
					} else {
						addMedia(parent, o.labels.get(idx), lang, o.text_id);
					}
	
					idx++;
				}
	
			}
			
			// If this widget is horizontal add filler labels to get spacing right
			if(hasAppearance(q.appearance, "horizontal")) {
				int nFillers = 3 - options.size() % 3;
				
				if(nFillers < 3 && nFillers > 0) {
					for(int i = 0;  i < nFillers; i++) {
						labelElement = outputDoc.createElement("label");
						parent.appendChild(labelElement);
						labelElement.setAttribute("class", "filler");
					}
				}
			}
		} else {
			// Presumably options are sourced from a repeat
		}
		


	}

	private boolean hasAppearance(String app, String test) {
		boolean result = false;
		if(app != null) {
			String[] appComp = app.split(" ");
			for(int i = 0; i < appComp.length; i++) {
				if(test.equals(appComp[i])) {
					return true;
				}
			}
		}
		return result;
	}
	
	private void addMinimalOptionLabels(Connection sd, Element parent, Question q, Form form) throws Exception {

		OptionList ol = survey.surveyData.optionLists.get(q.list_name);
		if(ol != null) {
			ArrayList<Option> options = ol.options;
			
			for (Option o : options) {
				
				//Element inputElement = outputDoc.createElement("span");
				//parent.appendChild(inputElement);
				int idx = 0;
				for (Language lang : survey.surveyData.languages) {
					Element optionElement = outputDoc.createElement("span");
					parent.appendChild(optionElement);
					optionElement.setAttribute("lang", lang.name);
					optionElement.setAttribute("class", "option-label" + (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));
					optionElement.setAttribute("data-itext-id", o.text_id);
					
					String label = o.labels.get(idx).text;
					try {
						label = convertMarkdown(label);
						label = UtilityMethods.convertAllxlsNames(label, true, paths, form.id, true, q.name, false);
	
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					}
					optionElement.setTextContent(sanitise.sanitiseHtml(label));
					idx++;
				}
			}
		} else {
			throw new Exception("List name: " +  q.list_name + " referenced by question " + q.name + " was not found");
		}


	}
	
	/*
	 * Add a wrapper for a group then return the new parent
	 */
	private Element addGroupWrapper(Element parent, Question q, boolean repeat, Form form) throws Exception {
		Element groupElement = outputDoc.createElement("section");
		parent.appendChild(groupElement);
		setQuestionClass(q, groupElement);

		if (q.relevant != null && q.relevant.trim().length() > 0) {
			groupElement.setAttribute("data-relevant",
					UtilityMethods.convertAllxlsNames(q.relevant, false, paths, form.id, true, q.name, false));
		}

		groupElement.setAttribute("name", paths.get(getRefName(q.name, form)));

		addGroupTitle(groupElement, q, form);
		return groupElement;
	}

	/*
	 * Add a wrapper for a repeat then return the new parent
	 */
	private Form addRepeat(Connection sd, Element parent, Question q, Form form) throws Exception {

		Form newForm = null;

		Element bodyElement = outputDoc.createElement("section");
		bodyElement.setAttribute("class", "or-repeat");
		bodyElement.setAttribute("name", paths.get(getRefName(q.name, form)));

		// Process sub form
		for (Form subForm : survey.surveyData.forms) {
			if (subForm.parentQuestion == q.id) { // continue with next form
				processQuestions(sd, bodyElement, subForm);
				newForm = subForm;
				break;
			}
		}

		parent.appendChild(bodyElement);

		return newForm;

	}

	private void addGroupTitle(Element parent, Question q, Form form) {
		Element bodyElement = outputDoc.createElement("h4");
		addLabels(bodyElement, q, form);
		parent.appendChild(bodyElement);
	}

	private void addLabels(Element parent, Question q, Form form) {
		int idx = 0;
		Element bodyElement = null;
		boolean requiredMessageAdded = false;
		boolean constraintMessageAdded = false;
		boolean requiredIndicatorAdded = false;
		for (Language lang : survey.surveyData.languages) {

			// Label
			bodyElement = outputDoc.createElement("span");
			bodyElement.setAttribute("lang", lang.name);
			bodyElement.setAttribute("class", "question-label" + (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));
			bodyElement.setAttribute("data-itext-id", q.text_id);

			String label = q.labels.get(idx).text;
			try {
				label = convertMarkdown(label);
				label = UtilityMethods.convertAllxlsNames(label, true, paths, form.id, true, q.name, false);

			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			bodyElement.setTextContent(sanitise.sanitiseHtml(label));
			parent.appendChild(bodyElement);

			addMedia(parent, q.labels.get(idx), lang, q.text_id);
			
			// Hint
			String hint = q.labels.get(idx).hint;

			if (hint != null && hint.trim().length() > 0) {
				bodyElement = outputDoc.createElement("span");
				bodyElement.setAttribute("lang", lang.name);
				bodyElement.setAttribute("class", "or-hint" + (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));
				bodyElement.setAttribute("data-itext-id", q.hint_id);

				try {
					hint = convertMarkdown(hint);
					hint = UtilityMethods.convertAllxlsNames(hint, true, paths, form.id, true, q.name, false);
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
				bodyElement.setTextContent(sanitise.sanitiseHtml(hint));
				parent.appendChild(bodyElement);
			}
			
			// Guidance
			String guidance = q.labels.get(idx).guidance_hint;

			if (guidance != null && guidance.trim().length() > 0) {
				bodyElement = outputDoc.createElement("details");
				bodyElement.setAttribute("lang", lang.name);
				bodyElement.setAttribute("class", "or-form-guidance" + (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));

				Element summaryElement = outputDoc.createElement("summary");
				summaryElement.setAttribute("data-i18n", "hint.guidance.details");
							
				try {
					guidance = convertMarkdown(guidance);
					guidance = UtilityMethods.convertAllxlsNames(guidance, true, paths, form.id, true, q.name, false);
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
				bodyElement.setTextContent(sanitise.sanitiseHtml(guidance));
				summaryElement.setTextContent(localisation.getString("wf_md"));
				bodyElement.appendChild(summaryElement);
				parent.appendChild(bodyElement);
			}
			
			// Constraint
			constraintMessageAdded = addConstraintMsg(q.labels.get(idx).constraint_msg, lang.name, parent, idx);
			if(q.isRequired()) {
				if(q.labels.get(idx).required_msg != null) {
					if(!requiredIndicatorAdded) {
						bodyElement = outputDoc.createElement("span");
						bodyElement.setAttribute("class", "required");
						bodyElement.setTextContent("*");
						parent.appendChild(bodyElement);
						requiredIndicatorAdded = true;
					}
					parent.appendChild(getRequiredMsg(q.labels.get(idx).required_msg, 
							lang.name, !requiredMessageAdded));
					requiredMessageAdded = true;
				}
			}

			idx++;
		}

		// Constraint message (Without a language)
		if(!constraintMessageAdded) {
			addConstraintMsg(q.constraint_msg, null, parent, 0);
		}

		// Required (Without a language)
		if (q.isRequired() && !requiredMessageAdded) {
			bodyElement = outputDoc.createElement("span");
			bodyElement.setAttribute("class", "required");
			bodyElement.setTextContent("*");
			parent.appendChild(bodyElement);
		}
		
		if (q.readonly && !(parent.getNodeName() == null || parent.getNodeName().equals("label") || parent.getNodeName().equals("h4"))) {
			parent.setAttribute("readonly", "readonly");
		}	
		
	}

	/*
	 * Add a constraint
	 */
	private boolean addConstraintMsg(String msg, String lang, Element parent, int idx) {		

		boolean added = false;
		
		if(lang == null) {
			lang = "";
		}
		
		if (msg != null && msg.length() > 0) {
			Element  bodyElement = outputDoc.createElement("span");
			bodyElement.setAttribute("lang", lang);
			
			String theClass = "or-constraint-msg";
			if(languageIndex == idx || lang.equals("")) {
				theClass += " active";
			}
			bodyElement.setAttribute("class", theClass);
			bodyElement.setTextContent(sanitise.sanitiseHtml(msg));
			parent.appendChild(bodyElement);
			added = true;
		}
		return added;
	}
	
	
	
	/*
	 * Get the required message
	 */
	private Element getRequiredMsg(String msg, String lang, boolean active) {
		
		if(lang == null) {
			lang = "";
		}
		
		Element bodyElement = outputDoc.createElement("span");
		String theClass = "or-required-msg";
		if(active) {
			theClass += " active";
		}
		bodyElement.setAttribute("class", theClass);
		bodyElement.setAttribute("lang", lang);
		bodyElement.setAttribute("data-i18n", "constraint.required");
		if (msg != null && msg.trim().length() > 0) {
			bodyElement.setTextContent(sanitise.sanitiseHtml(msg));
		} else {
			bodyElement.setTextContent(localisation.getString("wf_reqd"));
		}
		return bodyElement;
	}
	/*
	 * Add media
	 */
	private void addMedia(Element parent, Label label, Language lang, String textId) {

		Element bodyElement = null;

		// Image
		String image = label.image;
		if (image != null && image.trim().length() > 0) {
			bodyElement = outputDoc.createElement("img");
			bodyElement.setAttribute("lang", lang.name);
			bodyElement.setAttribute("class", (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));
			bodyElement.setAttribute("src", "jr://images/" + image);
			bodyElement.setAttribute("alt", "image");
			bodyElement.setAttribute("data-itext-id", textId);

			parent.appendChild(bodyElement);
		}

		// Audio
		String audio = label.audio;
		if (audio != null && audio.trim().length() > 0) {
			bodyElement = outputDoc.createElement("audio");
			bodyElement.setAttribute("lang", lang.name);
			bodyElement.setAttribute("class", (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));
			bodyElement.setAttribute("src", "jr://audio/" + audio);
			bodyElement.setAttribute("alt", "audio");
			bodyElement.setAttribute("controls", "controls");
			bodyElement.setAttribute("data-itext-id", textId);
			bodyElement.setTextContent("Audio not supported by your browser");

			parent.appendChild(bodyElement);
		}

		// Video
		String video = label.video;
		if (video != null && video.trim().length() > 0) {
			bodyElement = outputDoc.createElement("video");
			bodyElement.setAttribute("lang", lang.name);
			bodyElement.setAttribute("class", (GeneralUtilityMethods.isSameLanguage(lang.name, survey.surveyData.def_lang) ? " active" : ""));
			bodyElement.setAttribute("src", "jr://video/" + video);
			bodyElement.setAttribute("alt", "video");
			bodyElement.setAttribute("data-itext-id", textId);
			bodyElement.setAttribute("controls", "controls");
			bodyElement.setTextContent("Video not supported by your browser");

			parent.appendChild(bodyElement);
		}
	}

	/*
	 * Return the input type required by enketo
	 */
	private String getInputType(Question q) {

		String type = null;
		if (q.type.equals("int")) {
			type = "tel";
		} else if (q.type.equals("range")) {
			type = "number";
		} else if (q.type.equals("string")) {
			if(q.appearance.contains("numbers")) {
				type = "tel";
			} else {
				type = "text";
			}
		} else if (q.type.equals("phone")) {
			type = "tel";
		} else if (q.type.equals("select1")) {
			type = "radio";
		} else if (q.type.equals("select")) {
			type = "checkbox";
		} else if (q.type.equals("geopoint") || q.type.equals("geoshape") || q.type.equals("geotrace") || q.type.equals("geocompound")) {
			type = "text";
		} else if (q.type.equals("image") || q.type.equals("audio") || q.type.equals("video") || q.type.equals("file")) {
			type = "file";
		} else if (q.type.equals("date")) {
			type = "date";
		} else if (q.type.equals("dateTime")) {
			type = "datetime-local";
		} else if (q.type.equals("time")) {
			type = "time";
		} else if (q.type.equals("note")) {
			type = "text";
		} else if (q.type.equals("decimal")) {
			type = "number";
		} else if (q.type.equals("trigger") || q.type.equals("acknowledge")) {
			type = "radio";
		} else {
			log.info("#### unknown type: " + q.type + " for question " + q.name);
			type = "text";
		}
		return type;
	}

	/*
	 * Return the input type required by enketo
	 */
	private String getXmlType(Question q) {

		String type = null;
		if (q.type.equals("calculate")) {
			type = "string";
		} else if (q.type.equals("image") || q.type.equals("audio") || q.type.equals("video") || q.type.equals("file")) {
			type = "binary";
		} else if (q.type.equals("note")) {
			type = "string";
		} else if (q.type.equals("range")) {
			String p;
			p = GeneralUtilityMethods.getSurveyParameter("step", q.paramArray);
			if(p != null && p.contains(".")) {
				type = "decimal";
			} else {
				p = GeneralUtilityMethods.getSurveyParameter("start", q.paramArray);
				if(p != null && p.contains(".")) {
					type = "decimal";
				} else {
					p = GeneralUtilityMethods.getSurveyParameter("end", q.paramArray);
					if(p != null && p.contains(".")) {
						type = "decimal";
					} else {
						type = "int";
					}
				}
			}
			
		} else if (q.type.equals("trigger") || q.type.equals("acknowledge") ) {
			type = "trigger";
		} else if (q.type.equals("pdf_field")) {
			type = "geotrace";
		} else if (q.type.equals(SmapQuestionTypes.PARENT_FORM) || q.type.equals(SmapQuestionTypes.CHILD_FORM)) {
			type = "string";
		} else {
			type = q.type;
		}
		return type;
	}

	/*
	 * Returns true if the question has any label - text, image, audio or video
	 */
	private boolean hasLabel(Question q) {
		boolean hasLabel = false;

		for (int i = 0; i < survey.surveyData.languages.size(); i++) {
			if (q.labels.get(i) != null) {
				Label l = q.labels.get(i);
				if ((l.text != null && l.text.trim().length() > 0) || l.image != null || l.video != null
						|| l.audio != null) {
					hasLabel = true;
					break;
				}
			}

		}
		return hasLabel;

	}

	private String getRefName(String qName, Form form) {
		if(qName.equals("the_geom")) {
			return qName + form.id;
		} else {
			return qName;
		}
	}

	/*
	 * Convert Markdown as per support in Enketo
	 *  Supported
	 *     links:  [xxx](url)
	 *     strong: __, ** 
	 *     emphasis: _, *
	 *     paragraphs: \n
	 */
	private String convertMarkdown(String in) {

		// links
		if (in != null) {
			// Test for links
			StringBuffer out = new StringBuffer();
			String pattern = "\\[([^]]*)\\]\\(([^\\s^\\)]*)"; // from https://stackoverflow.com/a/40178293/1867651
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(in);

			int start = 0;
			while (m.find()) {
				if (m.start() > start) {
					out.append(in.substring(start, m.start()));
				}
				out.append("<a href =\"");
				out.append(m.group(2));
				out.append("\" target=\"_blank\">");
				out.append(m.group(1));
				out.append("</a>");

				start = m.end() + 1;
			}

			if (start < in.length()) {
				out.append(in.substring(start));
			}
			
			// Escape characters that should not be converted
			String outString = out.toString();
			outString = outString.replaceAll("\\\\&", "&amp;");
			outString = outString.replaceAll("\\\\\\\\", "&92;");
			outString = outString.replaceAll("\\\\\\*", "&42;");
			outString = outString.replaceAll("\\\\_", "&95;");
			outString = outString.replaceAll("\\\\#", "&35;");

			// Escape underscores in names
			boolean inName = false;
			StringBuffer outBuffer = new StringBuffer("");
			for(int i = 0; i < outString.length(); i++) {
				if(i < (outString.length() - 1) && outString.charAt(i) == '$' && outString.charAt(i + 1) == '{') {
					inName = true;
					outBuffer.append(outString.charAt(i));
				} else if(outString.charAt(i) == '}') {
					inName = false;
					outBuffer.append(outString.charAt(i));
				} else if(inName && outString.charAt(i) == '_') {
					outBuffer.append("&95;");
				} else {
					outBuffer.append(outString.charAt(i));
				}
			}
			outString = outBuffer.toString();
			
			outString = outString.replaceAll("__(.*?)__", "<strong>$1</strong>");						// Strong: __
			outString = outString.replaceAll("\\*\\*(.*?)\\*\\*", "<strong>$1</strong>");				// Strong: **
			outString = outString.replaceAll("_(.*?)_", "<em>$1</em>");								// Emphasis: _
			outString = outString.replaceAll("\\*(.*?)\\*", "<em>$1</em>");							// Emphasis: *
			outString = outString.replaceAll("\n", "<br/>" );											// New lines
			
			outString = outString.replaceAll("^\\s*#\\s+(.*)(\\n|$)", "<h1>$1</h1>");					// Heading 1
			outString = outString.replaceAll("^\\s*##\\s+(.*)(\\n|$)", "<h2>$1</h2>");				// Heading 2
			outString = outString.replaceAll("^\\s*###\\s+(.*)(\\n|$)", "<h3>$1</h3>");				// Heading 3
			outString = outString.replaceAll("^\\s*####\\s+(.*)(\\n|$)", "<h4>$1</h4>");				// Heading 4
			outString = outString.replaceAll("^\\s*#####\\s+(.*)(\\n|$)", "<h5>$1</h5>");				// Heading 5
			outString = outString.replaceAll("^\\s*######\\s+(.*)(\\n|$)", "<h6>$1</h6>");			// Heading 6
			
			outString = outString.replaceAll("&35;", "#");
			outString = outString.replaceAll("&95;", "_");
			outString = outString.replaceAll("&42;", "*");
			outString = outString.replaceAll("&92;", "\\\\");
			outString = outString.replaceAll("&amp;", "&");
			
			return outString;
		} else {
			return in;
		}

	}

	private boolean minSelect(String appearance) {

		if (appearance.contains("minimal") || appearance.contains("autocomplete") || appearance.contains("search")) {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * Attempt to get the full nodeset incorporating any external filters
	 */
	private String getNodeset(Question q, Form form) throws Exception {	
		String nodeset = null;
		if(q.nodeset.startsWith("${")) {
			nodeset = UtilityMethods.getRepeatNodeset(null, formRefs, paths, form.id,  q.nodeset);
		} else {
			nodeset =  UtilityMethods.getNodeset(true, false, paths, true, q.nodeset, q.appearance, form.id, q.name, 
					false /*(form.parentform > 0)*/);		// XXXXXX In our version of enketo core multiple relative predicates do not work. use non relative paths. Use relative paths if in a subform

		}
		String adjustedNodeset = GeneralUtilityMethods.addNodesetFunctions(nodeset, 
				GeneralUtilityMethods.getSurveyParameter("randomize", q.paramArray),
				GeneralUtilityMethods.getSurveyParameter("seed", q.paramArray)); 
		return adjustedNodeset;
	}

	/*
	 * Return true if this question has a nodeset
	 */
	private boolean hasNodeset(Connection sd, Question q, Form form) throws Exception {

		if (q.nodeset == null || q.nodeset.trim().length() == 0) {
			return false;
		} else if(q.type.equals("rank") || (q.appearance != null && q.appearance.contains("lookup_choices"))) {  // lookup choices is vanilla select in webforms
			return false;
		} else {
			return true;
		}

	}

	private void addDataList(Element parent, Question q, Form form) throws Exception {

		ArrayList<Option> options = survey.surveyData.optionLists.get(q.list_name).options;

		Element bodyElement = outputDoc.createElement("option"); // No selection value
		parent.appendChild(bodyElement);
		bodyElement.setAttribute("value", "");
		bodyElement.setTextContent("...");
		if(options != null) {
			for (Option o : options) {
	
				bodyElement = outputDoc.createElement("option");
				parent.appendChild(bodyElement);
				bodyElement.setAttribute("value", o.value);
				String label = UtilityMethods.convertAllxlsNames(o.labels.get(languageIndex).text, true, paths, form.id,
						true, o.value, false);
				bodyElement.setTextContent(sanitise.sanitiseHtml(label));
			}
		}

	}

	private void addOptionTranslations(Element parent, Question q, Form form) throws Exception {

		ArrayList<Option> options = survey.surveyData.optionLists.get(q.list_name).options;
		if(options != null) {
			for (Option o : options) {
				int idx = 0;
				Element bodyElement = null;
				for (Language lang : survey.surveyData.languages) {
					bodyElement = outputDoc.createElement("span");
					parent.appendChild(bodyElement);
					bodyElement.setAttribute("lang", lang.name);
					bodyElement.setAttribute("data-option-value", o.value);
					String label = UtilityMethods.convertAllxlsNames(o.labels.get(idx).text, true, paths, form.id, true, o.value, false);
					bodyElement.setTextContent(sanitise.sanitiseHtml(label));
	
					idx++;
				}
			}
		}

	}

	private Question getTableListLabelQuestion(Question q, Form form) {
		boolean inGroup = false;

		Question labelQ = null;

		for (Question qx : form.questions) {
			if (qx.type.equals("begin group") && qx.name.equals(q.name)) {
				inGroup = true;
				continue; // Skip the begin group question
			}
			if (inGroup && qx.type.equals("end group")) {
				inGroup = false;
				break; // Must be done
			}

			if (inGroup) {
				if (qx.type.startsWith("select")) {
					labelQ = qx;
					break; // Only need labels from one of the select questions
				}

			}
		}

		return labelQ;
	}

	/*
	 * Count occurence of characters in a string
	 */
	private int countChars(String in, char c) {
		int count = 0;
		if(in != null) {
			char [] cArray = in.toCharArray();
			for(int i = 0; i < cArray.length; i++) {
				if(cArray[i] == c) {
					count++;
				}
			}
		}
		return count;
	}
	
	/*
	 * Remove data casts these are required for database expressions but not for xpath
	 */
	private String removeCasts(String in) {
		StringBuilder sb = new StringBuilder("");
		
		if(in == null) {
			return null;
		} else {
			int lastIdx = 0;
			int idx = in.indexOf("cast(", 0);
			while(idx >= 0 && idx < in.length()) {
				sb.append(in.substring(lastIdx, idx));
				lastIdx = in.indexOf("#{", idx);
				if(lastIdx >= 0) {
					idx = in.indexOf('}', lastIdx);
					if(idx >= 0) {
						sb.append(in.substring(lastIdx, idx));
						idx = in.indexOf(')', idx);
						if(idx >= 0) {
							idx++;
						}
					}
				}
			}
		}
		return sb.toString();
	}
	
	String processPulldataSuffix(String calculation) {
		// Add a suffix to pulldata sources to differentiate them from search
		StringBuilder sb = new StringBuilder("");
		int idx1;
		while ((idx1 = calculation.indexOf("pulldata")) >= 0) {
			idx1 = GeneralUtilityMethods.indexOfQuote(calculation, idx1);
			int idx2 = GeneralUtilityMethods.indexOfQuote(calculation, idx1 + 1);
			sb.append(calculation.substring(0, idx2) + "__pull");
			calculation = calculation.substring(idx2);
		}
		sb.append(calculation);
		return sb.toString();
	}
	
	/*
	 * Get a calculation to create the URL to launch another survey from a survey
	 * This has to combine single quoted text with references to answers to questions without quotes
	 */
	String getFormLaunchURL(Question q) throws SQLException {
		
		String surveyIdent = GeneralUtilityMethods.getSurveyParameter("form_identifier", q.paramArray);
		String instance = GeneralUtilityMethods.getSurveyParameter("instance", q.paramArray);
		String initial = GeneralUtilityMethods.getSurveyParameter("initial", q.paramArray);
		
		StringBuilder url = new StringBuilder("concat('")
				.append(gUrlPrefix)
				.append("app/myWork/webForm/")
				.append(surveyIdent)
				.append("'");
		
		if(instance != null) {
			instance = instance.trim();
			if(instance.length() > 0) {
				url.append(",'?datakey=instanceid&datakeyvalue=',");
				if(instance.startsWith("$")) {
					url.append(instance);
				} else {
					url.append("'").append(instance).append("'");
				}
			}
		}
		
		if(initial != null) {
			initial = initial.trim();
			if(initial.length() > 0) {
				
				url.append(",'")
					.append(instance == null ? "?" : "&")
					.append("initial='");
				
				String [] a = initial.split(",");
				if(a.length > 0) {
					for(int i = 0; i < a.length; i++) {
						String [] a2 = a[i].split(":");
						if(i > 0) {
							url.append(",','");
						}
						if(a2.length == 2) {
							for(int j = 0; j < a2.length; j++) {
								a2[j] = a2[j].trim();
								url.append(",")
									.append(j == 0 ? "" : "':',")
									.append(a2[j].startsWith("$") ? a2[j] : "'" + a2[j] + "'");
							}	
						}
					}
				}
			}
		}
		url.append(")");
		
		return url.toString();
	}
	
}
