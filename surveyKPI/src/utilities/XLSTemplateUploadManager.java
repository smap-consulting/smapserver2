package utilities;


/*
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

 */

import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.Stack;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.*;
import org.javarosa.xpath.XPathParseTool;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.ApplicationWarning;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.constants.SmapQuestionTypes;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.constants.XLSFormColumns;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Condition;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.ManifestInfo;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Pulldata;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.RoleColumnFilterRef;
import org.smap.sdal.model.ServerCalculation;
import org.smap.sdal.model.SetValue;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.StyleList;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumnMarkup;

public class XLSTemplateUploadManager {

	/*
	 * Globals
	 */
	Workbook wb = null;
	Sheet surveySheet = null;
	Sheet choicesSheet = null;
	Sheet settingsSheet = null;

	int rowNumSurvey = 0;			// Heading row is 0
	int rowNumChoices = 0;		
	int rowNumSettings = 0;	
	int rowNumStyles = 0;
	int rowNumConditions = 0;
	int lastRowNumSurvey = 0;
	int lastRowNumChoices = 0;
	int lastRowNumSettings = 0;
	int lastRowNumStyles = 0;
	int lastRowNumConditions = 0;

	HashMap<String, Integer> surveyHeader = null;
	HashMap<String, Integer> choicesHeader = null;
	HashMap<String, Integer> settingsHeader = null;
	HashMap<String, Integer> stylesHeader = null;
	HashMap<String, Integer> conditionsHeader = null;
	HashMap<String, Integer> choiceFilterHeader = null;
	HashMap<String, Integer> columnRoleHeader = null;
	HashMap<String, Integer> rowRoleHeader = null;
	
	HashMap<String, QuestionForm> questionNames;	// Mapping between question name and truncated name
	HashMap<String, String> optionNames;			// Mapping between option name and truncated name
	boolean merge;

	HashMap<String, Integer> qNameMap = new HashMap<> ();							// Use in question name validation
	HashMap<String, Integer> qNameMapCaseInsensitive = new HashMap<> ();			// Use in question name uniqueness
	HashMap<String, HashMap<String, Integer>> oNameMap = new HashMap<> ();		// Use in option name validation
	Pattern validQname = Pattern.compile("^[A-Za-z_][A-Za-z0-9_\\-\\.]*$");
	Pattern validChoiceName = Pattern.compile("^[A-Za-z0-9_@&\\-\\.\\+%,():/]*$");

	HashMap<Integer, Stack<Question>> groupStackMap = new HashMap<>();			// Keep track of groups in forms
	boolean inFieldList = false;												// Only some questions are allowed inside a field list
	
	boolean useDefaultLanguage = false;
	
	boolean inTableListGroup = false;
	boolean foundSelectInTableListGroup = false;
	boolean justStartedTableListGroup = false;
	
	int metaId = MetaItem.INITIAL_ID;

	Survey survey = null;

	private class MatrixWidget {
		private Question begin;
		private ArrayList<Question> member = new ArrayList<Question> ();
		private int rowNumber;
		
		public MatrixWidget(Question q, int row) {
			begin = q;
			rowNumber = row;
		}
		public void addMember(Question q) {
			member.add(q);
		}
		public ArrayList<Question> getExpanded() {
			ArrayList<Question> expanded = new ArrayList<Question> ();
			
			ArrayList<Option> choices = survey.optionLists.get(begin.list_name).options;
			expanded.addAll(getGroupQuestions(null, begin.name, "header", 1 + 2 * member.size()));
			for(Option c : choices) {
				expanded.addAll(getGroupQuestions(c, begin.name, c.value, 1 + 2 * member.size()));
			}
			
			return expanded;
		}
		private ArrayList<Question> getGroupQuestions(Option choice, String matrixName, String choiceName, int groupWidth) {
			ArrayList<Question> questions = new ArrayList<Question> ();
			
			Question qb = new Question();
			qb.type = "begin group";
			qb.name = matrixName + "_" + choiceName;
			qb.appearance = "w" + groupWidth;
			qNameMap.put(qb.name, rowNumber);
			qNameMapCaseInsensitive.put(qb.name.toLowerCase(), rowNumber);
			questions.add(qb);
			
			Question qb2 = new Question();
			qb2.type = "note";
			qb2.source = "user";
			qb2.name = qb.name + "_note";
			qb2.columnName = GeneralUtilityMethods.cleanName(qb2.name, true, true, true);
			qb2.appearance = "w1";
			if(choice == null) {
				qb2.labels = copyLabelsFrom(begin.labels, "bold");
			} else {
				qb2.labels = copyLabelsFrom(choice.labels, "hash");
			}
			qNameMap.put(qb2.name, rowNumber);
			qNameMapCaseInsensitive.put(qb2.name.toLowerCase(), rowNumber);
			questions.add(qb2);
			
			for(Question qm : member) {
				// Clone the question so that the original members can be preserved
				Question qx = new Question(qm);
				qx.name = qb.name + "_" + qm.name;
				qx.source = "user";
				qx.appearance = "w2";
				qx.columnName = GeneralUtilityMethods.cleanName(qx.name, true, true, true);
				if(choice == null) {
					qx.type = "note";					
					qx.labels = copyLabelsFrom(qm.labels, "bold");
				} else {
					if(qx.type.startsWith("select")) {
						qx.appearance += "  horizontal-compact";
					} else {
						qx.appearance += "  no-label";
					}
					qx.labels = copyLabelsFrom(qm.labels, "empty");
				}
				questions.add(qx);
			}
			
			Question qe = new Question();
			qe.type = "end group";
			qe.name = qb.name;
			questions.add(qe);
			
			return questions;
		}
		
		private ArrayList<Label> copyLabelsFrom(ArrayList<Label> inLabels, String style) {
			ArrayList<Label> labels = new ArrayList<Label> ();
			for(Label l : inLabels) {
				Label nl = new Label();
				if(style.equals("bold")) {
					nl.text = "<h5>" + l.text + "</h5>";
				} else if(style.equals("hash")) {
					nl.text = "<h5>" + l.text + "</h5>";
				} else if(style.equals("empty")) {
					nl.text = "<span style='display:none'>" + l.text + "</span>";
				} else {
					nl.text = l.text;
				}
				labels.add(nl);
			}
			return labels;
		}
	}
	
	private class FunctionCheck {
		String name;
		int args;
		String template;
		
		FunctionCheck(String n, int a, String t) {
			name = n;
			args = a;
			template = t;
		}
	}
	private ArrayList<FunctionCheck> functions = new ArrayList<> ();
	private ResourceBundle localisation = null;
	private ArrayList<ApplicationWarning> warnings = null;

	public XLSTemplateUploadManager(ResourceBundle l, ArrayList<ApplicationWarning> w) {
		
		localisation = l;
		warnings = w;
		
		// Initialise Function Check array
		functions.add(new FunctionCheck("count", 1, "count(nodeset)"));
		functions.add(new FunctionCheck("if", 3, "if(condition, a, b)"));
		
		// Initialise question name map with internal names
		qNameMap.put("_user", -1);
		qNameMap.put(SmapServerMeta.UPLOAD_TIME_NAME, -1);
		qNameMap.put("_hrk", -1);
		qNameMap.put("_version", -1);
		
	}

	/*
	 * Get a survey definition from an XLS file
	 */
	public Survey getSurvey(
			Connection sd,
			int oId, 
			String type, 
			InputStream inputStream, 
			String displayName,
			int p_id,
			HashMap<String, QuestionForm> questionNames,
			HashMap<String, String> optionNames,
			boolean merge,
			int existingVersion) throws Exception {

		Sheet conditionSheet = null;
		
		this.questionNames = questionNames;
		this.optionNames = optionNames;
		this.merge = merge;

		wb = WorkbookFactory.create(inputStream);

		// Create survey and set defaults
		survey = new Survey();
		survey.displayName = displayName;
		survey.o_id = oId;
		survey.p_id = p_id;
		survey.version = merge ? existingVersion + 1 : 1;
		survey.loadedFromXLS = true;
		survey.deleted = false;
		survey.blocked = false;
		survey.meta.add(new MetaItem(metaId--, "string", "instanceID", null, "instanceid", null, false, null, null));
		survey.meta.add(new MetaItem(metaId--, "string", "instanceName", null, "instancename", null, false, null, null));

		surveySheet = wb.getSheet("survey");		
		choicesSheet = wb.getSheet("choices");
		settingsSheet = wb.getSheet("settings");
		conditionSheet = wb.getSheet("conditions");

		if(surveySheet == null) {
			throw XLSUtilities.getApplicationException(localisation, "tu_nw", -1, "survey", null, null, null);
		} else if(surveySheet.getPhysicalNumberOfRows() == 0) {
			throw XLSUtilities.getApplicationException(localisation, "tu_ew", -1, "survey", null, null, null);
		}

		lastRowNumSurvey = surveySheet.getLastRowNum();
		if(choicesSheet != null) {
			lastRowNumChoices = choicesSheet.getLastRowNum();
		}
		if(settingsSheet != null) {
			lastRowNumSettings = settingsSheet.getLastRowNum();
		}		

		getHeaders();	// get headers for survey, choices and setting. Use thse to set the languages

		/*
		 * 1. Process the choices sheet
		 */
		if(choicesSheet != null) {
			while(rowNumChoices <= lastRowNumChoices) {

				Row row = choicesSheet.getRow(rowNumChoices++);

				if(row != null) {
					int lastCellNum = row.getLastCellNum();	
					String listName = XLSUtilities.getTextColumn(row, "list_name", choicesHeader, lastCellNum, null);

					if(listName != null) {
						OptionList ol = survey.optionLists.get(listName);
						if(ol == null) {
							ol = new OptionList();
							survey.optionLists.put(listName, ol);
						}
						ol.options.add(getOption(row, listName));
					}

				}
			}
		}

		/*
		 * 2. Process the styles sheet
		 */
		Sheet styleSheet = wb.getSheet("styles");
		if(styleSheet != null) {

			lastRowNumStyles = styleSheet.getLastRowNum();
			getStyleHeaders(styleSheet);

			while(rowNumStyles <= lastRowNumStyles) {

				Row row = styleSheet.getRow(rowNumStyles++);

				if(row != null) {
					int lastCellNum = row.getLastCellNum();	
					String styleList = XLSUtilities.getTextColumn(row, "list_name", stylesHeader, lastCellNum, null);
					if(styleList == null) {
						styleList = XLSUtilities.getTextColumn(row, "list name", stylesHeader, lastCellNum, null);
					}

					if(styleList != null) {
						StyleList sl = survey.styleLists.get(styleList);
						if(sl == null) {
							sl = new StyleList ();
							survey.styleLists.put(styleList, sl);
						}
						sl.markup.add(getStyle(row, styleList));
					}

				}
			}
			styleSheet = null;		// Free memory
		}

		/*
		 * 3. Process the conditions sheet
		 */
		if(conditionSheet != null) {

			lastRowNumConditions = conditionSheet.getLastRowNum();
			getConditionHeaders(conditionSheet);

			while(rowNumConditions <= lastRowNumConditions) {

				Row row = conditionSheet.getRow(rowNumConditions++);

				if(row != null) {
					int lastCellNum = row.getLastCellNum();	
					String questionName = XLSUtilities.getTextColumn(row, "question_name", conditionsHeader, lastCellNum, null);
					String rule = XLSUtilities.getTextColumn(row, "rule", conditionsHeader, lastCellNum, null);
					String value = XLSUtilities.getTextColumn(row, "value", conditionsHeader, lastCellNum, null);

					if(questionName != null && rule != null && value != null) {
						ServerCalculation sc = survey.serverCalculations.get(questionName);
						if(sc == null) {
							sc = new ServerCalculation ();
							survey.serverCalculations.put(questionName, sc);
						}
						sc.addCondition(new Condition(rule, value));
					}

				}
			}
			conditionSheet = null;		// Free memory
		}
		
		
		/*
		 * 4. Process the survey sheet
		 */
		Form f = getForm("main", -1, -1, null);
		// Validate the top level form
		if(survey.forms.get(0).questions.size() == 0) {
			throw new ApplicationException(localisation.getString("tu_nq"));
		}
		validateForm(1, f);

		/*
		 * 5, Process the settings sheet
		 */
		if(settingsSheet != null && settingsHeader != null) {
			Row row = settingsSheet.getRow(rowNumSettings++);
			if(row != null) {
				int lastCellNum = row.getLastCellNum();

				// Default language
				survey.def_lang = XLSUtilities.getTextColumn(row, "default_language", settingsHeader, lastCellNum, null);
				if(survey.def_lang != null) {
					boolean validLanguage = false;
					for(Language l : survey.languages) {
						if(l.name.equals(survey.def_lang)) {
							validLanguage = true;
							break;
						}
					}
					if(!validLanguage) {
						throw new ApplicationException(localisation.getString("tu_idl"));
					}
				}

				survey.instanceNameDefn = XLSUtilities.getTextColumn(row, "instance_name", settingsHeader, lastCellNum, null);
				survey.surveyClass = XLSUtilities.getTextColumn(row, "style", settingsHeader, lastCellNum, null);
				survey.task_file = getBooleanColumn(row, "allow_import", settingsHeader, lastCellNum, false);
				survey.setHideOnDevice(getBooleanColumn(row, "hide_on_device", settingsHeader, lastCellNum, false));
				survey.setSearchLocalData(getBooleanColumn(row, "search_local_data", settingsHeader, lastCellNum, false));
				survey.dataSurvey = getBooleanColumn(row, "data_survey", settingsHeader, lastCellNum, true);
				survey.oversightSurvey = getBooleanColumn(row, "oversight_survey", settingsHeader, lastCellNum, true);
				survey.readOnlySurvey = getBooleanColumn(row, "read_only_survey", settingsHeader, lastCellNum, true);
				
				survey.timing_data = getBooleanColumn(row, "timing_data", settingsHeader, lastCellNum, false);
				survey.audit_location_data = getBooleanColumn(row, "audit_location_data", settingsHeader, lastCellNum, false);
				survey.track_changes = getBooleanColumn(row, "track_changes", settingsHeader, lastCellNum, false);
				survey.compress_pdf = getBooleanColumn(row, "compress_pdf", settingsHeader, lastCellNum, false);

				survey.hrk = XLSUtilities.getTextColumn(row, "key", settingsHeader, lastCellNum, null);
				
				survey.key_policy = XLSUtilities.getTextColumn(row, "key_policy", settingsHeader, lastCellNum, null);
				if(survey.key_policy == null) {
					survey.key_policy = SurveyManager.KP_NONE;
				}
				if(!SurveyManager.isValidSurveyKeyPolicy(survey.key_policy)) {
					String msg = localisation.getString("tu_inv_kp");
					msg = msg.replace("%s1", survey.key_policy);
					throw new ApplicationException(msg);
				}
				survey.autoTranslate = getBooleanColumn(row, "auto_translate", settingsHeader, lastCellNum, false);
				survey.default_logo = XLSUtilities.getTextColumn(row, "report_logo", settingsHeader, lastCellNum, null);
				
				String pdRepeats = XLSUtilities.getTextColumn(row, "pulldata_repeat", settingsHeader, lastCellNum, null);
				if(pdRepeats != null) {
					String [] pdArray = pdRepeats.split(":");
					if(pdArray.length > 0) {
						for(String pd : pdArray) {
							pd = pd.trim();
							int idx = pd.indexOf("(");
							if(idx > 0) {
								String sName = pd.substring(0, idx);
								String key = pd.substring(idx + 1, pd.length() - 1);
								if(survey.pulldata == null) {
									survey.pulldata = new ArrayList<Pulldata> ();
								}
								survey.pulldata.add(new Pulldata(sName, key));
							}

						}
					}
				}
				
				// Add row filters
				if(rowRoleHeader != null && rowRoleHeader.size() > 0) {
					for(String h : rowRoleHeader.keySet()) {
						String filter = XLSUtilities.getTextColumn(row, h, settingsHeader, lastCellNum, null);
						if(filter != null) {
							Role r = survey.roles.get(h);
							if(r != null) {
								SqlFrag sq = new SqlFrag();
								sq.addSqlFragment(filter, false, localisation, 0);
								settingsQuestionInSurvey(sq.humanNames, h);		// validate question names
								r.row_filter = filter;
							}
						}
					}
				}
			}
		}
		
		/*
		 * Add default preloads
		 */
		if(!hasMeta("start")) {
			survey.meta.add(new MetaItem(metaId--, "dateTime", "_start", "start", "_start", "timestamp", true, "start", null));
		}
		if(!hasMeta("end")) {
			survey.meta.add(new MetaItem(metaId--, "dateTime", "_end", "end", "_end", "timestamp", true, "end", null));
		}
		if(!hasMeta("deviceid")) {
			survey.meta.add(new MetaItem(metaId--, "string", "_device", "deviceid", "_device", "property", true, "device", null));
		}
		
		validateSurvey();	// 4. Final Validation

		return survey;

	}
	
	private boolean hasMeta(String sourceParam) {
		for(MetaItem mi : survey.meta) {
			if(mi.isPreload && mi.sourceParam.equals(sourceParam)) {
				return true;
			}
		}
		return false;
	}

	private Option getOption(Row row, String listName) throws ApplicationException, Exception {

		Option o = new Option();
		int lastCellNum = row.getLastCellNum();
		o.optionList = listName;

		o.value = XLSUtilities.getTextColumn(row, "name", choicesHeader, lastCellNum, null);
		o.display_name = XLSUtilities.getTextColumn(row, "display_name", choicesHeader, lastCellNum, null);
		getLabels(row, lastCellNum, choicesHeader, o.labels, "choice", true);
		
		if(merge) {
			// Attempt to get existing column name
			String n = optionNames.get(listName + "__" + o.value);
			if(n != null) {
				o.columnName = n;
			} else {
				o.columnName = GeneralUtilityMethods.cleanName(o.value, false, false, false);
			}
		} else {
			o.columnName = GeneralUtilityMethods.cleanName(o.value, false, false, false);
		}
		o.cascade_filters = new HashMap<String, String> ();  
		for(String key : choiceFilterHeader.keySet()) {
			String value = XLSUtilities.getTextColumn(row, key, choicesHeader, lastCellNum, null);
			if (value != null) {
				o.cascade_filters.put(key, value);
			}
		}

		o.published = false;		// Default to unpublished TODO work out when this can be set to published
		validateOption(o, rowNumChoices);

		return o;
	}
	
	private TableColumnMarkup getStyle(Row row, String styleList) throws ApplicationException, Exception {

		int lastCellNum = row.getLastCellNum();

		String name = XLSUtilities.getTextColumn(row, "value", stylesHeader, lastCellNum, null);
		String color = XLSUtilities.getTextColumn(row, "color", stylesHeader, lastCellNum, null);
		
		TableColumnMarkup s = new TableColumnMarkup(name, color);
		validateStyle(s, rowNumStyles, styleList);

		return s;
	}

	/*
	 * Get the survey header and the choices header so we can identify all the languages up front
	 * This should work gracefully with a badly designed forms where there are inconsistent language
	 *  names between the survey and choices sheet
	 */
	private void getHeaders() throws ApplicationException {

		choiceFilterHeader = new HashMap<String, Integer> ();		
		HashMap<String, String> langMap = new HashMap<String, String> ();

		// Get survey sheet headers
		while(rowNumSurvey <= lastRowNumSurvey) {
			Row row = surveySheet.getRow(rowNumSurvey++);
			if(row != null) {
				surveyHeader = XLSUtilities.getHeader(row, localisation, rowNumSurvey, "survey");

				// Add languages in order they exist in the header hence won't use keyset of surveyHeader
				int lastCellNum = row.getLastCellNum();							
				for(int i = 0; i <= lastCellNum; i++) {
					Cell cell = row.getCell(i);
					if(cell != null) {
						String name = cell.getStringCellValue().trim();
						if(name.startsWith("label::")) {	 // Only check the question label for languages, any others will be assumed to be errors
							String [] sArray = name.split("::");
							if(sArray.length > 0) {
								if(sArray.length == 1) {
									String msg = localisation.getString("tu_lnm");
									msg = msg.replace("%s1", name);
									msg = msg.replace("%s2", name);
									throw new ApplicationException(msg);
								}
								String exists = langMap.get(sArray[1]);
								if(exists == null) {
									langMap.put(sArray[1], sArray[1]);
									survey.languages.add(new Language(0, sArray[1],
											GeneralUtilityMethods.getLanguageCode(sArray[1]),
											GeneralUtilityMethods.isRtl(sArray[1])));
								}
							}
						}
					}
				}
				
				// Get security roles
				for(String h : surveyHeader.keySet()) {
					if(h.startsWith("role::")) {
						if(columnRoleHeader == null) {
							columnRoleHeader = new HashMap<String, Integer> ();
						}
						columnRoleHeader.put(h, surveyHeader.get(h));
						String [] roleA = h.split("::");
						if(roleA.length > 1) {
							survey.roles.put(h, new Role(roleA[1]));
						}
					}
				}

				break;
			}
		}

		// Get choice sheet header
		if(choicesSheet != null) {
			while(rowNumChoices <= lastRowNumChoices) {
				Row row = choicesSheet.getRow(rowNumChoices++);
				if(row != null) {
					choicesHeader = XLSUtilities.getHeader(row, localisation, rowNumChoices, "choices");	
					
					// Get the headers for filters
					for(String h : choicesHeader.keySet()) {
						h = h.trim();
						/*
						 * Languages can contain spaces so this check is wrong
						 *
						if(h.contains(" ")) {
							String msg = localisation.getString("tu_invf");
							msg = msg.replace("%s1", h);
							throw new ApplicationException(msg);
						}
						*/
						if(h.equals("list_name")
								|| h.equals("name")
								|| h.equals("label")
								|| h.equals("display_name")
								|| h.startsWith("label::") 
								|| h.equals("image")
								|| h.startsWith("image::")  		// deprecate?
								|| h.startsWith("media::image") 
								|| h.equals("audio")
								|| h.startsWith("audio::") 		// deprecate?
								|| h.startsWith("media::audio") 
								|| h.equals("video") 
								|| h.startsWith("media::video")
								|| h.startsWith("video::")) { 	// deprecate?
							continue;
						}
						// The rest must be filter columns
						choiceFilterHeader.put(h, choicesHeader.get(h));
					}
				
					break;
				}
			}
		}

		// Add a default language if needed
		if(survey.languages.size() == 0) {
			survey.languages.add(new Language(0, "language", null, false));
			useDefaultLanguage = true;
		}
		
		// Get Setting sheet headers
		if(settingsSheet != null) {
			while(rowNumSettings <= lastRowNumSettings) {
				Row row = settingsSheet.getRow(rowNumSettings++);
				if(row != null) {
					settingsHeader = XLSUtilities.getHeader(row, localisation, rowNumSettings, "settings");
					break;
				}
			}
			
			// Add security roles
			if(settingsHeader != null) {
				for(String h : settingsHeader.keySet()) {
					if(h.startsWith("role::")) {
						if(rowRoleHeader == null) {
							rowRoleHeader = new HashMap<String, Integer> ();
						}
						rowRoleHeader.put(h, settingsHeader.get(h));
						String [] roleA = h.split("::");
						if(roleA.length > 1) {
							survey.roles.put(h, new Role(roleA[1]));
						}
					}
				}
			}
		}

	}
	
	/*
	 * Get the headers for the styles sheet
	 */
	private void getStyleHeaders(Sheet styleSheet) throws ApplicationException {
		
		// Get Style sheet headers
		if(styleSheet != null) {
			while(rowNumStyles <= lastRowNumStyles) {
				Row row = styleSheet.getRow(rowNumStyles++);
				if(row != null) {
					stylesHeader = XLSUtilities.getHeader(row, localisation, rowNumStyles, "styles");
					break;
				}
			}
		}
	}
	
	/*
	 * Get the headers for the condition sheet
	 */
	private void getConditionHeaders(Sheet sheet) throws ApplicationException {
		
		// Get Style sheet headers
		if(sheet != null) {
			while(rowNumConditions <= lastRowNumConditions) {
				Row row = sheet.getRow(rowNumConditions++);
				if(row != null) {
					conditionsHeader = XLSUtilities.getHeader(row, localisation, rowNumConditions, "conditions");
					break;
				}
			}
		}

	}

	/*
	 * Process the question rows to create a form
	 */
	private Form getForm(String name, int parentFormIndex, int parentQuestionIndex, ArrayList<KeyValueSimp>  parameters) throws Exception {

		Form f = new Form(name, parentFormIndex, parentQuestionIndex);
		setFormReference(parameters, f);
		setFormMerge(parameters, f);
		survey.forms.add(f);
		
		int thisFormIndex = survey.forms.size() - 1;
		boolean inMatrix = false;
		MatrixWidget matrix = null;
		
		while(rowNumSurvey <= lastRowNumSurvey) {

			Row row = surveySheet.getRow(rowNumSurvey++);

			if(row != null) {
				Question q = getQuestion(row, thisFormIndex, f.questions.size(), rowNumSurvey);				
				if(q != null) {
					MetaItem item = GeneralUtilityMethods.getPreloadItem(q.type, q.name, q.display_name, metaId, q.appearance, q.paramArray);
					if(item != null) {
						metaId--;
						validateQuestion(q, rowNumSurvey, thisFormIndex);
						survey.meta.add(item);
					} else {
						if(q.type.equals("end repeat")) {
							if(parentFormIndex < 0) {
								throw XLSUtilities.getApplicationException(localisation, "tu_eer", rowNumSurvey, "survey", null, null, null);
							}
							return f; 
						}
							
						// Update the survey manifest if csv files are referenced from the appearance and/or the calculation
						ManifestInfo mi = GeneralUtilityMethods.addManifestFromAppearance(q.appearance, survey.manifest);
						mi = GeneralUtilityMethods.addManifestFromCalculate(q.calculation, mi.manifest);
						survey.manifest = mi.manifest;
						
						validateQuestion(q, rowNumSurvey, thisFormIndex);
						
						if(q.type.equals("begin matrix")) {
							matrix = new MatrixWidget(q, rowNumSurvey);
							inMatrix = true;
						} else if(q.type.equals("end matrix")) {
							// add all questions from matrix object
							for(Question qm : matrix.getExpanded()) {
								f.questions.add(qm);
							}
							inMatrix = false;
						} else {
							if(!inMatrix) {
								f.questions.add(q);
							} else {
								matrix.addMember(q);
							}
						}						
						
						if(q.type.equals("begin repeat")) {
							int repeatRowNumber = rowNumSurvey;
							Form subForm = getForm(q.name, thisFormIndex, f.questions.size() - 1, q.paramArray);
							validateForm(repeatRowNumber, subForm);
						}
						
					}
				}
						
			}
			
		}
		
		if(parentFormIndex >= 0) {
			throw XLSUtilities.getApplicationException(localisation, "tu_mer", rowNumSurvey, "survey", name, null, null);
		}
		return f;

	}

	/*
	 * Get a question from the excel sheet
	 */
	private Question getQuestion(Row row, int formIndex, 
			int questionIndex, int rowNum) throws ApplicationException, Exception {

		Question q = new Question();
		int lastCellNum = row.getLastCellNum();

		// 1. Question type
		String type = XLSUtilities.getTextColumn(row, "type", surveyHeader, lastCellNum, null);	
		
		// 2. Question name
		q.name = XLSUtilities.getTextColumn(row, "name", surveyHeader, lastCellNum, null);  
		
		// Check type is not null
		if(type == null && q.name != null ) {
			throw XLSUtilities.getApplicationException(localisation, "tu_mt", rowNumSurvey, "survey", null, null, null);
		} else if(type == null && q.name == null) {
			return null;		// blank row
		}
		
		q.type = convertType(type, q);			
		
		// Get the list name from the list_name column if it has not already been set
		if(q.list_name == null) {
			q.list_name = XLSUtilities.getTextColumn(row, "list_name", surveyHeader, lastCellNum, null); 
		}
		
		// 3. Labels
		getLabels(row, lastCellNum, surveyHeader, q.labels, q.type, false);	
		
		if(merge) {
			QuestionForm qt = questionNames.get(q.name);
			if(qt != null) {
				q.columnName = qt.columnName;
			} else {
				q.columnName = GeneralUtilityMethods.cleanName(q.name, true, true, true);
			}
		} else {
			q.columnName = GeneralUtilityMethods.cleanName(q.name, true, true, true);
		}	

		// display name
		q.display_name = XLSUtilities.getTextColumn(row, "display_name", surveyHeader, lastCellNum, null); 
		
		// 4. choice filter
		q.choice_filter = XLSUtilities.getTextColumn(row, "choice_filter", surveyHeader, lastCellNum, null);
		q.choice_filter = GeneralUtilityMethods.cleanXlsNames(q.choice_filter);
		
		// 5. Constraint
		q.constraint = XLSUtilities.getTextColumn(row, "constraint", surveyHeader, lastCellNum, null);  
		q.constraint = GeneralUtilityMethods.cleanXlsNames(q.constraint);
		
		// 6. Constraint message
		q.constraint_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.CONSTRAINT_MESSAGE, surveyHeader, lastCellNum, null); 
		if(q.constraint_msg == null) {
			q.constraint_msg = XLSUtilities.getTextColumn(row, "constraint-msg", surveyHeader, lastCellNum, null);   // as used by enketo
		}
		
		// 7. Relevant
		q.relevant = XLSUtilities.getTextColumn(row, "relevant", surveyHeader, lastCellNum, null);  
		q.relevant = GeneralUtilityMethods.cleanXlsNames(q.relevant);

		// 7. Repeat count
		if(q.type.equals("begin repeat")) {
			q.repeatCount = XLSUtilities.getTextColumn(row, "repeat_count", surveyHeader, lastCellNum, null);  
		}
		
		// 8. Default handles both dynamic and static defaults
		String def = XLSUtilities.getTextColumn(row, "default", surveyHeader, lastCellNum, null); 
		def = GeneralUtilityMethods.cleanXlsNames(def);
		if(GeneralUtilityMethods.isSetValue(def)) {
			// Set Value
			q.defaultanswer = null;
			q.addSetValue(SetValue.START, def, null);
		} else {
			q.defaultanswer = def;
		}
		
		// 9. Readonly
		q.readonly = getBooleanColumn(row, "readonly", surveyHeader, lastCellNum, false);
		
		// 10. Appearance
		q.appearance = XLSUtilities.getTextColumn(row, "appearance", surveyHeader, lastCellNum, null); 
		q.appearance = GeneralUtilityMethods.cleanXlsNames(q.appearance);
		
		// 11. Parameters
		String paramString = XLSUtilities.getTextColumn(row, "parameters", surveyHeader, lastCellNum, null);
		q.paramArray = GeneralUtilityMethods.convertParametersToArray(paramString);
		
		// 12. autoplay
		q.autoplay = XLSUtilities.getTextColumn(row, "autoplay", surveyHeader, lastCellNum, null);
		
		// 13. body::accuracyThreshold
		q.accuracy = XLSUtilities.getTextColumn(row, "body::accuracyThreshold", surveyHeader, lastCellNum, null);
		
		// 14. Required
		q.required = getBooleanColumn(row, "required", surveyHeader, lastCellNum, false);		
		
		// 15. Required Message
		q.required_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.REQUIRED_MESSAGE, surveyHeader, lastCellNum, null); 
		
		// 16. Calculation
		if(q.type.equals("server_calculate")) {
			String serverCalculation = XLSUtilities.getTextColumn(row, "server_calculation", surveyHeader, lastCellNum, null);
			if(serverCalculation != null) {
				SqlFrag testCalc = new SqlFrag();
				serverCalculation = serverCalculation.trim();
				
				q.server_calculation = new ServerCalculation();
				q.server_calculation.addExpression(serverCalculation);
				if(serverCalculation.startsWith("if(")) {			
					ServerCalculation sc = survey.serverCalculations.get(q.name);
					if(sc != null) {
						q.server_calculation.addAllConditions(sc.getConditions());
					}
				} else {		// Validate the expression as an sql fragment	
					testCalc.addSqlFragment(serverCalculation, true, localisation, rowNum);
				}
			}
		} else {
			q.calculation = XLSUtilities.getTextColumn(row, "calculation", surveyHeader, lastCellNum, null); 
			q.calculation = GeneralUtilityMethods.cleanXlsNames(q.calculation);
		}
		
		// 17. Display Name
		q.display_name = XLSUtilities.getTextColumn(row, "display_name", surveyHeader, lastCellNum, null); 
		
		// 18. Compressed
		q.compressed = true;
		
		// 19. body::intent
		q.intent = XLSUtilities.getTextColumn(row, "body::intent", surveyHeader, lastCellNum, null);
		
		// 20. Style
		q.style_list = XLSUtilities.getTextColumn(row, "style_list", surveyHeader, lastCellNum, null); 
		if(q.style_list == null) {
			q.style_list = XLSUtilities.getTextColumn(row, "style list", surveyHeader, lastCellNum, null);
		}
		
		// 21. Literacy Flash Interval
		String flashInterval = XLSUtilities.getTextColumn(row, "body::kb:flash", surveyHeader, lastCellNum, null); 
		if(flashInterval != null) {
			try {
				q.flash = Integer.valueOf(flashInterval);
			} catch (Exception e) {
				throw XLSUtilities.getApplicationException(localisation, "tu_if", rowNumSurvey, "survey", null, null, null);
			}
		}
		
		// 22. Trigger
		q.trigger = XLSUtilities.getTextColumn(row, "trigger", surveyHeader, lastCellNum, null); 
		
		// Add Column Roles
		if(columnRoleHeader != null && columnRoleHeader.size() > 0) {
			for(String h : columnRoleHeader.keySet()) {
				if(getBooleanColumn(row, h, surveyHeader, lastCellNum, false)) {
					Role r = survey.roles.get(h);
					if(r != null) {
						if(r.column_filter_ref == null) {
							r.column_filter_ref = new ArrayList<RoleColumnFilterRef> ();
						}
						r.column_filter_ref.add(new RoleColumnFilterRef(formIndex, questionIndex));
					}
				}
			}
		}
			
		/*
		 * Handle Groups
		 */
		if(q.type.equals("begin group") || q.type.equals("begin matrix")) {
			Stack<Question> groupStack = getGroupStack(formIndex);
			groupStack.push(q);
			if(q.appearance != null && q.appearance.contains("table-list")) {
				inTableListGroup = true;
				foundSelectInTableListGroup = false;
				justStartedTableListGroup = true;
			} else {
				inTableListGroup = false;
			}
		}
		if(q.type.equals("end group") || q.type.equals("end matrix")) {
			Stack<Question> groupStack = getGroupStack(formIndex);
			if(groupStack.isEmpty()) { 
				Form f = survey.forms.get(formIndex);
				throw XLSUtilities.getApplicationException(localisation, "tu_eegm", rowNumSurvey, "survey", f.name, null, null);
			}
			
			Question currentGroupQuestion = groupStack.pop();
			
			if(inTableListGroup && !foundSelectInTableListGroup) {
				throw XLSUtilities.getApplicationException(localisation, "tu_need_s", rowNumSurvey, "survey", currentGroupQuestion.name, null, null);
			}
			inTableListGroup = false;
			
			if(q.name != null && q.name.trim().length() > 0 && !q.name.endsWith("_groupEnd")) {  // ignore end groups that end with _groupEnd as they were generated by old xls exports
				// Validate the provided group name against the current group
				if(!q.name.equals(currentGroupQuestion.name)) {
					throw XLSUtilities.getApplicationException(localisation, "tu_eeg", rowNumSurvey, "survey", q.name, currentGroupQuestion.name, null);
				}			
			} else {
				// Set the name of the end group to its group
				q.name = currentGroupQuestion.name;
			}
		}
		
		/*
		 * Validate questions inside table list group
		 */
		if(inTableListGroup) {
			if(!justStartedTableListGroup) {
				if(!q.type.startsWith("select")) {
					throw XLSUtilities.getApplicationException(localisation, "tu_ns", rowNumSurvey, "survey", q.type, null, null);
				} else {
					foundSelectInTableListGroup = true;
				}
			}
			justStartedTableListGroup = false;
		}
		
		/*
		 * Derived Values
		 */
		// 1. Source
		if(q.type.equals("begin group") 
				|| q.type.equals("end group") 
				|| q.type.equals("begin matrix") 
				|| q.type.equals("end matrix") 
				|| q.type.equals("server_calculate") 
				|| q.type.equals("pdf_field") 
				|| q.type.equals("begin repeat")) {
			q.source = null;
		} else {
			q.source = "user";
		}
		
		// 2. Visibility
		q.visible = convertVisible(q.type);

		/*
		 * Validate question type compatability
		 */
		if(merge) {
			QuestionForm qt = questionNames.get(q.name);
			if(qt != null) {
				if(q.source != null && qt.published) {
					String newColType = GeneralUtilityMethods.getPostgresColType(q.type, false);
					String oldColType = GeneralUtilityMethods.getPostgresColType(qt.qType, false);
					if(!newColType.equals(oldColType) &&
							!newColType.equals("end repeat") &&	// Not sure why these are needed as source type should be null
							!oldColType.equals("end repeat")&& 
							!newColType.equals("end group") &&
							!oldColType.equals("end group")
							) {
						throw XLSUtilities.getApplicationException(localisation, "tu_it", rowNumSurvey, "survey", q.name, q.type, qt.qType);
					}
				}
			} 
		} 	
		
		return q;
	}

	/*
	 * For media try under the default column heading if the language specific is null
	 */
	private void getLabels(Row row, int lastCellNum, HashMap<String, Integer> header, ArrayList<Label> labels, 
			String type, boolean choiceSheet) throws ApplicationException, Exception {
		
		// Get the label language values
		String defaultLabel = getDefaultLabel(type);
		if(useDefaultLanguage) {
			Label lab = new Label();
			lab.text = XLSUtilities.getTextColumn(row, "label", header, lastCellNum, defaultLabel);
			
			if(!choiceSheet) {
				lab.hint = XLSUtilities.getTextColumn(row, "hint", header, lastCellNum, null);
				lab.guidance_hint = XLSUtilities.getTextColumn(row, "guidance_hint", header, lastCellNum, null);
				lab.constraint_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.CONSTRAINT_MESSAGE, header, lastCellNum, null);
				lab.required_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.REQUIRED_MESSAGE, header, lastCellNum, null);
			}
			
			lab.image = XLSUtilities.getTextColumn(row, "image", header, lastCellNum, null);
			if(lab.image == null) {
				lab.image = XLSUtilities.getTextColumn(row, "media::image", header, lastCellNum, null);
			}
			lab.video = XLSUtilities.getTextColumn(row, "video", header, lastCellNum, null);
			if(lab.video == null) {
				lab.video = XLSUtilities.getTextColumn(row, "media::video", header, lastCellNum, null);
			}
			lab.audio = XLSUtilities.getTextColumn(row, "audio", header, lastCellNum, null);
			if(lab.audio == null) {
				lab.audio = XLSUtilities.getTextColumn(row, "media::audio", header, lastCellNum, null);
			}
			
			lab.text = GeneralUtilityMethods.cleanXlsNames(lab.text);
			labels.add(lab);
		} else {
			
			// Find out if any language has a hint or label.  If so make sure every language does
			boolean hintSet = false;
			boolean guidanceHintSet = false;
			boolean constraintMsgSet = false;
			boolean requiredMsgSet = false;
			for(int i = 0; i < survey.languages.size(); i++) {
				String lang = survey.languages.get(i).name;
				if(!choiceSheet) {
					if(XLSUtilities.getTextColumn(row, "hint::" + lang, header, lastCellNum, null) != null) {
						hintSet = true;
					}
					if(XLSUtilities.getTextColumn(row, "guidance_hint::" + lang, header, lastCellNum, null) != null) {
						guidanceHintSet = true;
					}
					if(XLSUtilities.getTextColumn(row, XLSFormColumns.CONSTRAINT_MESSAGE + "::" + lang, header, lastCellNum, null) != null) {
						constraintMsgSet = true;
					}
					if(XLSUtilities.getTextColumn(row, XLSFormColumns.REQUIRED_MESSAGE + "::" + lang, header, lastCellNum, null) != null) {
						requiredMsgSet = true;
					}
				}
				
			}
			for(int i = 0; i < survey.languages.size(); i++) {
				String lang = survey.languages.get(i).name;
				
				Label lab = new Label();
				lab.text = XLSUtilities.getTextColumn(row, "label::" + lang, header, lastCellNum, "-");
	
				
				if(hintSet) {
					lab.hint = XLSUtilities.getTextColumn(row, "hint::" + lang, header, lastCellNum, "-");
				} else {
					lab.hint = XLSUtilities.getTextColumn(row, "hint::" + lang, header, lastCellNum, null);
				}
				
				if(guidanceHintSet) {
					lab.guidance_hint = XLSUtilities.getTextColumn(row, "guidance_hint::" + lang, header, lastCellNum, "-");
				} else {
					lab.guidance_hint = XLSUtilities.getTextColumn(row, "guidance_hint::" + lang, header, lastCellNum, null);
				}
				
				// Constraint message 
				if(constraintMsgSet) {
					lab.constraint_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.CONSTRAINT_MESSAGE + "::" + lang, header, lastCellNum, "-");
				} else {
					lab.constraint_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.CONSTRAINT_MESSAGE + "::" + lang, header, lastCellNum, null);
				}
				if(lab.constraint_msg == null) {	// use the universal setting
					lab.constraint_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.CONSTRAINT_MESSAGE, header, lastCellNum, null);
				}
				
				// Required message
				if(requiredMsgSet) {
					lab.required_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.REQUIRED_MESSAGE + "::" + lang, header, lastCellNum, "-");
				} else {
					lab.required_msg = XLSUtilities.getTextColumn(row, XLSFormColumns.REQUIRED_MESSAGE + "::" + lang, header, lastCellNum, null);
				}
			
				// image - try various combination of headers
				lab.image = XLSUtilities.getTextColumn(row, "media::image::" + lang, header, lastCellNum, null);
				if(lab.image == null) {
					lab.image = XLSUtilities.getTextColumn(row, "image::" + lang, header, lastCellNum, null);
				}
				if(lab.image == null) {
					lab.image = XLSUtilities.getTextColumn(row, "media::image", header, lastCellNum, null);
				}
				if(lab.image == null) {
					lab.image = XLSUtilities.getTextColumn(row, "image", header, lastCellNum, null);
				}
				
				// video - try various combination of headers
				lab.video = XLSUtilities.getTextColumn(row, "media::video::" + lang, header, lastCellNum, null);
				if(lab.video == null) {
					lab.video = XLSUtilities.getTextColumn(row, "video::" + lang, header, lastCellNum, null);
				}
				if(lab.video == null) {
					lab.video = XLSUtilities.getTextColumn(row, "media::video", header, lastCellNum, null);
				}
				if(lab.video == null) {
					lab.video = XLSUtilities.getTextColumn(row, "video", header, lastCellNum, null);
				}
				
				// video - try various combination of headers
				lab.audio = XLSUtilities.getTextColumn(row, "media::audio::" + lang, header, lastCellNum, null);
				if(lab.audio == null) {
					lab.audio = XLSUtilities.getTextColumn(row, "audio::" + lang, header, lastCellNum, null);
				}
				if(lab.audio == null) {
					lab.audio = XLSUtilities.getTextColumn(row, "media::audio", header, lastCellNum, null);
				}
				if(lab.audio == null) {
					lab.audio = XLSUtilities.getTextColumn(row, "audio", header, lastCellNum, null);
				}
				
				lab.text = GeneralUtilityMethods.cleanXlsNames(lab.text);
				labels.add(lab);
			}
		}

	}

	private String getDefaultLabel(String type) {
		String def = "-";
		if (type.equals("begin group") || type.equals("end group")
				|| type.equals("begin matrix") || type.equals("end matrix")
				|| type.equals("begin repeat") || type.equals("end repeat")) {
			def = null;
		}
		return def;
	}
	
	private String convertType(String in, Question q) throws ApplicationException {

		String type = getValidQuestionType(in);		
		
		// Validate and normalise input
		if(type == null) {
			throw XLSUtilities.getApplicationException(localisation, "tu_ut", rowNumSurvey, "survey", in, null, null);
		}

		// Do type conversions
		if (type.equals("text")) {
			type = "string";
		} else if(type.startsWith("select_one") || type.startsWith("select_multiple") || type.startsWith("rank")) {
			
			String [] array = type.split("\\s+");
			if(array.length <= 1) {
				throw XLSUtilities.getApplicationException(localisation, "tu_mln", rowNumSurvey, "survey", in.trim(), null, null);
			}
			q.list_name = array[1].trim();
			if(q.list_name.length() == 0) {
				q.list_name = null;
			}
			
			if(type.startsWith("select_one")) {
				type = "select1";
			} else if(type.startsWith("select_multiple")) {
				type = "select";
			} else if(type.startsWith("rank")) {
				type = "rank";
			}
		} 

		return type;
	}

	private boolean convertVisible(String type) throws Exception {
		boolean visible = true;
		if(type.equals("calculate") || type.equals("server_calculate") || type.equals("pdf_field")) {
			visible = false;
		} else if(type.equals("end group")) {
			visible = false;
		} else if(GeneralUtilityMethods.getPreloadItem(type, "", "", -2000, null, null) != null) {
			visible = false;
		}

		return visible;
	}

	private void validateForm(int rowNumber, Form f) throws Exception {
		
		if(f.questions.size() == 0) {
			// Form must have at least one question
			throw XLSUtilities.getApplicationException(localisation, "tu_er", rowNumber, "survey", null, null, null);
		} else {
			// Questions must be visible
			boolean hasVisibleQuestion = false;
			for(Question qx : f.questions) {
				if(!qx.type.equals("calculate")) {
					hasVisibleQuestion = true;
					break;
				}
			}
			
			if(!hasVisibleQuestion) {
				ApplicationException e = XLSUtilities.getApplicationException(localisation, "tu_er", rowNumber, "survey", null, null, null);
				warnings.add(new ApplicationWarning(e.getMessage()));
			}
		}
		
		/*
		 * Validate groups
		 */
		for(int i = 0; i < f.questions.size(); i++) {
			Question q = f.questions.get(i);
			if(q.type.equals("begin group")) {
				validateGroup(f.questions, q, i);
			}
		}
		
	}
	
	private int validateGroup(ArrayList<Question> questions, Question groupQuestion, int start) throws ApplicationException {
		
		Question q;
		String name = groupQuestion.name;
		int i;
		boolean hasVisibleQuestion = false;
		for(i = start + 1; i < questions.size(); i++) {
			q = questions.get(i);
			if(q.type.equals("begin group")) {
				hasVisibleQuestion = true;		// Count another group as a visible question, as long as this embedded group has a visible question then all is good
				validateGroup(questions, q, i);	// recursive validation
			} else if(q.type.equals("end group")) {
				break;
			} else if(!q.type.equals("calculate")) {
				hasVisibleQuestion = true;
			}
		}
		
		if(!hasVisibleQuestion) {
			Integer rowNumber = qNameMap.get(name);
			ApplicationException e = XLSUtilities.getApplicationException(localisation, "tu_er", rowNumber, "survey", null, null, null);
			warnings.add(new ApplicationWarning(e.getMessage()));
		}
		return i + 1;
	}
	
	private void validateQuestion(Question q, int rowNumber, int formIndex) throws Exception {

		/*
		 * Check Name
		 */
		if (q.name == null || q.name.trim().length() == 0) {
			// Check for a missing name
			throw XLSUtilities.getApplicationException(localisation, "tu_mn", rowNumber, "survey", null, null, null);

		} else if(!validQname.matcher(q.name).matches()) {
			// Check for a valid name
			throw XLSUtilities.getApplicationException(localisation, "tu_qn", rowNumber, "survey", q.name, null, null);

		} else if(!q.type.equals("end group") && !q.type.equals("end matrix") && qNameMapCaseInsensitive.get(q.columnName) != null) {
			// Check for a duplicate name
			// Temporary - allow multiple question names of the_geom
			if(!q.name.equals("the_geom")) {
				throw XLSUtilities.getApplicationException(localisation, "tu_dq", rowNumber, "survey", q.name, null, null);
			}
		}
		
		if(!q.type.equals("end group") && !q.type.equals("end matrix")) {		
			qNameMap.put(q.name, rowNumber);
			qNameMapCaseInsensitive.put(q.columnName, rowNumber);
		}
		
		// check relevance
		if(q.relevant != null) {
			ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.relevant);
			if(refs.contains(q.name)) {		// Circular references
				throw XLSUtilities.getApplicationException(localisation, "tu_cr", rowNumber, "survey", "relevant", q.name, q.relevant);
			}
			
			checkParentheses(localisation, q.relevant, rowNumber, "survey", "relevant", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.relevant));
			} catch (Exception e) {
				throw XLSUtilities.getApplicationException(localisation, "tu_jr", rowNumber, "survey", "relevant", e.getMessage(), null);
			}
			testXExprFunctions(q.relevant, localisation, true, rowNumber, "relevant");
		}
		
		// check constraint
		if(q.constraint != null) {
			checkParentheses(localisation, q.constraint, rowNumber, "survey", "constraint", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.constraint));
			} catch (Exception e) {
				throw XLSUtilities.getApplicationException(localisation, "tu_jr", rowNumber, "survey", "constraint", e.getMessage(), null);
			}
			testXExprFunctions(q.constraint, localisation, true, rowNumber, "constraint");
		}
		
		// check calculate
		if(q.calculation != null) {
			checkParentheses(localisation, q.calculation, rowNumber, "survey", "calculation", q.name);
			checkCalculationCircularReferences(localisation, q.calculation, rowNumber, "survey", "calculation", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.calculation));
			} catch (Exception e) {
				throw XLSUtilities.getApplicationException(localisation, "tu_jr", rowNumber, "survey", "calculation", e.getMessage(), null);
			}
			testXExprFunctions(q.calculation, localisation, true, rowNumber, "calculation");
		}
		
		// check appearance
		if(q.appearance != null) {
			checkParentheses(localisation, q.appearance, rowNumber, "survey", "appearance", q.name);
			testXExprFunctions(q.appearance, localisation, true, rowNumber, "appearance");		
		}
		
		// Check choice filter
		if(q.choice_filter != null) {
			checkParentheses(localisation, q.choice_filter, rowNumber, "survey", "choice_filter", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.choice_filter));
			} catch (Exception e) {
				throw XLSUtilities.getApplicationException(localisation, "tu_jr", rowNumber, "survey", "choice_filter", e.getMessage(), null);
			}
			testXExprFunctions(q.choice_filter, localisation, true, rowNumber, "choice_filter");
		}
		
		// Check intent
		if(q.intent != null ) {
			boolean valid = false;
			if(q.type.equals("begin group")) {
				if(q.appearance != null && q.appearance.contains("field-list")) {
					valid = true;
				}
			}
			if(!valid) {
				throw XLSUtilities.getApplicationException(localisation, "tu_int", rowNumber, "survey", null, null, null);
			}
		}
		
		// invalid question in field-list
		if(inFieldList) {
			if(q.type.equals("end group")) {
				inFieldList = false;
			//} else if(q.type.equals("begin group") || q.type.equals("begin repeat")) {
			} else if(q.type.equals("begin repeat")) {		// Tentatively should be ok to have a group inside a fieldlist group
				Stack<Question> groupStack = getGroupStack(formIndex);
				String groupName = groupStack.pop().name;
				throw XLSUtilities.getApplicationException(localisation, "tu_fl", rowNumber, "survey", q.type, groupName, null);
			}
		} else {
			if(q.type.equals("begin group")) {
				if(q.appearance != null && q.appearance.contains("field-list")) {
					inFieldList = true;
				}
			}
		}
		
		// List name not in choices
		if(q.list_name != null  && !q.list_name.startsWith("$")) {
			if(survey.optionLists.get(q.list_name) == null) {
				throw XLSUtilities.getApplicationException(localisation, "tu_lnf", rowNumber, "survey", q.list_name, null, null);
			}
		}
		
		// Matrix
		if(q.type.equals("begin matrix")) {
			// Missing list
			if(q.list_name == null) {
				throw XLSUtilities.getApplicationException(localisation, "tu_mat_list", rowNumSurvey, "survey", null, null, null);
			}
		}
		
		// check parameters
		if(q.paramArray != null) {
			ArrayList<KeyValueSimp> noDups = new ArrayList<KeyValueSimp> ();
			HashMap<String, String> paramHashMap = new HashMap<> ();
			for(KeyValueSimp kv : q.paramArray) {
				String existing = paramHashMap.get(kv.k);
				if(existing == null) {
					noDups.add(kv);
					paramHashMap.put(kv.k, kv.v);
				} else {
					if(existing.equals(kv.v)) {
						// Its a duplicate just discard by not adding to the noDups output
					} else {
						// Conflicting values
						throw XLSUtilities.getApplicationException(localisation, "tu_cf", rowNumber, "survey", kv.k, null, null);
					}
				}
			}
			q.paramArray = noDups;
			
		}
		
		// Check that parent and child forms have the form_identifier parameter
		if(q.type.equals("parent_form") || q.type.equals(SmapQuestionTypes.CHILD_FORM)) {
			boolean hasFormIdentifier = false;
			for(KeyValueSimp kv : q.paramArray) {
				if(kv.k.equals("form_identifier")) {
					hasFormIdentifier = true;
					break;
				}
			}
			if(!hasFormIdentifier) {
				throw XLSUtilities.getApplicationException(localisation, "tu_form_launch", rowNumber, "survey", null, null, null);
			}
		}
		
	}
	
	private void checkParentheses(ResourceBundle localisation, String expression, int rowNumber, String sheet, String column, String name) throws ApplicationException {
		checkMatchedParenthesies(localisation, '{', '}', expression, rowNumber, sheet, column);
		checkMatchedParenthesies(localisation, '(', ')', expression, rowNumber, sheet, column);
	}
	
	private void checkMatchedParenthesies(ResourceBundle localisation, char p1, char p2, String expression, int rowNumber, String sheet, String column) throws ApplicationException {
		int depth = 0; 
		int locn = 0;
		if(expression != null) {
             for(int i = 0; i < expression.length(); i++) {
                 char c = expression.charAt(i);
                 if( c == p1) {
                     depth++;
                     locn = i;
                 } else if( c == p2) {
                     depth--;
                     locn = i;
                 }
                 if(depth < 0) {
                     break;
                 }
             }

             if(depth != 0) {
            	 	if(p1 == '(') {
            	 		throw XLSUtilities.getApplicationException(localisation, "tu_mbs", rowNumber, sheet, column, String.valueOf(locn), null);
            	 	} else if(p1 == '{') {
            	 		throw XLSUtilities.getApplicationException(localisation, "tu_mbc", rowNumber, sheet, column, String.valueOf(locn), null);
            	 	}
                
             }
         }
	}
	
	private void checkCalculationCircularReferences(ResourceBundle localisation, String expression, int rowNumber, String sheet, String column, String name) throws ApplicationException {
		
		Pattern pattern = Pattern.compile("\\$\\{.+?\\}");
		java.util.regex.Matcher matcher = pattern.matcher(expression);
		while (matcher.find()) {
			String matched = matcher.group();
			String qname = matched.substring(2, matched.length() - 1);
			if(qname.equals(name)) {
				throw XLSUtilities.getApplicationException(localisation, "tu_cr", rowNumber, sheet, column, name, expression);
			}
		}		
	}
	
	private void validateSurvey() throws Exception {
		
		// Validate forms and questions
		for(Form f : survey.forms) {
			if(f.reference) {
				Integer rowNumber = qNameMap.get(f.name);
				if(f.name.equals(f.referenceName)) {
					throw XLSUtilities.getApplicationException(localisation, "tu_ref_self", rowNumber, "survey", f.name, null, null);
				} else {
					boolean validRef = false;
					for(Form refForm : survey.forms) {
						if(refForm.name.equals(f.referenceName)) {
							if(refForm.reference) {
								throw XLSUtilities.getApplicationException(localisation, "tu_ref_ref", rowNumber, "survey", f.name, refForm.name, null);
							} else {
								validRef = true;
								break;
							}
						}
					}
					if(!validRef) {
						throw XLSUtilities.getApplicationException(localisation, "tu_ref_nf", rowNumber, "survey", f.referenceName, f.name, null);
					}
				}
			}
			for(Question q : f.questions) {
				if(q.relevant != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.relevant);
					if(refs.size() > 0) {
						questionInSurvey(refs, "relevant", q);
					}
				}
				if(q.constraint != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.constraint);
					if(refs.size() > 0) {
						questionInSurvey(refs, "constraint", q);
					}
				}
				if(q.repeatCount != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.repeatCount);
					if(refs.size() > 0) {
						questionInSurvey(refs, "repeat_count", q);
					}
					// Make sure there is not a question with a name that will clash with the automatically generated repeat count name
					repeatCountClash(q);
				}
				if(q.choice_filter != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.choice_filter);
					if(refs.size() > 0) {
						questionInSurvey(refs, "choice_filter", q);
					}
				}
				if(q.labels != null) {
					int idx = 0;
					for(Label l : q.labels) {
						ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(l.text);
						if(refs.size() > 0) {
							questionInSurvey(refs, "label::" + survey.languages.get(idx).name, q);
							
							if(refs.contains(q.name)) {		// Check for self reference
								Integer rowNumber = qNameMap.get(q.name);
								throw XLSUtilities.getApplicationException(localisation, "tu_cr", rowNumber, "survey", 
										"label::" + survey.languages.get(idx).name, q.name, q.relevant);
							}
						}
						idx++;
					}
				}
				if(q.calculation != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.calculation);
					if(refs.size() > 0) {
						questionInSurvey(refs, "calculation", q);
					}
				}
				if(q.appearance != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.appearance);
					if(refs.size() > 0) {
						questionInSurvey(refs, "appearance", q);
					}
				}
				// check default
				if(q.setValues != null) {
					for(SetValue sv : q.setValues) {
						if(sv.value != null) {
							if(sv.value.contains("last-saved#")) {
								int idx1 = sv.value.indexOf('#');
								int idx2 = sv.value.indexOf('}', idx1);
								if(idx2 > 0) {
									String sourceQuestion = sv.value.substring(idx1 + 1, idx2);
									if(sourceQuestion != null) {
										ArrayList<String> refs = new ArrayList<String> ();
										refs.add(sourceQuestion);
										questionInSurvey(refs, "default", q);
									}
								}
							}
						}
					}
				}
			}
		}
		
		// Validate Settings
		if(survey.instanceNameDefn != null) {
			ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(survey.instanceNameDefn );
			if(refs.size() > 0) {
				settingsQuestionInSurvey(refs, "instance_name");
			}
		}
		
		// Validate groups
		for(Integer formIndex : groupStackMap.keySet()) {
			Stack<Question> groupStack = groupStackMap.get(formIndex);
		
			if(!groupStack.isEmpty()) {
				String groupName = groupStack.pop().name;
				Integer rowNumber = qNameMap.get(groupName);
				throw XLSUtilities.getApplicationException(localisation, "tu_meg", rowNumber, "survey", groupName, null, null);
			}
		}
		
	}

	private void validateOption(Option o, int rowNumber) throws ApplicationException {

		HashMap<String, Integer> listMap = oNameMap.get(o.optionList);
		if(listMap == null) {
			listMap = new HashMap<String, Integer> ();
			oNameMap.put(o.optionList, listMap);
		}

		if(o.value == null || o.value.trim().length() == 0) {
			// Check for a missing value
			throw XLSUtilities.getApplicationException(localisation, "tu_vr", rowNumber, "choices", o.optionList, null, null);

		} else if(!validChoiceName.matcher(o.value).matches()) {
			// Check for a valid value
			throw XLSUtilities.getApplicationException(localisation, "tu_cn",rowNumber, "choices", o.value, null, null);

		} else if(!validChoiceName.matcher(o.optionList).matches()) {
			// Check for a valid value
			throw XLSUtilities.getApplicationException(localisation, "tu_ln",rowNumber, "choices", o.optionList, null, null);

		} else if(listMap.get(o.value) != null) {
			// Check for a duplicate value
			ApplicationException e = XLSUtilities.getApplicationException(localisation, "tu_do", rowNumber, "choices", o.value, o.optionList, null);
			warnings.add(new ApplicationWarning(e.getMessage()));
		}

		listMap.put(o.value, rowNumber);

	}
	
	private void validateStyle(TableColumnMarkup s, int rowNumber, String styleName) throws ApplicationException {


		if(s.value == null || s.value.trim().length() == 0) {
			// Check for a missing value
			throw XLSUtilities.getApplicationException(localisation, "tu_vr", rowNumber, "styles", styleName, null, null);

		} else if(s.classes == null) {
			// Check for a valid color
			throw XLSUtilities.getApplicationException(localisation, "tu_cn",rowNumber, "styles", s.classes, null, null);

		} 
	}
	
	private void questionInSurvey(ArrayList<String> names, String context, Question q) throws ApplicationException {
		for(String name : names) {
			if(qNameMap.get(name) == null) {
				Integer rowNumber = qNameMap.get(q.name);
				throw XLSUtilities.getApplicationException(localisation, "tu_mq", rowNumber, "survey", context, name, null);
			}
		}
	}
	
	private void repeatCountClash(Question q) throws ApplicationException {
		String name = q.name.toLowerCase() + "_count";
		if(qNameMap.get(name) != null) {
			Integer rowNumber = qNameMap.get(name);
			throw XLSUtilities.getApplicationException(localisation, "tu_rc", rowNumber, "survey", name, q.name, null);
		}
	}
	
	private void settingsQuestionInSurvey(ArrayList<String> names, String colname) throws ApplicationException {
		for(String name : names) {
			if(qNameMap.get(name) == null) {
				String msg = localisation.getString("tu_mq");
				msg = msg.replace("%s1", colname);
				msg = msg.replace("%s2", "settings");
				msg = msg.replaceAll("%s3", name);
				throw new ApplicationException(msg);
			}
		}
	}
	
	private String getValidQuestionType(String in) {
		
		String out = null;	
		in = in.trim().replaceAll(" +", " ");	// From https://stackoverflow.com/questions/2932392/java-how-to-replace-2-or-more-spaces-with-single-space-in-string-and-delete-lead
		String type = in.toLowerCase();
		
		if(type.equals("text")) {
			out = "text";
		} else if(type.equals("integer") || type.equals("int")) {
			out = "int";
		} else if (type.equals("decimal")) {
			out = "decimal";
		} else if (type.startsWith("select_one") || type.startsWith("select one")) {
			int idx = type.indexOf("one");
			out = "select_one " + in.substring(idx + 3).trim();
		} else if (type.startsWith("select_multiple") || type.startsWith("select multiple")) {
			int idx = type.indexOf("multiple");
			out = "select_multiple " + in.substring(idx + 8).trim();
		} else if (type.startsWith("rank") || type.startsWith("odk:rank")) {
			int idx = type.indexOf("rank");
			out = "rank " + in.substring(idx + 4).trim();
		} else if (type.equals("note")) {
			out = "note";
		} else if (type.equals("geopoint") || type.equals("location")) {
			out = "geopoint";
		} else if (type.equals("geotrace")) {
			out = "geotrace";
		} else if (type.equals("geocompound")) {
			out = "geocompound";
		} else if (type.equals("geoshape")) {
			out = "geoshape";
		} else if (type.equals("date")) {
			out = "date";
		} else if (type.equals("datetime")) {
			out = "dateTime";
		} else if (type.equals("time")) {
			out = "time";
		} else if (type.equals("image")) {
			out = "image";
		} else if (type.equals("audio")) {
			out = "audio";
		} else if (type.equals("video")) {
			out = "video";
		} else if (type.equals("file")) {
			out = "file";
		} else if (type.equals("barcode")) {
			out = "barcode";
		} else if (type.equals("calculate") || type.equals("calculation")) {
			out = "calculate";
		} else if (type.equals("acknowledge")) {
			out = "acknowledge";
		} else if (type.equals("chart")) {
			out = "chart";
		} else if (type.equals("parent_form")) {
			out = "parent_form";
		} else if (type.equals(SmapQuestionTypes.CHILD_FORM)) {
			out = SmapQuestionTypes.CHILD_FORM;
		} else if (type.equals("range")) {
			out = "range";
		} else if (type.equals("begin repeat") || type.equals("begin_repeat")) {
			out = "begin repeat";
		} else if (type.equals("end repeat") || type.equals("end_repeat")) {
			out = "end repeat";
		} else if (type.equals("begin group") || type.equals("begin_group")) {
			out = "begin group";
		} else if (type.equals("begin matrix") || type.equals("begin_matrix")) {
			out = "begin matrix";
		} else if (type.equals("end group") || type.equals("end_group")) {
			out = "end group";
		} else if (type.equals("end matrix") || type.equals("end_matrix")) {
			out = "end matrix";
		} else if (type.equals("start")) {
			out = "start";
		} else if (type.equals("end")) {
			out = "end";
		} else if (type.equals("today")) {
			out = "today";
		} else if (type.equals("deviceid")) {
			out = "deviceid";
		} else if (type.equals("subscriberid")) {
			out = "subscriberid";
		} else if (type.equals("simserial")) {
			out = "simserial";
		} else if (type.equals("phonenumber")) {
			out = "phonenumber";
		} else if (type.equals("username")) {
			out = "username";
		} else if (type.equals("email")) {
			out = "email";
		} else if (type.equals("server_calculate")) {
			out = "server_calculate";
		} else if (type.equals("start-geopoint")) {	
			out = "start-geopoint";
		} else if (type.equals("hidden value")) {    // Commcare 
			out = "calculate";
		} else if (type.equals("label")) {			// Commcare 
			out = "note";
		} else if (type.equals("trigger")) {			// Commcare 
			out = "trigger";
		} else if (type.equals("background-audio")) {
			out = "background-audio";
		} else if (type.equals("pdf_field")) {
			out = "pdf_field";
		}
				
		return out;
	}
	
	private boolean getBooleanColumn(Row row, String name, HashMap<String, Integer> header, int lastCellNum, boolean def) throws ApplicationException {
		String v = XLSUtilities.getTextColumn(row, name, header, lastCellNum, null); 
		
		if(v != null) {
			v = v.trim();
		} else {
			return def;
		}
		
		if(v.equalsIgnoreCase("yes") 
				|| v.equalsIgnoreCase("true") 
				|| v.equalsIgnoreCase("y")) {
			return true;
		} else {
			return false;
		}
	}
	
	private void setFormReference(ArrayList<KeyValueSimp> parameters, Form f) throws ApplicationException {
		if(parameters != null) {
			String ref = GeneralUtilityMethods.getSurveyParameter("ref", parameters);
			if(ref != null) {
				f.reference = true;			
				f.referenceName = ref;
			}
			
		}
	}
	
	private void setFormMerge(ArrayList<KeyValueSimp>  parameters, Form f) throws ApplicationException {
		if(parameters != null) {
			String ref = GeneralUtilityMethods.getSurveyParameter("merge", parameters);	// deprecate
			if(ref != null) {
				f.merge = true;			
			}
			ref = GeneralUtilityMethods.getSurveyParameter("key_policy", parameters);
			if(ref != null) {
				if(ref.equals("replace")) {
					f.replace = true;
				} else if(ref.equals("merge")) {
					f.merge = true;
				} else if(ref.equals("append")) {
					f.append = true;
				}
			}
			
		}
	}
	
	/*
	 * Test for valid javarosa functions in an XPath expression
	 * If the call is for an appearance then only the search function is valid and "search" without parameters is also valid
	 */
	private void testXExprFunctions(String in, ResourceBundle localisation, 
			boolean isAppearance,
			int rowNumber,
			String column) throws Exception {
		
		// 1. remove any text inside quotes
		boolean inside = false;
		StringBuffer noText = new StringBuffer("");
		for(int i = 0; i < in.length(); i++) {
			if(in.charAt(i) == '\'') {
				inside = !inside;
			} else if(!inside) {
				noText.append(in.charAt(i));
			}
		}
		
		if(noText.length() > 0) {
			String process = noText.toString();
			for(FunctionCheck f : functions) {
				Pattern pattern = Pattern.compile(f.name + "[\\s]*\\(");
				java.util.regex.Matcher matcher = pattern.matcher(process);
	
				while (matcher.find()) {
	
					StringBuffer toTest = new StringBuffer("");
					String matched = matcher.group();			
					
					// remove sub functions
					int depth = 0;
					boolean end = false;
					for(int i = matcher.start(); i < noText.length(); i++) {
						if(noText.charAt(i) == '(') {
							depth++;
						} else if(noText.charAt(i) == ')') {
							depth--;
							if(depth == 0) {
								end = true;
							}
						}
						if(depth <= 1) {
							toTest.append(noText.charAt(i));
						}
						if(end) {
							break;
						}
					}
					
					String[] args = toTest.toString().split(",");
					if(args.length != f.args) {
						throw XLSUtilities.getApplicationException(localisation, "tu_args", rowNumber, "survey", column, 
								f.name, f.template);
					}
				}
			}
		}
	}
	
	private Stack<Question> getGroupStack(Integer formIdx) {
		Stack<Question> gs = null;
		gs = groupStackMap.get(formIdx);
		if(gs == null) {
			gs = new Stack<Question> ();
			groupStackMap.put(formIdx, gs);
		}
		return gs;
	}

}
