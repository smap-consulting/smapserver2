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

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.ManifestInfo;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.RoleColumnFilterRef;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.Survey;

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
	int lastRowNumSurvey = 0;
	int lastRowNumChoices = 0;
	int lastRowNumSettings = 0;

	HashMap<String, Integer> surveyHeader = null;
	HashMap<String, Integer> choicesHeader = null;
	HashMap<String, Integer> settingsHeader = null;
	HashMap<String, Integer> choiceFilterHeader = null;
	HashMap<String, Integer> columnRoleHeader = null;
	HashMap<String, Integer> rowRoleHeader = null;
	
	HashMap<String, String> questionNames;	// Mapping between original name and truncated name
	HashMap<String, String> optionNames;		// Mapping between original name and truncated name
	boolean merge;

	HashMap<String, Integer> qNameMap = new HashMap<> ();							// Use in question name validation
	HashMap<String, HashMap<String, Integer>> oNameMap = new HashMap<> ();		// Use in option name validation
	Pattern validQname = Pattern.compile("^[A-Za-z_][A-Za-z0-9_\\-\\.]*$");
	Pattern validChoiceName = Pattern.compile("^[A-Za-z0-9_@\\-\\.\\+%,():/]*$");

	Stack<String> groupStack = new Stack<String>();							// Keep track of groups
	boolean inFieldList = false;												// Only some questions are allowed inside a field list
	
	boolean useDefaultLanguage = false;

	Survey survey = null;
	ResourceBundle localisation = null;

	public XLSTemplateUploadManager() {
	}

	/*
	 * Get a survey definition from an XLS file
	 */
	public Survey getSurvey(Connection sd, 
			int oId, 
			String type, 
			InputStream inputStream, 
			ResourceBundle localisation, 
			String displayName,
			int p_id,
			HashMap<String, String> questionNames,
			HashMap<String, String> optionNames,
			boolean merge) throws Exception {

		this.localisation = localisation;
		this.questionNames = questionNames;
		this.optionNames = optionNames;
		this.merge = merge;

		if(type != null && type.equals("xls")) {
			wb = new HSSFWorkbook(inputStream);
		} else {
			wb = new XSSFWorkbook(inputStream);
		}

		// Create survey and set defaults
		survey = new Survey();
		survey.displayName = displayName;
		survey.o_id = oId;
		survey.p_id = p_id;
		survey.version = 1;
		survey.loadedFromXLS = true;
		survey.deleted = false;
		survey.blocked = false;
		survey.meta.add(new MetaItem("string", "instanceID", null, "instanceid", null, false, null));

		surveySheet = wb.getSheet("survey");
		choicesSheet = wb.getSheet("choices");
		settingsSheet = wb.getSheet("settings");

		if(surveySheet == null) {
			throw new Exception("A worksheet called 'survey' not found");
		} else if(surveySheet.getPhysicalNumberOfRows() == 0) {
			throw new Exception("The survey worksheet is empty");
		} else {

			lastRowNumSurvey = surveySheet.getLastRowNum();
			if(choicesSheet != null) {
				lastRowNumChoices = choicesSheet.getLastRowNum();
			}
			if(settingsSheet != null) {
				lastRowNumSettings = settingsSheet.getLastRowNum();
			}			

			getHeaders();	// get headers and set the languages from them

			/*
			 * 1. Process the choices sheet
			 */
			if(choicesSheet != null) {
				while(rowNumChoices <= lastRowNumChoices) {

					Row row = choicesSheet.getRow(rowNumChoices++);

					if(row != null) {
						int lastCellNum = row.getLastCellNum();	
						String listName = XLSUtilities.getTextColumn(row, "list name", choicesHeader, lastCellNum);
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
			 * 2. Process the survey sheet
			 */
			getForm("main", -1, -1, null);
			if(survey.forms.get(0).questions.size() == 0) {
				throw new ApplicationException(localisation.getString("tu_nq"));
			}

		}

		/*
		 * 3, Process the settings sheet
		 */
		if(settingsSheet != null && settingsHeader != null) {
			Row row = settingsSheet.getRow(rowNumSettings++);
			if(row != null) {
				int lastCellNum = row.getLastCellNum();
				
				// Default language
				survey.def_lang = XLSUtilities.getTextColumn(row, "default_language", settingsHeader, lastCellNum);
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
				
				survey.instanceNameDefn = XLSUtilities.getTextColumn(row, "instance_name", settingsHeader, lastCellNum);
				survey.surveyClass = XLSUtilities.getTextColumn(row, "style", settingsHeader, lastCellNum);
				survey.task_file = getBooleanColumn(row, "allow_import", settingsHeader, lastCellNum);
				survey.hrk = XLSUtilities.getTextColumn(row, "key", settingsHeader, lastCellNum);
				survey.key_policy = XLSUtilities.getTextColumn(row, "key_policy", settingsHeader, lastCellNum);
				
				// Add row filters
				if(rowRoleHeader != null && rowRoleHeader.size() > 0) {
					for(String h : rowRoleHeader.keySet()) {
						String filter = XLSUtilities.getTextColumn(row, h, settingsHeader, lastCellNum);
						if(filter != null) {
							Role r = survey.roles.get(h);
							if(r != null) {
								SqlFrag sq = new SqlFrag();
								sq.addSqlFragment(filter, localisation, false);
								settingsQuestionInSurvey(sq.columns, h);		// validate question names
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
			survey.meta.add(new MetaItem("dateTime", "_start", "start", "_start", "timestamp", true, "start"));
		}
		if(!hasMeta("end")) {
			survey.meta.add(new MetaItem("dateTime", "_end", "end", "_end", "timestamp", true, "end"));
		}
		if(!hasMeta("deviceid")) {
			survey.meta.add(new MetaItem("string", "_device", "deviceid", "_device", "property", true, "device"));
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

	private Option getOption(Row row, String listName) throws ApplicationException {

		Option o = new Option();
		int lastCellNum = row.getLastCellNum();
		o.optionList = listName;

		o.value = XLSUtilities.getTextColumn(row, "name", choicesHeader, lastCellNum);
		getLabels(row, lastCellNum, choicesHeader, o.labels);
		
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
		o.cascade_filters = new HashMap<String, String> ();   // TODO - Choice filters from choices sheet
		for(String key : choiceFilterHeader.keySet()) {
			String value = XLSUtilities.getTextColumn(row, key, choicesHeader, lastCellNum);
			if (value != null) {
				o.cascade_filters.put(key, value);
			}
		}

		validateOption(o, rowNumChoices);

		return o;
	}

	/*
	 * Get the survey header and the choices header so we can identify all the languages up front
	 * This should work gracefully with a badly designed forms where there are inconsistent language
	 *  names between the survey and choices sheet
	 */
	private void getHeaders() {

		choiceFilterHeader = new HashMap<String, Integer> ();		
		HashMap<String, String> langMap = new HashMap<String, String> ();

		// Get survey sheet headers
		while(rowNumSurvey <= lastRowNumSurvey) {
			Row row = surveySheet.getRow(rowNumSurvey++);
			if(row != null) {
				surveyHeader = XLSUtilities.getHeader(row);

				// Add languages in order they exist in the header hence won't use keyset of surveyHeader
				int lastCellNum = row.getLastCellNum();							
				for(int i = 0; i <= lastCellNum; i++) {
					Cell cell = row.getCell(i);
					if(cell != null) {
						String name = cell.getStringCellValue();
						if(name.startsWith("label::")) {	 // Only check the question label for languages, any others will be assumed to be errors
							String [] sArray = name.split("::");
							if(sArray.length > 0) {
								String exists = langMap.get(sArray[1]);
								if(exists == null) {
									langMap.put(sArray[1], sArray[1]);
									survey.languages.add(new Language(0, sArray[1]));
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
					choicesHeader = XLSUtilities.getHeader(row);	
					
					// Get the headers for filters
					for(String h : choicesHeader.keySet()) {
						if(h.equals("list name")
								|| h.equals("name")
								|| h.equals("label")
								|| h.startsWith("label::") 
								|| h.equals("image")
								|| h.startsWith("image::") 
								|| h.equals("audio")
								|| h.startsWith("audio::") 
								|| h.equals("video") 
								|| h.startsWith("video::")) {
							continue;
						}
						// The rest must be filter columns
						choiceFilterHeader.put(h, choicesHeader.get(h));
					}
	
					// Add languages in order they exist in the header hence won't use keyset of surveyHeader
					/*
					int lastCellNum = row.getLastCellNum();				
					int idx = 0;				
					for(int i = 0; i <= lastCellNum; i++) {
						Cell cell = row.getCell(i);
						if(cell != null) {
							String name = cell.getStringCellValue();
							if(name.startsWith("label::") 
									|| name.startsWith("image::") || name.startsWith("video::") 
									|| name.startsWith("audio::")) {
								String [] sArray = name.split("::");
								if(sArray.length > 0) {
									String exists = langMap.get(sArray[1]);
									if(exists == null) {
										langMap.put(sArray[1], sArray[1]);
										survey.languages.add(new Language(idx++, sArray[1]));
									}
								}
							}
						}
					}
					*/
					break;
				}
			}
		}

		// Add a default language if needed
		if(survey.languages.size() == 0) {
			survey.languages.add(new Language(0, "language"));
			useDefaultLanguage = true;
		}
		
		// Get Setting sheet headers
		if(settingsSheet != null) {
			while(rowNumSettings <= lastRowNumSettings) {
				Row row = settingsSheet.getRow(rowNumSettings++);
				if(row != null) {
					settingsHeader = XLSUtilities.getHeader(row);
					break;
				}
			}
			
			// Add security roles
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

	/*
	 * Process the question rows to create a form
	 */
	private void getForm(String name, int parentFormIndex, int parentQuestionIndex, String parameters) throws Exception {

		Form f = new Form(name, parentFormIndex, parentQuestionIndex);
		f.setReference(parameters);
		survey.forms.add(f);
		
		int thisFormIndex = survey.forms.size() - 1;

		while(rowNumSurvey <= lastRowNumSurvey) {

			Row row = surveySheet.getRow(rowNumSurvey++);

			if(row != null) {
				Question q = getQuestion(row, thisFormIndex, f.questions.size());				
				if(q != null) {
					MetaItem item = GeneralUtilityMethods.getPreloadItem(q.type, q.name, q.display_name);
					if(item != null) {
						validateQuestion(q, rowNumSurvey);
						survey.meta.add(item);
					} else {
						if(q.type.equals("end repeat")) {
							if(parentFormIndex < 0) {
								throw XLSUtilities.getApplicationException(localisation, "tu_eer", rowNumSurvey, "survey", null, null);
							}
							return; 
						}
							
						// Update the survey manifest if csv files are referenced from the appearance and then the calculation
						ManifestInfo mi = GeneralUtilityMethods.addManifestFromAppearance(q.appearance, survey.manifest);
						mi = GeneralUtilityMethods.addManifestFromCalculate(q.calculation, mi.manifest);
						survey.manifest = mi.manifest;
						
						validateQuestion(q, rowNumSurvey);
						f.questions.add(q);
						
						if(q.type.equals("begin repeat")) {
							getForm(q.name, thisFormIndex, f.questions.size() - 1, q.parameters);
						}
					}
				}
						
			}
		}

	}

	/*
	 * Get a question from the excel sheet
	 */
	private Question getQuestion(Row row, int formIndex, int questionIndex) throws ApplicationException {

		Question q = new Question();
		int lastCellNum = row.getLastCellNum();

		// 1. Question type
		String type = XLSUtilities.getTextColumn(row, "type", surveyHeader, lastCellNum);	
		
		// 2. Question name
		q.name = XLSUtilities.getTextColumn(row, "name", surveyHeader, lastCellNum);  
		if(type == null && q.name != null) {
			throw XLSUtilities.getApplicationException(localisation, "tu_mt", rowNumSurvey, "survey", null, null);
		} else if(type == null && q.name == null) {
			return null;		// blank row
		}
		
		q.type = convertType(type, q);			
		if(q.type.equals("geopoint") || q.type.equals("geotrace") || q.type.equals("geoshape")) {
			q.name = "the_geom";
		}	
		
		
		if(merge) {
			String n = questionNames.get(q.name);
			if(n != null) {
				q.columnName = n;
			} else {
				q.columnName = GeneralUtilityMethods.cleanName(q.name, true, true, true);
			}
		} else {
			q.columnName = GeneralUtilityMethods.cleanName(q.name, true, true, true);
		}
		
		// 3. Labels
		getLabels(row, lastCellNum, surveyHeader, q.labels);		

		// 4. choice filter
		q.choice_filter = XLSUtilities.getTextColumn(row, "choice_filter", surveyHeader, lastCellNum);
		
		// 5. Constraint
		q.constraint = XLSUtilities.getTextColumn(row, "constraint", surveyHeader, lastCellNum);  
		
		// 6. Constraint message
		q.constraint_msg = XLSUtilities.getTextColumn(row, "constraint_message", surveyHeader, lastCellNum); 
		
		// 7. Relevant
		q.relevant = XLSUtilities.getTextColumn(row, "relevant", surveyHeader, lastCellNum);  		

		// 7. Repeat count
		if(q.type.equals("begin repeat")) {
			q.repeatCount = XLSUtilities.getTextColumn(row, "repeat_count", surveyHeader, lastCellNum);  
		}
		
		// 8. Default
		q.defaultanswer = XLSUtilities.getTextColumn(row, "default", surveyHeader, lastCellNum); 
		
		// 9. Readonly
		q.readonly = getBooleanColumn(row, "readonly", surveyHeader, lastCellNum);
		
		// 10. Appearance
		q.appearance = XLSUtilities.getTextColumn(row, "appearance", surveyHeader, lastCellNum); 
		
		// 11. Parameters TODO
		q.parameters = XLSUtilities.getTextColumn(row, "parameters", surveyHeader, lastCellNum);
		
		// 12. autoplay TODO
		q.autoplay = XLSUtilities.getTextColumn(row, "autoplay", surveyHeader, lastCellNum);
		
		// 13. body::accuracyThreshold
		q.accuracy = XLSUtilities.getTextColumn(row, "body::accuracyThreshold", surveyHeader, lastCellNum);
		
		// 14. Required
		q.required = getBooleanColumn(row, "required", surveyHeader, lastCellNum);		
		
		// 15. Required Message
		q.required_msg = XLSUtilities.getTextColumn(row, "required_message", surveyHeader, lastCellNum); 
		
		// 16. Calculation
		q.calculation = XLSUtilities.getTextColumn(row, "calculation", surveyHeader, lastCellNum); 
		
		// 17. Display Name
		q.display_name = XLSUtilities.getTextColumn(row, "display_name", surveyHeader, lastCellNum); 
		
		// Add Column Roles
		if(columnRoleHeader != null && columnRoleHeader.size() > 0) {
			for(String h : columnRoleHeader.keySet()) {
				if(getBooleanColumn(row, h, surveyHeader, lastCellNum)) {
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
		if(q.type.equals("begin group")) {	
			groupStack.push(q.name);
		}
		if(q.type.equals("end group")) {
			String currentGroup = groupStack.pop();
			if(q.name != null && q.name.trim().length() > 0) {
				// Validate the provided group name against the current group
				if(!q.name.equals(currentGroup)) {
					throw XLSUtilities.getApplicationException(localisation, "tu_eeg", rowNumSurvey, "survey", q.name, currentGroup);
				}			
			} else {
				// Set the name of the end group to its group
				q.name = currentGroup;
			}
		}
		
		/*
		 * Derived Values
		 */
		// 1. Source
		if(q.type.equals("begin group") || q.type.equals("end group") || q.type.equals("begin repeat")) {
			q.source = null;
		} else {
			q.source = "user";
		}
		
		// 2. Visibility
		q.visible = convertVisible(type);

		return q;
	}

	/*
	 * For media try under the default column heading if the language specific is null
	 */
	private void getLabels(Row row, int lastCellNum, HashMap<String, Integer> header, ArrayList<Label> labels) throws ApplicationException {

		// Get the label language values
		if(useDefaultLanguage) {
			Label lab = new Label();
			lab.text = XLSUtilities.getTextColumn(row, "label", header, lastCellNum);
			lab.hint = XLSUtilities.getTextColumn(row, "hint", header, lastCellNum);
			lab.image = XLSUtilities.getTextColumn(row, "image", header, lastCellNum);
			lab.video = XLSUtilities.getTextColumn(row, "video", header, lastCellNum);
			lab.audio = XLSUtilities.getTextColumn(row, "audio", header, lastCellNum);
			
			labels.add(lab);
		} else {
			for(int i = 0; i < survey.languages.size(); i++) {
				String lang = survey.languages.get(i).name;
				Label lab = new Label();
				lab.text = XLSUtilities.getTextColumn(row, "label::" + lang, header, lastCellNum);
				lab.hint = XLSUtilities.getTextColumn(row, "hint::" + lang, header, lastCellNum);
				lab.image = XLSUtilities.getTextColumn(row, "image::" + lang, header, lastCellNum);
				if(lab.image == null) {
					lab.image = XLSUtilities.getTextColumn(row, "image", header, lastCellNum);
				}
				lab.video = XLSUtilities.getTextColumn(row, "video::" + lang, header, lastCellNum);
				if(lab.video == null) {
					lab.video = XLSUtilities.getTextColumn(row, "video", header, lastCellNum);
				}
				lab.audio = XLSUtilities.getTextColumn(row, "audio::" + lang, header, lastCellNum);
				if(lab.audio == null) {
					lab.audio = XLSUtilities.getTextColumn(row, "audio", header, lastCellNum);
				}
				labels.add(lab);
			}
		}

	}


	private String convertType(String in, Question q) throws ApplicationException {

		String type = getValidQuestionType(in);		
		
		// Validate and normalise input
		if(type == null) {
			throw XLSUtilities.getApplicationException(localisation, "tu_ut", rowNumSurvey, "survey", in, null);
		}

		// Do type conversions
		if (type.equals("text")) {
			type = "string";
		} else if(in.startsWith("select_one") || in.startsWith("select_multiple")) {
			
			if(in.startsWith("select_one")) {
				type = "select1";
			} else if(in.startsWith("select_multiple")) {
				type = "select";
			}
			
			String [] array = in.split(" ");
			if(array.length <= 1) {
				throw XLSUtilities.getApplicationException(localisation, "tu_mln", rowNumSurvey, "survey", in.trim(), null);
			}
			q.list_name = array[1].trim();
		} 
		
		return type;
	}

	private boolean convertVisible(String type) {
		boolean visible = true;
		if(type.equals("calculate")) {
			visible = false;
		} else if(type.equals("end group")) {
			visible = false;
		} else if(GeneralUtilityMethods.getPreloadItem(type, "", "") != null) {
			visible = false;
		}

		return visible;
	}

	private void validateQuestion(Question q, int rowNumber) throws Exception {

		if (q.name == null || q.name.trim().length() == 0) {
			// Check for a missing name
			throw XLSUtilities.getApplicationException(localisation, "tu_mn", rowNumber, "survey", null, null);

		} else if(!validQname.matcher(q.name).matches()) {
			// Check for a valid name
			throw XLSUtilities.getApplicationException(localisation, "tu_qn", rowNumber, "survey", q.name, null);

		} else if(!q.type.equals("end group") && qNameMap.get(q.name) != null) {
			// Check for a duplicate name
			throw XLSUtilities.getApplicationException(localisation, "tu_dq", rowNumber, "survey", q.name, null);

		}
		if(!q.type.equals("end group")) {		
			qNameMap.put(q.name, rowNumber);
		}
		
		// Circular references
		if(q.relevant != null) {
			ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.relevant);
			if(refs.contains(q.name)) {
				throw XLSUtilities.getApplicationException(localisation, "tu_cr", rowNumber, "survey", "relevant", q.name);
			}
			
			checkExpression(localisation, q.relevant, rowNumber, "survey", "relevant", q.name);
		}
		
		if(q.constraint != null) {
			checkExpression(localisation, q.constraint, rowNumber, "survey", "constraint", q.name);
		}
		
		if(q.calculation != null) {
			checkExpression(localisation, q.calculation, rowNumber, "survey", "calculation", q.name);
		}
		
		if(q.choice_filter != null) {
			checkExpression(localisation, q.choice_filter, rowNumber, "survey", "choice_filter", q.name);
		}
		
		// invalid question in field-list
		if(inFieldList) {
			if(q.type.equals("end group")) {
				inFieldList = false;
			} else if(q.type.equals("begin group") || q.type.equals("begin repeat")) {
				String groupName = groupStack.pop();
				throw XLSUtilities.getApplicationException(localisation, "tu_fl", rowNumber, "survey", q.type, groupName);
			}
		} else {
			if(q.type.equals("begin group")) {
				if(q.appearance != null && q.appearance.contains("field-list")) {
					inFieldList = true;
				}
			}
		}
		
	}
	
	private void checkExpression(ResourceBundle localisation, String expression, int rowNumber, String sheet, String column, String name) throws ApplicationException {
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
            	 		throw XLSUtilities.getApplicationException(localisation, "tu_mbs", rowNumber, sheet, column, String.valueOf(locn));
            	 	} else if(p1 == '{') {
            	 		throw XLSUtilities.getApplicationException(localisation, "tu_mbc", rowNumber, sheet, column, String.valueOf(locn));
            	 	}
                
             }
         }
	}
	
	private void validateSurvey() throws Exception {
		
		// Validate questions
		for(Form f : survey.forms) {
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
						}
					}
				}
				if(q.calculation != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.calculation);
					if(refs.size() > 0) {
						questionInSurvey(refs, "calculation", q);
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
		if(!groupStack.isEmpty()) {
			String groupName = groupStack.pop();
			Integer rowNumber = qNameMap.get(groupName);
			throw XLSUtilities.getApplicationException(localisation, "tu_meg", rowNumber, "survey", groupName, null);
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
			throw XLSUtilities.getApplicationException(localisation, "tu_vr", rowNumber, "choices", o.optionList, null);

		} else if(!validChoiceName.matcher(o.value).matches()) {
			// Check for a valid value
			throw XLSUtilities.getApplicationException(localisation, "tu_cn",rowNumber, "choices", o.value, null);

		} else if(listMap.get(o.value) != null) {
			// Check for a duplicate value
			throw XLSUtilities.getApplicationException(localisation, "tu_do", rowNumber, "choices", o.value, o.optionList);

		}

		listMap.put(o.value, rowNumber);

	}
	
	private void questionInSurvey(ArrayList<String> names, String context, Question q) throws ApplicationException {
		for(String name : names) {
			if(qNameMap.get(name) == null) {
				Integer rowNumber = qNameMap.get(q.name);
				throw XLSUtilities.getApplicationException(localisation, "tu_mq", rowNumber, "survey", context, name);
			}
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
		String type = in.toLowerCase().trim();
		
		if(type.equals("text")) {
			out = "text";
		} else if(type.equals("integer") || type.equals("int")) {
			out = "int";
		} else if (type.equals("decimal")) {
			out = "decimal";
		} else if (type.startsWith("select_one") || type.startsWith("select one")) {
			int idx = in.indexOf("one");
			out = "select_one " + in.substring(idx + 3);
		} else if (type.startsWith("select_multiple") || type.startsWith("select multiple")) {
			int idx = in.indexOf("multiple");
			out = "select_multiple " + in.substring(idx + 8);;
		} else if (type.equals("note")) {
			out = "note";
		} else if (type.equals("geopoint")) {
			out = "geopoint";
		} else if (type.equals("geotrace")) {
			out = "geotrace";
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
		} else if (type.equals("barcode")) {
			out = "barcode";
		} else if (type.equals("calculate")) {
			out = "calculate";
		} else if (type.equals("acknowledge")) {
			out = "acknowledge";
		} else if (type.equals("graph")) {
			out = "graph";
		} else if (type.equals("range")) {
			out = "range";
		} else if (type.equals("begin repeat")) {
			out = "begin repeat";
		} else if (type.equals("end repeat")) {
			out = "end repeat";
		} else if (type.equals("begin group")) {
			out = "begin group";
		} else if (type.equals("end group")) {
			out = "end group";
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
		} else if (type.equals("hidden value")) {	// Commcare 
			out = "calculate";
		} else if (type.equals("label")) {			// Commcare 
			out = "note";
		}
				
		return out;
	}
	
	private boolean getBooleanColumn(Row row, String name, HashMap<String, Integer> header, int lastCellNum) throws ApplicationException {
		String v = XLSUtilities.getTextColumn(row, name, header, lastCellNum); 
		
		if(v != null) {
			v = v.trim();
		} else {
			return false;
		}
		
		if(v.equalsIgnoreCase("yes") 
				|| v.equalsIgnoreCase("true") 
				|| v.equalsIgnoreCase("y")) {
			return true;
		} else {
			return false;
		}
	}

}
