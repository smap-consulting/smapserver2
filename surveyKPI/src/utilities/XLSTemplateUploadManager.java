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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.RoleColumnFilter;
import org.smap.sdal.model.RoleColumnFilterRef;
import org.smap.sdal.model.Survey;
import org.w3c.dom.Element;

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

	HashMap<String, Integer> surveyHeader = null;
	HashMap<String, Integer> choicesHeader = null;
	HashMap<String, Integer> choiceFilterHeader = null;
	HashMap<String, Integer> columnRoleHeader = null;

	HashMap<String, Integer> qNameMap = new HashMap<> ();							// Use in question name validation
	HashMap<String, HashMap<String, Integer>> oNameMap = new HashMap<> ();		// Use in option name validation
	Pattern validQname = Pattern.compile("^[A-Za-z_][A-Za-z0-9_\\-\\.]*$");
	Pattern validChoiceName = Pattern.compile("^[A-Za-z0-9_@\\-\\.:/]*$");

	Stack<String> groupStack = new Stack<String>();							// Keep track of groups
	
	boolean useDefaultLanguage = false;

	Survey survey = null;
	ResourceBundle localisation = null;

	public XLSTemplateUploadManager(String type) {
		if(type != null && type.equals("xls")) {
			wb = new HSSFWorkbook();
		} else {
			wb = new XSSFWorkbook();
		}
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
			int p_id) throws Exception {

		this.localisation = localisation;

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
		survey.meta.add(new MetaItem("string", "instanceID", null, "instanceid", null));

		surveySheet = wb.getSheet("survey");
		choicesSheet = wb.getSheet("choices");

		if(surveySheet == null) {
			throw new Exception("A worksheet called 'survey' not found");
		} else if(surveySheet.getPhysicalNumberOfRows() == 0) {
			throw new Exception("The survey worksheet is empty");
		} else {

			lastRowNumSurvey = surveySheet.getLastRowNum();
			if(choicesSheet != null) {
				lastRowNumChoices = choicesSheet.getLastRowNum();
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
						String listName = XLSUtilities.getColumn(row, "list name", choicesHeader, lastCellNum, null);
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
			getForm("main", -1, -1);
			if(survey.forms.get(0).questions.size() == 0) {
				throw new ApplicationException(localisation.getString("tu_nq"));
			}

		}

		/*
		 * 3, Process the settings sheet
		 */
		settingsSheet = wb.getSheet("settings");
		if(settingsSheet != null && settingsSheet.getPhysicalNumberOfRows() > 0) {


		}
		
		/*
		 * 4. Final Validation
		 */
		validateSurvey();

		return survey;


	}

	private Option getOption(Row row, String listName) throws ApplicationException {

		Option o = new Option();
		int lastCellNum = row.getLastCellNum();
		o.optionList = listName;

		o.value = XLSUtilities.getColumn(row, "name", choicesHeader, lastCellNum, null);
		getLabels(row, lastCellNum, choicesHeader, o.labels);
		o.columnName = GeneralUtilityMethods.cleanName(o.value, false, false, false);
		o.cascade_filters = new HashMap<String, String> ();   // TODO - Choice filters from choices sheet
		for(String key :choiceFilterHeader.keySet()) {
			String value = XLSUtilities.getColumn(row, key, choicesHeader, lastCellNum, null);
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

		while(rowNumSurvey <= lastRowNumSurvey) {
			Row row = surveySheet.getRow(rowNumSurvey++);
			if(row != null) {
				surveyHeader = XLSUtilities.getHeader(row);

				// Add languages in order they exist in the header hence won't use keyset of surveyHeader
				int lastCellNum = row.getLastCellNum();				
				int idx = 0;				
				for(int i = 0; i <= lastCellNum; i++) {
					Cell cell = row.getCell(i);
					if(cell != null) {
						String name = cell.getStringCellValue();
						if(name.startsWith("label::") || name.startsWith("hint::") 
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
				break;
			}
		}

		// Add a default language if needed
		if(survey.languages.size() == 0) {
			survey.languages.add(new Language(0, "language"));
			useDefaultLanguage = true;
		}

	}

	/*
	 * Process the question rows to create a form
	 */
	private void getForm(String name, int parentFormIndex, int parentQuestionIndex) throws Exception {

		Form f = new Form(name, parentFormIndex, parentQuestionIndex);
		survey.forms.add(f);
		
		int thisFormIndex = survey.forms.size() - 1;

		while(rowNumSurvey <= lastRowNumSurvey) {

			Row row = surveySheet.getRow(rowNumSurvey++);

			if(row != null) {
				Question q = getQuestion(row, thisFormIndex, f.questions.size());				
				if(q != null) {
					if(q.type.equals("end repeat")) {
						if(parentFormIndex < 0) {
							throw XLSUtilities.getApplicationException(localisation, "tu_eer", rowNumSurvey, "survey", null, null);
						}
						return;
					}
					validateQuestion(q, rowNumSurvey);
					f.questions.add(q);
					
					if(q.type.equals("begin repeat")) {
						getForm(q.name, thisFormIndex, f.questions.size() - 1);
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
		String type = XLSUtilities.getColumn(row, "type", surveyHeader, lastCellNum, null);
		if(type == null) {
			throw XLSUtilities.getApplicationException(localisation, "tu_mt", rowNumSurvey, "survey", null, null);
		}		
		q.type = convertType(type, q);	
		
		// 2. Question name
		q.name = XLSUtilities.getColumn(row, "name", surveyHeader, lastCellNum, null);  
		if(q.type.equals("geopoint") || q.type.equals("geotrace") || q.type.equals("geoshape")) {
			q.name = "the_geom";
		}
		
		// 3. Labels
		getLabels(row, lastCellNum, surveyHeader, q.labels);		

		// 4. choice filter
		q.choice_filter = XLSUtilities.getColumn(row, "choice_filter", surveyHeader, lastCellNum, null);
		
		// 5. Constraint
		q.constraint = XLSUtilities.getColumn(row, "constraint", surveyHeader, lastCellNum, null);  
		
		// 6. Constraint message
		q.constraint_msg = XLSUtilities.getColumn(row, "constraint_message", surveyHeader, lastCellNum, null); 
		
		// 7. Relevant
		q.relevant = XLSUtilities.getColumn(row, "relevant", surveyHeader, lastCellNum, null);  		

		// 7. Repeat count
		if(q.type.equals("begin repeat")) {
			q.repeatCount = XLSUtilities.getColumn(row, "repeat_count", surveyHeader, lastCellNum, null);  
		}
		
		// 8. Default
		q.defaultanswer = XLSUtilities.getColumn(row, "default", surveyHeader, lastCellNum, null); 
		
		// 9. Readonly
		q.readonly = getBooleanColumn(row, "readonly", surveyHeader, lastCellNum);
		
		// 10. Appearance
		q.appearance = XLSUtilities.getColumn(row, "appearance", surveyHeader, lastCellNum, null); 
		
		// 11. Parameters TODO
		
		// 12. autoplay TODO
		
		// 13. body::accuracyThreshold TODO
		
		// 14. Required
		q.required = getBooleanColumn(row, "required", surveyHeader, lastCellNum);		
		
		// 15. Required Message
		q.required_msg = XLSUtilities.getColumn(row, "required_message", surveyHeader, lastCellNum, null); 
		
		// 16. Calculation
		q.calculation = XLSUtilities.getColumn(row, "calculation", surveyHeader, lastCellNum, null); 
		
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
					throw XLSUtilities.getApplicationException(localisation, "tu_gm", rowNumSurvey, "survey", q.name, currentGroup);
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

		// 3. Column Name
		return q;
	}

	private void getLabels(Row row, int lastCellNum, HashMap<String, Integer> header, ArrayList<Label> labels) throws ApplicationException {

		// Get the label language values
		if(useDefaultLanguage) {
			Label lab = new Label();
			lab.text = XLSUtilities.getColumn(row, "label", surveyHeader, lastCellNum, null);
			lab.hint = XLSUtilities.getColumn(row, "hint", surveyHeader, lastCellNum, null);
			lab.image = XLSUtilities.getColumn(row, "image", surveyHeader, lastCellNum, null);
			lab.video = XLSUtilities.getColumn(row, "video", surveyHeader, lastCellNum, null);
			lab.audio = XLSUtilities.getColumn(row, "audio", surveyHeader, lastCellNum, null);
			labels.add(lab);
		} else {
			for(int i = 0; i < survey.languages.size(); i++) {
				String lang = survey.languages.get(i).name;
				Label lab = new Label();
				lab.text = XLSUtilities.getColumn(row, "label::" + lang, surveyHeader, lastCellNum, null);
				lab.hint = XLSUtilities.getColumn(row, "hint::" + lang, surveyHeader, lastCellNum, null);
				lab.image = XLSUtilities.getColumn(row, "image::" + lang, surveyHeader, lastCellNum, null);
				lab.video = XLSUtilities.getColumn(row, "video::" + lang, surveyHeader, lastCellNum, null);
				lab.audio = XLSUtilities.getColumn(row, "audio::" + lang, surveyHeader, lastCellNum, null);
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
		} else if(in.startsWith("select_one")) {
			type = "select1";
			q.list_name = in.substring("select_one".length() + 1).trim();
		} else if(in.startsWith("select_multiple")) {
			type = "select";
			q.list_name = in.substring("select_multiple".length() + 1).trim();
		} 
		
		return type;
	}

	private boolean convertVisible(String type) {
		boolean visible = true;
		if(type.equals("calculate")) {
			visible = false;
		}
		// TODO preloads

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
		}
	}
	
	private void validateSurvey() throws Exception {
		
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
		} else if (type.equals("begin repeat")) {
			out = "begin repeat";
		} else if (type.equals("end repeat")) {
			out = "end repeat";
		} else if (type.equals("begin group")) {
			out = "begin group";
		} else if (type.equals("end group")) {
			out = "end group";
		}
				
		return out;
	}
	
	private boolean getBooleanColumn(Row row, String name, HashMap<String, Integer> header, int lastCellNum) throws ApplicationException {
		String v = XLSUtilities.getColumn(row, name, header, lastCellNum, null); 
		
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
