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


//import org.apache.poi.hssf.usermodel.HSSFWorkbook;
//import org.apache.poi.ss.usermodel.Workbook;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.util.HashMap;
import java.util.ResourceBundle;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Question;
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

	HashMap<String, Integer> surveyHeader = null;
	HashMap<String, Integer> choicesHeader = null;

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
				
			}

			/*
			 * 2. Process the survey sheet
			 */
			
			survey.forms.add(getForm("main", 0, 0));

			if(survey.forms.get(0).questions.size() == 0) {
				throw new ApplicationException("There are no questions in this survey");
			}

		}

		/*
		 * 3, Process the settings sheet
		 */
		settingsSheet = wb.getSheet("settings");
		if(settingsSheet != null && settingsSheet.getPhysicalNumberOfRows() > 0) {


		}

		return survey;


	}

	/*
	 * Get the survey header and the choices header so we can identify all the languages up front
	 * This should work gracefully with a badly designed forms where there are inconsistent language
	 *  names between the survey and choices sheet
	 */
	private void getHeaders() {

		HashMap<String, String> langMap = new HashMap<String, String> ();

		while(rowNumSurvey < lastRowNumSurvey) {
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

				break;
			}
			
		}
			
		while(rowNumChoices < lastRowNumChoices) {
			Row row = choicesSheet.getRow(rowNumChoices++);
			if(row != null) {
				choicesHeader = XLSUtilities.getHeader(row);				
				
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
			rowNumChoices++;
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
	private Form getForm(String name, int parentFormIndex, int parentQuestionIndex) throws Exception {

		Form f = new Form(name, parentFormIndex, parentQuestionIndex);

		while(rowNumSurvey < lastRowNumSurvey) {

			Row row = surveySheet.getRow(rowNumSurvey);

			if(row != null) {
				Question q = getQuestion(row);
				if(q != null) {
					f.questions.add(q);
				}
			}
			rowNumSurvey++;
		}

		return f;
	}

	/*
	 * Get a question from the excel sheet
	 */
	private Question getQuestion(Row row) throws ApplicationException {

		Question q = new Question();

		int lastCellNum = row.getLastCellNum();

		String type = XLSUtilities.getColumn(row, "type", surveyHeader, lastCellNum, null);
		if(type == null) {
			return null;			// empty row
		}
		q.type = convertType(type);
		q.name = XLSUtilities.getColumn(row, "name", surveyHeader, lastCellNum, null);
		
		// Get the label language values
		if(useDefaultLanguage) {
			Label lab = new Label();
			lab.text = XLSUtilities.getColumn(row, "label", surveyHeader, lastCellNum, null);
			lab.hint = XLSUtilities.getColumn(row, "hint", surveyHeader, lastCellNum, null);
			lab.image = XLSUtilities.getColumn(row, "image", surveyHeader, lastCellNum, null);
			lab.video = XLSUtilities.getColumn(row, "video", surveyHeader, lastCellNum, null);
			lab.audio = XLSUtilities.getColumn(row, "audio", surveyHeader, lastCellNum, null);
			q.labels.add(lab);
		} else {
			for(int i = 0; i < survey.languages.size(); i++) {
				String lang = survey.languages.get(i).name;
				Label lab = new Label();
				lab.text = XLSUtilities.getColumn(row, "label::" + lang, surveyHeader, lastCellNum, null);
				lab.hint = XLSUtilities.getColumn(row, "hint::" + lang, surveyHeader, lastCellNum, null);
				lab.image = XLSUtilities.getColumn(row, "image::" + lang, surveyHeader, lastCellNum, null);
				lab.video = XLSUtilities.getColumn(row, "video::" + lang, surveyHeader, lastCellNum, null);
				lab.audio = XLSUtilities.getColumn(row, "audio::" + lang, surveyHeader, lastCellNum, null);
				q.labels.add(lab);
			}
		}
		q.source = "user";
		
		// Derived values
		q.visible = convertVisible(type);

		return q;
	}

	private String convertType(String in) {
		String out = in;
		if(in.equals("text")) {
			out = "string";
		}
		return out;
	}

	private boolean convertVisible(String type) {
		boolean visible = true;
		if(type.equals("calculate")) {
			visible = false;
		}
		// TODO preloads

		return visible;
	}
}
