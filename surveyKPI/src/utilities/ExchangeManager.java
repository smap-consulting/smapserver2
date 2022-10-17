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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.*;
import org.smap.model.FormDesc;
import org.smap.model.TableManager;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.FileDescription;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.TableColumn;

import com.opencsv.CSVReader;

import model.ExchangeColumn;
import model.ExchangeHeader;

/*
 * Handle import export of files
 * Handle the formats created by Smap Exports
 *     Worksheets containing form data are called "d_" formname
 *     Column containing a unique key for each record is called "prikey"
 *     Column linking to the unique key of a parent record is called "parkey"
 * Attempt to handle exports of Aggregate data from Google sheets    
 *     The names of worksheets will need to be changed before importing
 *          The main worksheet needs to be called "d_main"
 *          The other worksheets need "d_" prepended
 *     Column containing a unique key for each record is called "metainstanceid"
 *         Column linking to the unique key of a parent record is called "parentuid"
 */
public class ExchangeManager {
	
	private static Logger log =
			 Logger.getLogger(ExchangeManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	public static int MAX_EXPORT_MEDIA = 500;
	public static int  MAX_EXPORT = 10000;
	
	private ResourceBundle localisation;
	private String tz;
	
	Workbook wb = null;
	boolean isXLSX = false;
	
	public ExchangeManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	HashMap<String, String> surveyNames = null;
	
	public ArrayList<FileDescription> createExchangeFiles(
			Connection sd, 
			Connection connectionResults,
			String user,
			int sId,
			HttpServletRequest request,
			String dirPath,
			boolean superUser,
			boolean incMedia,
			int startRec,
			int endRec,
			ArrayList<String> responseMsg
			) throws Exception {
		
		wb = new SXSSFWorkbook(10);
		Sheet sheet = null;
		ArrayList<FileDescription> files = new ArrayList<FileDescription> ();
			
		String filename = "data.xlsx";
		String filePath = dirPath + "/" + filename;

		OutputStream outputStream = new FileOutputStream(dirPath + "/data.xlsx");
		files.add(new FileDescription(filename, filePath));
		
		HashMap<String, String> selMultChoiceNames = new HashMap<String, String> ();

		Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
		surveyNames = new HashMap<String, String> ();
		String basePath = null;
		String language = "none";
		
		String dateName = null;
		int dateForm = 0;
		if(sId != 0) {
			
			PreparedStatement pstmt2 = null;
			PreparedStatement pstmtSSC = null;
			PreparedStatement pstmtQType = null;
			PreparedStatement pstmtDateFilter = null;

			try {
				
				basePath = GeneralUtilityMethods.getBasePath(request);
				
				// Prepare the statement to get the question type and read only attribute
				String sqlQType = "select q.qtype, q.readonly from question q, form f " +
						" where q.f_id = f.f_id " +
						" and f.table_name = ? " +
						" and q.qname = ?;";
				pstmtQType = sd.prepareStatement(sqlQType);
				
				TableManager tm = new TableManager(localisation, tz);
				ArrayList <FormDesc> formList = tm.getFormList(sd, sId);
				
				/*
				 * Create a work sheet for each form
				 */
				String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				for(FormDesc f : formList) {
					
					sheet = wb.createSheet("d_" + f.name);	
					
					TableColumn c;
					int parentId = 0;
					if(f.parent > 0) {
						parentId = f.parent;
					}
					int level = getLevel(f, formList, 0);
					
					HashMap<String, String> selectMultipleColumnNames = new HashMap<String, String> ();
					
					// Get the list of table columns
					f.columnList = GeneralUtilityMethods.getColumnsInForm(
							sd,
							connectionResults,
							localisation,
							language,
							sId,
							surveyIdent,
							user,
							null,		// Roles to apply
							parentId,
							f.f_id,
							f.table_name,
							false,		// Don't include Read Only
							true,		// Include parent key
							false,		// Don't include "bad" columns
							false,		// Don't include instance id
							true,		// Include prikey
							true,		// Include other meta data
							true,		// Include preloads
							true,		// instancename
							false,		// Survey duration
							false,		// Case Management
							superUser,
							false,
							false,		// Don't include audit data
							tz,
							false,		// mgmt
							false,		// Accuracy and Altitude
							false		// server calculates
							);
						
					// Get the list of spreadsheet columns
					ArrayList<ExchangeColumn> cols = new ArrayList<> ();
					for(int j = 0; j < f.columnList.size(); j++) {

						c = f.columnList.get(j);
						
						//String name = c.column_name;
						String qType = c.type;
						String questionName;
						String optionName = null;
						
						// Hack for meta values use the column name as the question name may have been translated
						if(c.isMeta) {
							questionName = c.column_name;
						} else {
							questionName = c.question_name;
						} 
						
						if(qType.equals("select")) {
							optionName = c.option_name;

							selMultChoiceNames.put(c.column_name, optionName);		// Add the name of sql column to a look up table for the get data stage
							String n = selectMultipleColumnNames.get(questionName);
							if(n == null) {
								// New Select multiple
								selectMultipleColumnNames.put(questionName, questionName);		// Record that we have this select multiple
								addToHeader(sd, cols, "none", questionName, c.column_name, qType, sId, f,true);
							}
						} else {
							addToHeader(sd, cols, "none", questionName, c.column_name, qType, sId, f,true);
						}
						
						// Set the sql selection text for this column
						String selName = null;
						if(GeneralUtilityMethods.isGeometry(c.type)) {
							selName = "ST_AsTEXT(" + c.column_name + ") ";
						} else if(qType.equals("dateTime")) {	// Return all timestamps at UTC with no time zone
							selName = "timezone('UTC', " + c.column_name + ") as " + c.column_name;	
						} else {
							selName = c.column_name;
						}
						
						if(f.columns == null) {
							f.columns = selName;
						} else {
							f.columns += "," + selName;
						}
					}
					
					createHeader(cols, sheet, styles);
					
					try {
						getData(sd, 
								connectionResults, 
								formList, 
								f, 
								selMultChoiceNames,
								cols,
								sheet,
								styles,
								sId,
								null, 
								null, 
								dateName,
								dateForm,
								basePath,
								dirPath,
								files,
								incMedia,
								startRec,
								endRec,
								level,
								responseMsg);
						
					} catch(Exception e) {
						// Ignore errors if the only problem is that the tables have not been created yet
						if(e.getMessage() != null) {
							if(e.getMessage().contains("ERROR: relation") && e.getMessage().contains("does not exist")) {
								// all good
							} else {
								throw e;
							}
						} else {
							throw e;
						}
					}
					
				}
 
			} finally {
				
				try {if (pstmt2 != null) {pstmt2.close();	}} catch (SQLException e) {	}
				try {if (pstmtSSC != null) {pstmtSSC.close();	}} catch (SQLException e) {	}
				try {if (pstmtQType != null) {pstmtQType.close();	}} catch (SQLException e) {	}
				try {if (pstmtDateFilter != null) {pstmtDateFilter.close();	}} catch (SQLException e) {	}
				
			}
		}
		
		wb.write(outputStream);
		outputStream.close();
		
		// XLSX temporary streaming files need to be deleted
		((SXSSFWorkbook) wb).dispose();
		
		return files;
	}
	
	/*
	 * Get the level of a form
	 * Forms that are the child of a child cannot be filtered by record number
	 */
	private int getLevel(FormDesc f, ArrayList <FormDesc> formList, int level) {
		
		if(f.parent > 0) {
			level++;
			for(FormDesc fx : formList) {
				if(fx.f_id == f.parent) {					
					level =  getLevel(fx, formList, level);
					break;
				}
			}
		}
		return level;
	}

	
	/*
	 * Load data from a file into the form
	 */
	public int loadFormDataFromCsvFile(
			Connection sd,
			Connection results,
			PreparedStatement pstmtGetCol, 
			PreparedStatement pstmtGetColGS,
			PreparedStatement pstmtGetChoices,
			File file,
			FormDesc form,
			String sIdent,
			HashMap<String, File> mediaFiles,
			ArrayList<String> responseMsg,
			String basePath,
			ResourceBundle localisation,
			ArrayList<MetaItem> preloads,
			String importSource,
			Timestamp importTime,
			String serverName,
			SimpleDateFormat sdf,
			int oId
			) throws Exception {
		
		CSVReader reader = null;
		XlsReader xlsReader = null;
		
		int recordsWritten = 0;
		
		PreparedStatement pstmtDeleteExisting = null;
		ExchangeHeader eh = new ExchangeHeader();
		
		try {
			
			form.keyMap = new HashMap<String, String> ();
			pstmtGetCol.setInt(1, form.f_id);
			pstmtGetColGS.setInt(1, form.f_id);		// Prepare the statement to get column names for the form
			
			String [] line;
			reader = new CSVReader(new FileReader(file));
			line = reader.readNext();
			
			if(line != null && line.length > 0) {
				
				processHeader(
						sd,
						results,
						sIdent,
						pstmtGetCol, 
						pstmtGetChoices,
						pstmtGetColGS,
						responseMsg,
						preloads,
						eh, 
						line, 
						form);	
				
				if(eh.columns.size() > 0) {
					
					/*
					 * Get the data
					 */
					while (true) {
						
						line = reader.readNext();					
						if(line == null) {
							break;
						}
						
						recordsWritten += processRecord(sd, 
								eh, 
								line, 
								form, 
								importSource, 
								importTime, 
								responseMsg,
								serverName,
								basePath,
								sIdent,
								mediaFiles,
								sdf,
								recordsWritten,
								oId);

												
				    }
					results.commit();
					
				} else {
					responseMsg.add(
							localisation.getString("pk_nq") + " " + form.name);
				}
				
			}
		} finally {
			
			try{if(xlsReader != null) {xlsReader.close();}} catch (Exception e) {}
			//try{if(fis != null) {fis.close();}} catch (Exception e) {}
			try {if (reader != null) {reader.close();}} catch (Exception e) {}
			
			try {if (eh.pstmtInsert != null) {eh.pstmtInsert.close();}} catch (Exception e) {}
			try {if (pstmtDeleteExisting != null) {pstmtDeleteExisting.close();}} catch (Exception e) {}
		}
		
		log.info("Records written: " + recordsWritten);
		
		return recordsWritten;
	}

	/*
	 * Create a header row and set column widths
	 */
	private void createHeader(
			ArrayList<ExchangeColumn> cols, 
			Sheet sheet, 
			Map<String, CellStyle> styles) {
				
		// Create survey sheet header row
		Row headerRow = sheet.createRow(0);
		CellStyle headerStyle = styles.get("header");
		for(int i = 0; i < cols.size(); i++) {
			ExchangeColumn col = cols.get(i);
			
            Cell cell = headerRow.createCell(i);
            cell.setCellStyle(headerStyle);
            cell.setCellValue(col.humanName);
        }
	}
	
	
	/*
	 * Add to the header
	 */
	private void addToHeader(Connection sd, 
			ArrayList<ExchangeColumn> cols, 
			String language, 
			String human_name, 
			String colName, 
			String qType, 
			int sId, 
			FormDesc f,
			boolean merge_select_multiple) throws SQLException {
		
		if(qType != null && qType.equals("geopoint")) {
			cols.add(new ExchangeColumn(human_name + "__lat"));
			cols.add(new ExchangeColumn(human_name + "__lon"));
		} else {
			ExchangeColumn col = new ExchangeColumn(human_name);
			if(qType.equals("image")) {
				// TODO ??
			}
			cols.add(col);
			
		}
		
	}
	


	/*
	 * Get the longitude and latitude from a WKT POINT
	 */
	private String [] getLonLat(String point) {
		String [] coords = null;
		int idx1 = point.indexOf("(");
		int idx2 = point.indexOf(")");
		if(idx2 > idx1) {
			String lonLat = point.substring(idx1 + 1, idx2);
			coords = lonLat.split(" ");
		}
		return coords;
	}
	
	
	private boolean notEmpty(String v) {
		if(v == null || v.trim().length() == 0) {
			return false;
		} else {
			return true;
		}
	}
	/*
	 * Write out the data
	 */
	private void getData(Connection sd, 
			Connection connectionResults, 
			ArrayList<FormDesc> formList, 
			FormDesc f,
			HashMap<String, String> choiceNames,
			ArrayList<ExchangeColumn> cols, 
			Sheet sheet, 
			Map<String, CellStyle> styles,
			int sId,
			Date startDate,
			Date endDate,
			String dateName,
			int dateForm,
			String basePath,
			String dirPath,
			ArrayList<FileDescription> files,
			boolean incMedia,
			int startRec,
			int endRec,
			int level,
			ArrayList<String> responseMsg) throws Exception {
		
		StringBuffer sql = new StringBuffer();
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		
		sql.append("select ")
			.append(f.columns)
			.append(" from ")
			.append(f.table_name)
			.append(" where _bad is false"); 
		/*
		 * Can only apply record filtering to first two levels
		 */
		ArrayList<Integer> params = new ArrayList<Integer> ();
		if(level == 0) {
			if(startRec > 0) {
				sql.append(" and prikey >= ?");
				params.add(startRec);
			}
			if(endRec > 0) {
				sql.append(" and prikey <= ?");
				params.add(endRec);
			}
		} else if(level == 1) {
			if(startRec > 0) {
				sql.append(" and parkey >= ?");
				params.add(startRec);
			}
			if(endRec > 0) {
				sql.append(" and parkey <= ?");
				params.add(endRec);
			}
		}
		sql.append(" order by prikey asc");

		try {
			pstmt = connectionResults.prepareStatement(sql.toString());
			int paramIdx = 1;
			for(int p : params) {
				pstmt.setInt(paramIdx++, p);
			}
			log.info("Get data: " + pstmt.toString());
			
			sd.setAutoCommit(false);		// page the results to reduce memory usage
			pstmt.setFetchSize(100);	
			
			resultSet = pstmt.executeQuery();
			
			int rowIndex = 1;
			while (resultSet.next()) {	
				Row row = sheet.createRow(rowIndex);
				
				int colIndex = 0;	// Column index
				String currentSelectMultipleQuestionName = null;
				String multipleChoiceValue = null;
				for(int i = 0; i < f.columnList.size(); i++) {
					
					TableColumn c = f.columnList.get(i);

					String columnName = c.column_name;
					String columnType = c.type;
					String value = resultSet.getString(i + 1);
					boolean writeValue = true;
					
					if(value == null) {
						value = "";	
					}
					
					String choice = choiceNames.get(columnName);
					if(choice != null) {
						// Have to handle merge of select multiple
						String selectMultipleQuestionName = columnName.substring(0, columnName.indexOf("__"));
						if(currentSelectMultipleQuestionName == null) {
							currentSelectMultipleQuestionName = selectMultipleQuestionName;
							multipleChoiceValue = XLSUtilities.updateMultipleChoiceValue(value, choice, multipleChoiceValue);
							writeValue = false;
						} else if(currentSelectMultipleQuestionName.equals(selectMultipleQuestionName) && (i != f.columnList.size() - 1)) {
							// Continuing on with the same select multiple and its not the end of the record
							multipleChoiceValue = XLSUtilities.updateMultipleChoiceValue(value, choice, multipleChoiceValue);
							writeValue = false;
						} else if (i == f.columnList.size() - 1) {
							//  Its the end of the record		
							multipleChoiceValue = XLSUtilities.updateMultipleChoiceValue(value, choice, multipleChoiceValue);
							value = multipleChoiceValue;
						} else {
							// A second select multiple directly after the first - write out the previous
							String newMultipleChoiceValue = value;
							value = multipleChoiceValue;
					
							// Restart process for the new select multiple
							currentSelectMultipleQuestionName = selectMultipleQuestionName;
							multipleChoiceValue = null;
							multipleChoiceValue = XLSUtilities.updateMultipleChoiceValue(newMultipleChoiceValue, choice, multipleChoiceValue);
						}
					} else {
						// Write out the previous multiple choice value before continuing with the non multiple choice value
						if(currentSelectMultipleQuestionName != null) {
							ArrayList<String> values = getContent(sd, multipleChoiceValue, false, 
									currentSelectMultipleQuestionName, "select", responseMsg);
							for(int j = 0; j < values.size(); j++) {
								writeValue(row, colIndex++, values.get(j), sheet, styles);
							}
						}
						
						// Restart Select Multiple Process
						multipleChoiceValue = null;
						currentSelectMultipleQuestionName = null;
					}
					
					if(c.isAttachment() && value != null && value.trim().length() > 0) {
						
						if(incMedia) {
							// Path to attachment
							String attachmentPath = basePath + "/" + value;
						
							// Get name
							int idx = value.lastIndexOf('/');
							if(idx > -1) {
								value = value.substring(idx + 1);
							}
						
							// Copy file to temporary zip folder

							File source = new File(attachmentPath);
							if (source.exists()) {
								String newPath = dirPath + "/" + value;
								File dest = new File(newPath);
								try {
									FileUtils.copyFile(source, dest);				
									files.add(new FileDescription(value, newPath));
								} catch (Exception e) {
									log.info("Error: Failed to add file " + source + " to exchange export. " + e.getMessage());
								}
							} else {
								log.info("Error: media file does not exist: " + attachmentPath);
							}
						} 
						
					}
					
					if(writeValue) {
						ArrayList<String> values = getContent(sd, value, false, columnName, columnType, responseMsg);
						for(int j = 0; j < values.size(); j++) {
							writeValue(row, colIndex++, values.get(j), sheet, styles);
						}
					}
				}
				rowIndex++;
				
			}
			
			
		} finally {
			sd.setAutoCommit(true);
			if(resultSet != null) try {resultSet.close();} catch(Exception e) {};
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		
	}
	
	/*
	 * Create a header row and set column widths
	 */
	private void writeValue(
			Row row,
			int idx,
			String value, 
			Sheet sheet, 
			Map<String, CellStyle> styles) throws IOException {
		
		CellStyle style = styles.get("default");		
			
        Cell cell = row.createCell(idx);        
		cell.setCellStyle(style);
        
		// String cell values are limited to 32767 characters
		if(value == null) {
			value = "";
		}
		if(value.length() > 32767) {
			value = value.substring(0, 32763) + "...";
		}
		
        cell.setCellValue(value);


	}
	
	/*
	 * Return the text
	 */
	private ArrayList<String> getContent(Connection con, String value, boolean firstCol, String columnName,
			String columnType,
			ArrayList<String> responseMsg) throws NumberFormatException, SQLException {

		ArrayList<String> out = new ArrayList<String>();
		if(value == null) {
			value = "";
		}
		
		if(columnType.equals("geopoint")) {

			String coords [] = getLonLat(value);

			if(coords != null && coords.length > 1) { 
				out.add(coords[1]);
				out.add(coords[0]); 
			} else {
				out.add(value);
				out.add(value);
			}
				
			
		} else if(value.startsWith("POLYGON") || value.startsWith("LINESTRING")) {
			
			// Can't split linestrings and polygons so just remove the POLYGON or LINESTRING wrapper
			int idx = value.lastIndexOf('(');
			int idx2 = value.indexOf(')');
			if(idx2 > idx && idx > -1) {
				out.add(value.substring(idx + 1, idx2));
			} else {
				out.add("");
			}
			if(idx < 0 || idx2 < 0 || idx2 < idx) {
				responseMsg.add(
						localisation.getString("imp_mfg") +
						" " + value);
			}

			
		} else if(columnName.equals("_device")) {
			out.add(value);				
		} else if(columnName.equals("_complete")) {
			out.add(value.equals("f") ? "No" : "Yes"); 
				
		} else if(columnName.equals(SmapServerMeta.SURVEY_ID_NAME)) {
			String displayName = surveyNames.get(out);
			if(displayName == null) {
				try {
					displayName = GeneralUtilityMethods.getSurveyName(con, Integer.parseInt(value));
				} catch (Exception e) {
					displayName = "";
				}
				surveyNames.put(value, displayName);
			}
			out.add(displayName); 
				
		} else if(columnType.equals("dateTime")) {
			// Convert the timestamp to the excel format specified in the xl2 mso-format
			int idx1 = out.indexOf('.');	// Drop the milliseconds
			if(idx1 > 0) {
				value = value.substring(0, idx1);
			} 
			out.add(value);
		} else {
			out.add(value);
		}

		return out;
	}
	
	/*
	 * Check to see if a question is in a form
	 */
	private ExchangeColumn getColumn(
			Connection sd,
			Connection cResults,
			String sIdent,
			int fId,
			String tableName,
			PreparedStatement pstmtGetCol, 
			PreparedStatement pstmtGetChoices, 
			String qName,
			ArrayList<ExchangeColumn> columns,
			ArrayList<String> responseMsg,
			ResourceBundle localisation,
			ArrayList<MetaItem> preloads,
			int index,
			boolean incTwiceMsg
			) throws SQLException {
		
		ExchangeColumn col = null;
		String geomCol = null;
		String geomColumnName = null;
		int sId;
		int latIndex = -1;
		int lonIndex = -1;
		
		// Cater for lat, lon columns which map to a geopoint
		// This is the old format which assumes a single geometry in a form
		if(qName.equals("lat") || qName.equals("lon") 
				|| qName.equals("plotgpsLatitude") || qName.equals("plotgpsLongitude")) {
			sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
			geomColumnName = GeneralUtilityMethods.getGeomColumnOfType(sd, sId, fId, "geopoint");
			if(qName.equals("lat") || qName.equals("lon")) {
				qName = GeneralUtilityMethods.getGeomNameOfType(sd, sId, fId, "geopoint");
			}
			geomCol = qName;
			if(qName.equals("lat")) {
				latIndex = index;
			} else {
				lonIndex = index;
			}
		} 
		
		// Handle new geopoints which append the __lat and __lon values
		if(qName.endsWith("__lat") || qName.endsWith("__lon")) {
			int idx = qName.lastIndexOf("__");
			
			if(qName.endsWith("__lat")) {
				latIndex = index;
			} else {
				lonIndex = index;
			}
			
			qName = qName.substring(0, idx);
			geomCol = qName;
		}
		
		// Only add this question if it has not previously been added, questions can only be updated once in a single transaction
		for(ExchangeColumn haveColumn : columns) {
			if(haveColumn.name.equals(qName)) {
				// Add the geopoint column information
				if(latIndex >= 0) {
					haveColumn.latIndex = latIndex;
				} else if(lonIndex >= 0) {
					haveColumn.lonIndex = lonIndex;
				} else if(incTwiceMsg) {
					responseMsg.add(
							localisation.getString("mf_col") +
							" " + qName + " " +
							localisation.getString("imp_i2"));
				}
				return null;
			}
		}
			
		if(qName.equals("prikey") || qName.equals("metainstanceid")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "prikey";
			col.type = "int";
			col.write = false;					// Don't write the primary key a new one will be generated
		} else if(qName.equals("parkey") || qName.equals("parentuid")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "parkey";
			col.type = "int";
		} else if(qName.equals("Key") || qName.equals("_hrk")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_hrk";
			col.type = "string";
		} else if(qName.equals("User") || qName.equals("_user")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_user";
			col.type = "string";
		} else if(qName.equals("Survey Name") || qName.equals(SmapServerMeta.SURVEY_ID_NAME)) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = SmapServerMeta.SURVEY_ID_NAME;
			col.type = "int";
		} else if(qName.equals("Survey Notes") || qName.equals("_survey_notes")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_survey_notes";
			col.type = "int";
		} else if(qName.equals("Location Trigger") || qName.equals("_location_trigger")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_location_trigger";
			col.type = "int";
		} else if(qName.equals("Upload Time") || qName.equals(SmapServerMeta.UPLOAD_TIME_NAME) || qName.equals("metasubmissiondate")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = SmapServerMeta.UPLOAD_TIME_NAME;
			col.type = "dateTime";
		} else if(qName.equals(SmapServerMeta.SCHEDULED_START_NAME)) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = SmapServerMeta.SCHEDULED_START_NAME;
			col.type = "dateTime";
		} else if(qName.equals("Version") || qName.equals("_version")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_version";
			col.type = "int";
		} else if(qName.equals("Complete") || qName.equals("_complete") || qName.equals("metaiscomplete")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_complete";
			col.type = "boolean";
		} else if(qName.equals("Instance Name") || qName.equals("instancename")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "instancename";
			col.type = "string";
		} else if(qName.toLowerCase().equals("instanceid")) {
			// Don't add a column, instanceid is added by default, however record the column for this data
		} else if(qName.equals("plotgpsAltitude")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = geomColumnName + "_alt";
			col.type = "decimal";
		} else if(qName.equals("plotgpsAccuracy")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = geomColumnName + "_acc";
			col.type = "decimal";
		} else if(qName.equals("_alert")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_alert";
			col.type = "string";
		} else if(qName.equals("_thread_created")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_thread_created";
			col.type = "dateTime";
		} else if(qName.equals("_case_closed")) {
			col = new ExchangeColumn();
			col.name = qName;
			col.columnName = "_case_closed";
			col.type = "dateTime";
		} else {
			pstmtGetCol.setString(2, qName.toLowerCase());		// Search for a question
			ResultSet rs = pstmtGetCol.executeQuery();
			if(rs.next()) {
				// This column name is in the survey
				col = new ExchangeColumn();
				col.name = qName;
				col.columnName = rs.getString("column_name");
				if(geomCol != null) {
					col.geomCol = geomCol;				// This column holds the latitude, longitude, Altitude, Accuracy or none of these
				}

				col.type = rs.getString("qtype");
				if(latIndex >= 0) {
					col.latIndex = latIndex;
				} else {
					col.lonIndex = lonIndex;
				}

				if(col.type.startsWith("select")) {

					// Get choices for this select question
					int qId = rs.getInt("q_id");

					col.choices = new ArrayList<Option> ();
					pstmtGetChoices.setInt(1, qId);
					log.info("Get choices:" + pstmtGetChoices.toString());
					ResultSet rsChoices = pstmtGetChoices.executeQuery();
					while(rsChoices.next()) {
						Option o = new Option();
						o.columnName = rsChoices.getString("column_name");
						o.value = rsChoices.getString("ovalue");
						col.choices.add(o);
					}
				}
			} else {
				// Check to see if it is a preload

				for(MetaItem mi : preloads) {
					if(mi.isPreload) {
						if(mi.name.equals(qName)) {
							col = new ExchangeColumn();
							col.name = qName;
							col.columnName = mi.columnName;
							col.type = mi.type;
							break;
						}
					}
				}
			}
		}

		if(col == null && incTwiceMsg) {
			responseMsg.add(
					localisation.getString("imp_qn") +
					" " + qName + " " +
					localisation.getString("imp_nfi") +
					": " + tableName);  
		}
	
		return col;
	}
	
	public void processHeader(
			Connection sd,
			Connection results,
			String sIdent,
			PreparedStatement pstmtGetCol,
			PreparedStatement pstmtGetChoices,
			PreparedStatement pstmtGetColGS,
			ArrayList<String> responseMsg,
			ArrayList<MetaItem> preloads,
			ExchangeHeader eh, 
			String [] line, 
			FormDesc form) throws Exception {
		/*
		 * Get the columns in the file that are also in the form
		 * Assume first line is the header
		 */
		for(int i = 0; i < line.length; i++) {
			String colName = line[i].replace("'", "''");	// Escape apostrophes
			
			if(colName.trim().length() > 0) {
				if(colName.toLowerCase().equals("instanceid")) {
					eh.instanceIdColumn = i;
				}
				// If this column is in the survey then add it to the list of columns to be processed
				// Do this test for a load from excel
				ExchangeColumn col = getColumn(sd, results, sIdent, form.f_id, form.table_name, pstmtGetCol, 
						pstmtGetChoices, colName, eh.columns, 
						responseMsg, localisation, preloads, i, true);
				if(col != null) {
					col.index = i;
					eh.columns.add(col);
				} else {
					// Perform test for a load from a google sheets export
					col = getColumn(sd, results, sIdent, form.f_id, form.table_name, pstmtGetColGS, pstmtGetChoices, colName, eh.columns, responseMsg, localisation, preloads, i, false);
					if(col != null) {
						col.index = i;
						eh.columns.add(col);
					} 
				}
			}

		}
		
		log.info("Loading data from " + eh.columns.size() + " columns out of " + line.length + " columns in the data file");
		
		if(eh.columns.size() > 0) {
				
			/*
			 * Add the source column if it is not already in the results table
			 */
			if(!GeneralUtilityMethods.hasColumn(results, form.table_name, "_import_source")) {
				GeneralUtilityMethods.addColumn(results, form.table_name, "_import_source", "text");
			}
			if(!GeneralUtilityMethods.hasColumn(results, form.table_name, "_import_time")) {
				GeneralUtilityMethods.addColumn(results, form.table_name, "_import_time", "timestamp with time zone");
			}
			/*
			 * Create the insert statement
			 */		
			StringBuffer sqlInsert = new StringBuffer("insert into " + form.table_name + "(");
			sqlInsert.append("_import_source, _import_time");
			if(form.parent == 0) {
				sqlInsert.append(",instanceid");
			}
			
			for(int i = 0; i < eh.columns.size(); i++) {						
				ExchangeColumn col = eh.columns.get(i);						
				if(col.write) {
					sqlInsert.append(",").append(col.columnName);
				} 
			}			
			
			/*
			 * Add place holders for the data
			 */
			sqlInsert.append(") values("); 
			sqlInsert.append("?, ?");			// _import_source and _import_time
			if(form.parent == 0) {
				sqlInsert.append(",?");		// instanceid
			}
			
			
			for(int i = 0; i < eh.columns.size(); i++) {
				
				ExchangeColumn col = eh.columns.get(i);
				
				if(col.write) {
					if(col.type.equals("geoshape")) {
						sqlInsert.append(",").append("ST_GeomFromText('POLYGON((' || ? || '))', 4326)");
					
					} else if(col.type.equals("geotrace") || col.type.equals("geocompound")) {
						sqlInsert.append(",").append("ST_GeomFromText('LINESTRING(' || ? || ')', 4326)");
					
					}  else if(col.type.equals("geopoint")) {
						sqlInsert.append(",").append("ST_GeomFromText('POINT(' || ? || ' ' || ? ||')', 4326)");
					
					} else {
						sqlInsert.append(",").append("?");
					}
				}
			}
			
			sqlInsert.append(")");
			
			eh.pstmtInsert = results.prepareStatement(sqlInsert.toString(), Statement.RETURN_GENERATED_KEYS);
		} else {
			throw new Exception(localisation.getString("pk_nq"));
		}
	}

	public int processRecord(
			Connection sd,
			ExchangeHeader eh, 
			String [] line, 
			FormDesc form,
			String importSource,
			Timestamp importTime,
			ArrayList<String> responseMsg,
			String serverName,
			String basePath,
			String sIdent,
			HashMap<String, File> mediaFiles,
			SimpleDateFormat sdf,
			int recordsWritten,
			int oId) throws SQLException {
		
		int index = 1;
		int count = 0;
		String prikey = null;
		boolean writeRecord = true;
		eh.pstmtInsert.setString(index++, importSource);
		eh.pstmtInsert.setTimestamp(index++, importTime);
		if(form.parent == 0) {
			String instanceId = null;
			if(eh.instanceIdColumn >= 0) {
				instanceId = line[eh.instanceIdColumn].trim();
			}
			if(instanceId == null || instanceId.trim().length() == 0) {
				instanceId = "uuid:" + String.valueOf(UUID.randomUUID());
			}
			eh.pstmtInsert.setString(index++, instanceId);
		} 
		
		for(int i = 0; i < eh.columns.size(); i++) {
			ExchangeColumn col = eh.columns.get(i);
			
			// ignore empty columns at end of line
			if(col.index >= line.length) {
				
				if(col.type.equals("int")) {
					eh.pstmtInsert.setInt(index++, 0);
				} else if(col.type.equals("decimal")) {
					eh.pstmtInsert.setDouble(index++, 0.0);
				} else if(col.type.equals("date")) {
					eh.pstmtInsert.setDate(index++, null);
				} else if(col.type.equals("dateTime")) {
					eh.pstmtInsert.setTimestamp(index++, null);
				} else if(col.type.equals("time")) {
					eh.pstmtInsert.setTime(index++, null);
				} else {
					eh.pstmtInsert.setString(index++, null);
				}
				continue;
			}
			

			if(col.type.equals("geopoint") && col.lonIndex >=0 && col.latIndex >= 0) {
				String lon = null;
				String lat = null;
				if(col.lonIndex < line.length && col.latIndex < line.length) {
					lon = line[col.lonIndex];
					lat = line[col.latIndex];
				}
				if(lon == null || lon.length() == 0) {
					lon = "0.0";
				}
				if(lat == null || lat.length() == 0) {
					lat = "0.0";
				}
				eh.pstmtInsert.setString(index++, lon);
				eh.pstmtInsert.setString(index++, lat);
			} else {
				
				String value = line[col.index].trim();	
				
				if(col.name.equals("prikey") || col.name.equals("metainstanceid")) {
					prikey = value;
				} else if(col.name.equals("parkey") || col.name.equals("parentuid")) {
					if(form.parent == 0) {
						eh.pstmtInsert.setInt(index++, 0);
					} else {
						String parkey = value;
						String newParKey = form.parentForm.keyMap.get(parkey);
						int iParKey = -1;
						try {iParKey = Integer.parseInt(newParKey); } catch (Exception e) {}
						if(newParKey == null || iParKey == -1) {
							responseMsg.add(
									localisation.getString("pk_nf") +
									" " + parkey + " " +
									localisation.getString("pk_if") +
									" " + form.name);
							writeRecord = false;
						} else {
							eh.pstmtInsert.setInt(index++, iParKey);
						}
					}
				} else if(GeneralUtilityMethods.isAttachmentType(col.type)) {
					
					// If the data references a media file then process the attachment
					File srcPathFile = null;
					String srcUrl = null;
					if(value != null && (value.trim().startsWith("https://") || value.trim().startsWith("http://"))) {
						
						// If the link is to a file on the same server (or this is localhost) do not duplicate the media
						value = value.trim();
						String serverHttpsUrl = "https://" + serverName + "/";
						String serverHttpUrl = "http://" + serverName + "/";
						if(serverName.equals("localhost") || value.startsWith(serverHttpUrl) || value.startsWith(serverHttpsUrl)) {
							int idx = value.indexOf(serverName) + serverName.length();
							value = value.substring(idx);
						} else {
							// Get the attachment from the link so it can be loaded
							srcUrl = value;
							value = UUID.randomUUID().toString();	// Create a random name for the initial download
						}
					} else {
						// Attachment should have been loaded with the zip file
						srcPathFile = mediaFiles.get(value);
					}
					
					// Copy the attachments to the target location and get the new name
					if(srcPathFile != null || srcUrl != null) {
						value = GeneralUtilityMethods.createAttachments(
							sd,
							value, 
							srcPathFile, 
							basePath, 
							sIdent,
							srcUrl,
							null,
							oId);
					}
					if(value != null && value.trim().length() == 0) {
						value = null;
					}
					eh.pstmtInsert.setString(index++, value);
				} else if(col.type.equals("int")) {
					int iVal = 0;
					if(notEmpty(value)) {
						try { iVal = Integer.parseInt(value);} catch (Exception e) {}
					}
					eh.pstmtInsert.setInt(index++, iVal);
				} else if(col.type.equals("decimal") || col.type.equals("range")) {
					double dVal = 0.0;
					if(notEmpty(value)) {
						try { dVal = Double.parseDouble(value);} catch (Exception e) {}
					}
					eh.pstmtInsert.setDouble(index++, dVal);
				} else if(col.type.equals("boolean")) {
					boolean bVal = false;
					if(notEmpty(value)) {
						try { bVal = Boolean.parseBoolean(value);} catch (Exception e) {}
					}
					eh.pstmtInsert.setBoolean(index++, bVal);
				} else if(col.type.equals("date")) {
					Date dateVal = null;
					if(notEmpty(value)) {
						try {
							dateVal = Date.valueOf(value); 
							
						} catch (Exception e) {
							try {
								java.util.Date uDate = sdf.parse(value);		
								dateVal = new Date(uDate.getTime());
							} catch (Exception ex) {
								log.info("Error parsing date: " + col.columnName + " : " + value + " : " + e.getMessage());
							}
						}
					}
					eh.pstmtInsert.setDate(index++, dateVal);
				} else if(col.type.equals("dateTime")) {
					Timestamp tsVal = null;
					if(notEmpty(value)) {
						try {
							java.util.Date uDate = sdf.parse(value);
							tsVal = new Timestamp(uDate.getTime());
						} catch (Exception e) {
							try {
								java.util.Date uDate = sdf.parse(value);		
								tsVal = new Timestamp(uDate.getTime());
							} catch (Exception ex) {
								log.info("Error parsing datetime: " + value + " : " + e.getMessage());
							}
						}
					}
					
					eh.pstmtInsert.setTimestamp(index++, tsVal);
				} else if(col.type.equals("time")) {
					
					int hour = 0;
					int minute = 0;
					int second = 0;
					if(notEmpty(value)) {
						try {
							String [] tVals = value.split(":");
							if(tVals.length > 0) {
								hour = Integer.parseInt(tVals[0]);
							}
							if(tVals.length > 1) {
								minute = Integer.parseInt(tVals[1]);
							}
							if(tVals.length > 2) {
								second = Integer.parseInt(tVals[2]);
							}
						} catch (Exception e) {
							log.info("Error parsing datetime: " + value + " : " + e.getMessage());
						}
					}
					
					Time tVal = new Time(hour, minute, second);
					eh.pstmtInsert.setTime(index++, tVal);
				} else if(col.type.equals("geoshape") || col.type.equals("geotrace") || col.type.equals("geocompound")) {
					if(!notEmpty(value)) {		
						value = null;
					} else if(value.endsWith("...")) {
						value = null;		// An overlong invalid geometry so ignore it
					}
					eh.pstmtInsert.setString(index++, value);
				} else {
					eh.pstmtInsert.setString(index++, value);
				}
				
			}
		}
		
		if(writeRecord) {
			if(recordsWritten == 0) {
				log.info("Inserting first record: " + eh.pstmtInsert.toString());
			}
			eh.pstmtInsert.executeUpdate();
		
			ResultSet rs = eh.pstmtInsert.getGeneratedKeys();
			if(rs.next()) {
				form.keyMap.put(prikey, rs.getString(1));
			}
			count++;
		}
		
		return count;
	}
}
