package org.smap.sdal.managers;

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

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.QueryGenerator;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.constants.SmapExportTypes;
import org.smap.sdal.model.ColDesc;
import org.smap.sdal.model.ColValues;
import org.smap.sdal.model.OptionDesc;
import org.smap.sdal.model.QueryForm;
import org.smap.sdal.model.ReadData;
import org.smap.sdal.model.SqlDesc;


/*
 * Reports requested from a service
 */

public class XLSXReportsManager {
	
	private static Logger log =
			 Logger.getLogger(XLSXReportsManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	
	public XLSXReportsManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Create the new style XLSX report
	 */
	public Response getNewReport(
			Connection sd,
			Connection cResults,
			String username,
			String scheme,
			String serverName,
			String basePath,
			OutputStream outputStream,
			int sId, 
			String sIdent,
			boolean split_locn, 
			boolean get_acc_alt,
			boolean merge_select_multiple,
			String language,
			boolean exp_ro,
			boolean embedImages,
			boolean excludeParents,
			boolean hxl,
			int fId,
			Date startDate,
			Date endDate,
			int dateId,
			String filter,
			boolean meta,
			String tz) {
		
		Response responseVal = null;

		HashMap<ArrayList<OptionDesc>, String> labelListMap = new  HashMap<ArrayList<OptionDesc>, String> ();
		HashMap<String, String> surveyNames = new HashMap<String, String> ();

		String urlprefix = scheme + "://" + serverName + "/";		

		lm.writeLog(sd, sId, username, LogManager.VIEW, "Export as: xlsx", 0, serverName);
		
		if(sId != 0) {

			PreparedStatement pstmt = null;
			SXSSFWorkbook wb = null;
			int rowNumber = 0;
			Sheet dataSheet = null;
			Sheet settingsSheet = null;
			CellStyle errorStyle = null;

			try {	

				if(language == null) {	// ensure a language is set
					language = "none";
				}

				String surveyName = GeneralUtilityMethods.getSurveyName(sd, sId);
				
				/*
				 * Get the list of forms and surveys to be exported
				 */
				QueryManager qm = new QueryManager();				
				ArrayList<QueryForm> queryList = qm.getFormList(sd, sId, fId);		// Get a form list for this survey / form combo

				QueryForm startingForm = qm.getQueryTree(sd, queryList);	// Convert the query list into a tree

				
				/*
				 * Create XLSX File
				 */
				log.info("####################### Create XLSX file");
				wb = new SXSSFWorkbook(10);		// Serialised output
				Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
				CellStyle headerStyle = styles.get("header");
				errorStyle = styles.get("error");
						
				dataSheet = wb.createSheet(localisation.getString("rep_data"));
				settingsSheet = wb.createSheet(localisation.getString("rep_settings"));
				
				/*
				 * Populate settings sheet
				 */
				log.info("####################### Populate settings sheet: ");
				int settingsRowIdx = 0;
				Row settingsRow = settingsSheet.createRow(settingsRowIdx++);
				Cell sk = settingsRow.createCell(0);
				Cell sv = settingsRow.createCell(1);
				sk.setCellStyle(headerStyle);	
				sk.setCellValue(localisation.getString("a_tz"));
				sv.setCellValue(tz);
				
				settingsRow = settingsSheet.createRow(settingsRowIdx++);
				sk = settingsRow.createCell(0);
				sv = settingsRow.createCell(1);	
				sk.setCellValue(localisation.getString("a_dfq"));
				sv.setCellStyle(headerStyle);	
				sv.setCellValue(GeneralUtilityMethods.getQuestionNameFromId(sd, sId, dateId));
				
				settingsRow = settingsSheet.createRow(settingsRowIdx++);
				sk = settingsRow.createCell(0);
				sv = settingsRow.createCell(1);	
				sk.setCellValue(localisation.getString("a_st"));
				sv.setCellStyle(styles.get("date"));	
				if(startDate != null) {
					sv.setCellValue(startDate);
				}
				
				settingsRow = settingsSheet.createRow(settingsRowIdx++);
				sk = settingsRow.createCell(0);
				sv = settingsRow.createCell(1);
				sk.setCellValue(localisation.getString("a_et"));
				sv.setCellStyle(styles.get("date"));	
				if(endDate != null) {
					sv.setCellValue(endDate);
				}
				
				// Get the SQL for this query
				SqlDesc sqlDesc = QueryGenerator.gen(sd, 
						cResults,
						localisation,
						sId,
						sIdent,
						fId,
						language, 
						SmapExportTypes.XLSX, 
						urlprefix, 
						true,
						exp_ro,
						excludeParents,
						labelListMap,
						false,
						false,
						null,
						null,
						null,
						username,
						startDate,
						endDate,
						dateId,
						false,				// Super user - always apply filters
						startingForm,
						filter,
						meta,
						false,
						tz,
						null,			// geomQuestion
						true,			// Outer join of tables
						get_acc_alt);				
				
				// Populate data sheet
				rowNumber = 0;		
				
				/*
				 * Write the labels if language has been set
				 */
				if(language != null && !language.equals("none")) {
					Row headerRow = dataSheet.createRow(rowNumber++);				
					int colNumber = 0;
					int dataColumn = 0;
					while(dataColumn < sqlDesc.column_details.size()) {
						ColValues values = new ColValues();
						ColDesc item = sqlDesc.column_details.get(dataColumn);
						
						dataColumn = GeneralUtilityMethods.getColValues(
								null, 
								values, 
								dataColumn,
								sqlDesc.column_details, 
								merge_select_multiple,
								surveyName);	
						
						// Add literacy specific columns if required
						if(item.qType != null && item.qType.equals("select") && item.literacy) {
							for(int i = 0; i < 4; i++) {
								Cell cell = headerRow.createCell(colNumber++);
								cell.setCellStyle(headerStyle);
								cell.setCellValue("");
							}
						}
						
						if(split_locn && item.qType != null && item.rawQuestionType.equals("geopoint")) {
							Cell cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(values.label);
							
							cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(values.label);
						} else if(item.qType != null && item.qType.equals("select") && !merge_select_multiple && item.choices != null &&  item.compressed) {
							for(int i = 0; i < item.choices.size(); i++) {
								Cell cell = headerRow.createCell(colNumber++);
								cell.setCellStyle(headerStyle);
								String label = item.choices.get(i).k;
								if(i < item.optionLabels.size()) {
									label = item.optionLabels.get(i).label;
								}
								cell.setCellValue(values.label + " - " + label);
							}
						} else if(item.qType != null && item.qType.equals("rank") && !merge_select_multiple && item.choices != null) {
							for(int i = 0; i < item.choices.size(); i++) {
								Cell cell = headerRow.createCell(colNumber++);
								cell.setCellStyle(headerStyle);
								cell.setCellValue(values.label + " - " + (i + 1));
							}
						} else if(item.qType != null && item.qType.equals("select1") && item.optionLabels != null) {
							StringBuffer label = null;
							if(values.label == null) {
								label = new StringBuffer("");
							} else {
								label = new StringBuffer(values.label);
							}
							label.append(" (");
							for(OptionDesc o : item.optionLabels) {
								label.append(" ").append(o.value).append("=").append(o.label);
								
							}
							label.append(")");
							Cell cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(label.toString());
						} else {
							Cell cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(values.label);
						}
					}
				}
				
				/*
				 * Write Question Name Header
				 */
				Row headerRow = dataSheet.createRow(rowNumber++);				
				int colNumber = 0;
				int dataColumn = 0;
				while(dataColumn < sqlDesc.column_details.size()) {
					ColValues values = new ColValues();
					ColDesc item = sqlDesc.column_details.get(dataColumn);
					dataColumn = GeneralUtilityMethods.getColValues(
							null, 
							values, 
							dataColumn,
							sqlDesc.column_details, 
							merge_select_multiple,
							surveyName);	
						
					// Add literacy specific columns if required
					if(item.qType != null && item.qType.equals("select") && item.literacy) {
						Cell cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue(values.name + " - Time");
						
						cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue(values.name + " - Flash Index");
						
						cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue(values.name + " - Final Index");
						
						
						cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue(values.name + " - Error Count");
					}
					
					else if(split_locn && item.qType != null && item.rawQuestionType.equals("geopoint")) {
						Cell cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue("Latitude");
						
						cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue("Longitude");
					} else if(item.qType != null && item.qType.equals("select") && !merge_select_multiple && item.choices != null  && item.compressed) {
						
						for(int i = 0; i < item.choices.size(); i++) {
							Cell cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							if(item.selectDisplayNames) {
								cell.setCellValue(item.choices.get(i).v);	// Just show the choice display name
							} else {
								cell.setCellValue(values.name + " - " + item.choices.get(i).v);
							}
						}
					} else if(item.qType != null && item.qType.equals("rank") && !merge_select_multiple && item.choices != null) {
						for(int i = 0; i < item.choices.size(); i++) {
							Cell cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(values.name + " - " + (i + 1));
						}
					} else {
						Cell cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue(values.name);
					}
				}
				
				pstmt = cResults.prepareStatement(sqlDesc.sql);
				
				// Add parameters
				int paramCount = 1;
				// Add parameters in table column selections
				if (sqlDesc.columnSqlFrags.size() > 0) {
					paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, sqlDesc.columnSqlFrags, paramCount, tz);
				}
				
				cResults.setAutoCommit(false);	// page the results to reduce memory usage	
				pstmt.setFetchSize(100);	
				
				log.info("Get results: " + pstmt.toString());
				ResultSet rs = pstmt.executeQuery();
				ArrayList<ReadData> dataItems = null;
				Row dataRow = null;
				int rowCount = 1;
				while(rs.next()) {
					
					if(rowCount++ % 1000 == 0) {
						log.info("#report row: " + rowCount);
					}
					// Re-get the survey name for the survey that wrote this record, this may vary in groups
					if(meta) {
						String recordSId = rs.getString("_s_id");
						surveyName = surveyNames.get(recordSId);
						if(surveyName == null) {
							if(recordSId != null && recordSId.trim().length() > 0) {
								surveyName = GeneralUtilityMethods.getSurveyName(sd, Integer.parseInt(recordSId));
								surveyNames.put(recordSId, surveyName);
							} else {
								surveyName = "";
							}
							
						}
					}
					
					/*
					 * Write out the previous record if this report does not use transforms or the key has changed
					 */
					if(dataItems != null ) {
						dataRow = dataSheet.createRow(rowNumber++);	
						writeOutData(dataItems, dataRow, wb, dataSheet, styles, embedImages, 
								basePath, rowNumber);
					}
					
					dataItems = new ArrayList<> ();
					
					dataColumn = 0;
					while(dataColumn < sqlDesc.column_details.size()) {
						ColValues values = new ColValues();
						ColDesc item = sqlDesc.column_details.get(dataColumn);
						dataColumn = GeneralUtilityMethods.getColValues(
								rs, 
								values, 
								dataColumn,
								sqlDesc.column_details, 
								merge_select_multiple,
								surveyName);						
						
						
						if(split_locn && values.value != null && values.value.startsWith("POINT")) {

							String coords [] = GeneralUtilityMethods.getLonLat(values.value);

							ReadData rd = new ReadData(values.name, false, values.type);
							dataItems.add(rd);
							
							if(coords.length > 1) {
								rd.values.add(coords[1]);
								rd.values.add(coords[0]);
							} else {
								rd.values.add(values.value);
								rd.values.add(values.value);
							}

						} else if(split_locn && values.value != null && (values.value.startsWith("POLYGON") || values.value.startsWith("LINESTRING"))) {

							// Can't split linestrings and polygons
							ReadData rd = new ReadData(values.name, false, "string");
							dataItems.add(rd);
							rd.values.add(values.value);


						} else if(split_locn && item.rawQuestionType.equals("geopoint") ) {
							// Geopoint that needs to be split but there is no data
							ReadData rd = new ReadData(values.name, false, "string");
							dataItems.add(rd);
							rd.values.add("");
							rd.values.add("");

						} else if(item.qType != null && item.qType.equals("select") && !merge_select_multiple && item.choices != null  && item.compressed) {
							
							String [] vArray = null;
							if(values.value != null) {
								vArray = values.value.split(" ");
							} 
										
							ReadData rd = new ReadData(values.name, false, "int");		// write out as integer
							dataItems.add(rd);
							
							if(item.literacy) {
								addLiteracyColumns(vArray, rd);
							}
							
							for(int i = 0; i < item.choices.size(); i++) {			
								
								String v = "0";
								if(vArray != null) {
									
									String choiceValue = item.choices.get(i).k;
									for(int k = 0; k < vArray.length; k++) {
										if(vArray[k].equals(choiceValue)) {
											v = "1";
											break;
										}
									}
								}
								rd.values.add(v);
									
							}
						} else if(item.qType != null && item.qType.equals("select") && merge_select_multiple && item.choices != null  && item.compressed) {
							
							// Merge the select values into a single column
							StringBuilder value = new StringBuilder("");
							String [] vArray = null;
							if(values.value != null) {
								vArray = values.value.split(" ");
							} 
					
							if(vArray != null) {
							
								for(int i = 0; i < item.choices.size(); i++) {		
									
									String choiceValue = item.choices.get(i).k;
									for(int k = 0; k < vArray.length; k++) {
										if(vArray[k].equals(choiceValue)) {
											if(value.length() > 0) {
												value.append(" ");
											}
											value.append(item.choices.get(i).v);
											break;
										}
									}
								}
									
							}
							
							if(item.literacy) {
								ReadData rd = new ReadData(values.name, false, "int");
								dataItems.add(rd);
								addLiteracyColumns(vArray, rd);
							}
							
							ReadData rd = new ReadData(values.name, false, values.type);
							dataItems.add(rd);
							rd.values.add(value.toString());
							rd.type = values.type;
							
						} else if(item.qType != null && item.qType.equals("rank") && !merge_select_multiple && item.choices != null) {
							
							String [] vArray = {""};
							if(values.value != null) {
								vArray = values.value.split(" ");
							} 
							
							ReadData rd = new ReadData(values.name, false, values.type);
							dataItems.add(rd);
							
							for(int i = 0; i < item.choices.size(); i++) {							
								if(i < vArray.length) {
									rd.values.add(vArray[i]);	
								} else {
									rd.values.add("");  // Just write spaces
								}		
									
							}
						} else if(item.qType != null && item.qType.equals("select1") && item.selectDisplayNames) {
							
							String value = values.value;
							// Convert the value into the display name
							for(int i = 0; i < item.choices.size(); i++) {							
								String choiceValue = item.choices.get(i).k;
								if(choiceValue != null && choiceValue.equals(value)) {
									value = item.choices.get(i).v;
									break;
								}	
							}
							
							ReadData rd = new ReadData(values.name, false, values.type);
							dataItems.add(rd);
							rd.values.add(value);
							rd.type = values.type;
							
						} else {
							ReadData rd = new ReadData(values.name, false, values.type);
							dataItems.add(rd);
							rd.values.add(values.value);
							rd.type = values.type;
						}
					}

				}
				cResults.setAutoCommit(true);
				
				// Write the last row
				dataRow = dataSheet.createRow(rowNumber++);	
				writeOutData(dataItems, dataRow, wb, dataSheet, styles, embedImages, 
						basePath, rowNumber);
			

			}  catch (Exception e) {
				try {cResults.setAutoCommit(true);} catch (Exception ex) {}
				log.log(Level.SEVERE, "Error", e);
				//response.setHeader("Content-type",  "text/html; charset=UTF-8");
				lm.writeLog(sd, sId, username, LogManager.ERROR, e.getMessage(), 0, serverName);
				
				String msg = e.getMessage();
				if(msg != null && msg.contains("does not exist")) {
					msg = localisation.getString("msg_no_data");
				}
				Row dataRow = dataSheet.createRow(rowNumber + 1);	
				Cell cell = dataRow.createCell(0);
				cell.setCellStyle(errorStyle);
				cell.setCellValue(msg);
				
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} finally {	

				try {
					wb.write(outputStream);
					wb.close();
					outputStream.close();
					wb.dispose();		// Dispose of temporary files
				} catch (Exception ex) {
					log.log(Level.SEVERE, "Error", ex);
				}
				
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}


			}
		}

		return responseVal;
	}
	
	private void addLiteracyColumns(String [] vArray, ReadData rd) {
		
		String time = "";
		String flashIndex = "";
		String finalIndex = "";
		int errorCount = 0;
		
		if(vArray != null) {
			if(vArray.length > 0) {
				flashIndex = vArray[0];
				vArray[0] = "";  // Make sure this value does not later match a numeric choice
			}
			if(vArray.length > 1) {
				time = vArray[1];
				vArray[1] = "";
			}
			if(vArray.length > 2) {
				finalIndex = vArray[2];
				vArray[2] = "";
			}
			
			// Get error count
			if(vArray.length > 3) {
				for(int i = 3; i < vArray.length; i++) {
					if(vArray[i] != null && !vArray[i].equals("null")) {
						errorCount++;
					}
				}
			}
			rd.values.add(time);
			rd.values.add(flashIndex);
			rd.values.add(finalIndex);
			rd.values.add(String.valueOf(errorCount));
		}
	}
	
	private void writeOutData(ArrayList<ReadData> dataItems,
			Row dataRow,
			SXSSFWorkbook wb,
			Sheet dataSheet,
			Map<String, CellStyle> styles,
			boolean embedImages,
			String basePath,
			int rowNumber) {
		
		int colNumber = 0;
		if(dataItems != null) {
			for(ReadData item : dataItems) {
				for(String v : item.values) {
					Cell cell = dataRow.createCell(colNumber++);
					XLSUtilities.setCellValue(wb, dataSheet, cell, styles, v, 
							item.type, embedImages, basePath, rowNumber, colNumber - 1, true);
				}
			}
		}
	}

}
