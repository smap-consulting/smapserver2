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

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.QueryGenerator;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QueryManager;
import org.smap.sdal.model.ColDesc;
import org.smap.sdal.model.ColValues;
import org.smap.sdal.model.OptionDesc;
import org.smap.sdal.model.QueryForm;
import org.smap.sdal.model.SqlDesc;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXReportsManager {
	
	private static Logger log =
			 Logger.getLogger(SurveyInfo.class.getName());
	
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
			HttpServletRequest request,
			HttpServletResponse response,
			int sId, 
			String filename, 
			boolean split_locn, 
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
			boolean meta) {
		
		Response responseVal = null;

		HashMap<ArrayList<OptionDesc>, String> labelListMap = new  HashMap<ArrayList<OptionDesc>, String> ();

		log.info("userevent: " + username + " Export " + sId + " as an xlsx file to " + filename + " starting from form " + fId);

		String urlprefix = request.getScheme() + "://" + request.getServerName() + "/";		

		lm.writeLog(sd, sId, username, "view", "Export as: xlsx");

		String escapedFileName = null;
		try {
			escapedFileName = URLDecoder.decode(filename, "UTF-8");
			escapedFileName = URLEncoder.encode(escapedFileName, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}

		escapedFileName = escapedFileName.replace("+", " "); // Spaces ok for file name within quotes
		escapedFileName = escapedFileName.replace("%2C", ","); // Commas ok for file name within quotes
		
		if(sId != 0) {

			PreparedStatement pstmt = null;

			try {	

				if(language == null) {	// ensure a language is set
					language = "none";
				}

				/*
				 * Get the list of forms and surveys to be exported
				 */
				ArrayList<QueryForm> queryList = null;
				QueryManager qm = new QueryManager();				
				queryList = qm.getFormList(sd, sId, fId);		// Get a form list for this survey / form combo

				QueryForm startingForm = qm.getQueryTree(sd, queryList);	// Convert the query list into a tree

				// Get the SQL for this query
				SqlDesc sqlDesc = QueryGenerator.gen(sd, 
						cResults,
						localisation,
						sId,
						fId,
						language, 
						"xlsx", 
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
						meta);

				String basePath = GeneralUtilityMethods.getBasePath(request);					
				
				/*
				 * Create XLSX File
				 */
				GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
				Workbook wb = null;
				int rowNumber = 0;
				wb = new SXSSFWorkbook(10);		// Serialised output
				Sheet dataSheet = wb.createSheet("data");
				
				Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
				CellStyle headerStyle = styles.get("header");
				
				/*
				 * Write the labels if language has been set
				 */
				if(language != null && !language.equals("none")) {
					Row headerRow = dataSheet.createRow(rowNumber++);				
					int colNumber = 0;
					int dataColumn = 0;
					while(dataColumn < sqlDesc.colNames.size()) {
						ColValues values = new ColValues();
						ColDesc item = sqlDesc.colNames.get(dataColumn);
						
						dataColumn = GeneralUtilityMethods.getColValues(
								null, 
								values, 
								dataColumn,
								sqlDesc.colNames, 
								merge_select_multiple);	
						
						if(split_locn && values.name.equals("the_geom")) {
							Cell cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(values.label);
							
							cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(values.label);
						} else if(item.qType != null && item.qType.equals("select") && !merge_select_multiple && item.choices != null) {
							for(int i = 0; i < item.choices.size(); i++) {
								Cell cell = headerRow.createCell(colNumber++);
								cell.setCellStyle(headerStyle);
								cell.setCellValue(values.label + " - " + item.choices.get(i).k);
							}
						} else if(item.qType != null && item.qType.equals("select1") && item.optionLabels != null) {
							StringBuffer label = new StringBuffer(values.label);
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
				while(dataColumn < sqlDesc.colNames.size()) {
					ColValues values = new ColValues();
					ColDesc item = sqlDesc.colNames.get(dataColumn);
					dataColumn = GeneralUtilityMethods.getColValues(
							null, 
							values, 
							dataColumn,
							sqlDesc.colNames, 
							merge_select_multiple);	
						
					if(split_locn && values.name.equals("the_geom")) {
						Cell cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue("Latitude");
						
						cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						cell.setCellValue("Longitude");
					} else if(item.qType != null && item.qType.equals("select") && !merge_select_multiple && item.choices != null) {
						for(int i = 0; i < item.choices.size(); i++) {
							Cell cell = headerRow.createCell(colNumber++);
							cell.setCellStyle(headerStyle);
							cell.setCellValue(values.name + " - " + item.choices.get(i).k);
						}
					} else {
						Cell cell = headerRow.createCell(colNumber++);
						cell.setCellStyle(headerStyle);
						if(item.humanName != null && item.humanName.trim().length() > 0) {
							cell.setCellValue(item.humanName);
						} else {
							cell.setCellValue(values.name);
						}
					}
				}
				
				/*
				 * Write each row of data
				 */
				pstmt = cResults.prepareStatement(sqlDesc.sql);
				log.info("Get results: " + pstmt.toString());
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					
					Row dataRow = dataSheet.createRow(rowNumber++);	
					
					colNumber = 0;
					dataColumn = 0;
					while(dataColumn < sqlDesc.colNames.size()) {
						ColValues values = new ColValues();
						ColDesc item = sqlDesc.colNames.get(dataColumn);
						dataColumn = GeneralUtilityMethods.getColValues(
								rs, 
								values, 
								dataColumn,
								sqlDesc.colNames, 
								merge_select_multiple);						

						if(split_locn && values.value != null && values.value.startsWith("POINT")) {

							String coords [] = GeneralUtilityMethods.getLonLat(values.value);

							if(coords.length > 1) {
								Cell cell = dataRow.createCell(colNumber++);
								XLSUtilities.setCellValue(wb, dataSheet, cell, styles, coords[1], 
										values.type, embedImages, basePath, rowNumber, colNumber - 1, true);
								cell = dataRow.createCell(colNumber++);
								XLSUtilities.setCellValue(wb, dataSheet, cell, styles, coords[0], 
										values.type, embedImages, basePath, rowNumber, colNumber - 1, true);
								//out.add(new CellItem(coords[1], CellItem.DECIMAL));
								//out.add(new CellItem(coords[0], CellItem.DECIMAL)); 
							} else {
								Cell cell = dataRow.createCell(colNumber++);
								XLSUtilities.setCellValue(wb, dataSheet, cell, styles, values.value, 
										"decimal", embedImages, basePath, rowNumber, colNumber - 1, true);
								cell = dataRow.createCell(colNumber++);
								XLSUtilities.setCellValue(wb, dataSheet, cell, styles, values.value, 
										"decimal", embedImages, basePath, rowNumber, colNumber - 1, true);
								//out.add(new CellItem(value, CellItem.STRING));
								//out.add(new CellItem(value, CellItem.STRING));
							}


						} else if(split_locn && values.value != null && (values.value.startsWith("POLYGON") || values.value.startsWith("LINESTRING"))) {

							// Can't split linestrings and polygons, leave latitude and longitude as blank
							Cell cell = dataRow.createCell(colNumber++);
							XLSUtilities.setCellValue(wb, dataSheet, cell, styles, values.value, 
									"string", embedImages, basePath, rowNumber, colNumber - 1, true);
							cell = dataRow.createCell(colNumber++);
							XLSUtilities.setCellValue(wb, dataSheet, cell, styles, values.value, 
									"string", embedImages, basePath, rowNumber, colNumber - 1, true);
							//out.add(new CellItem("", CellItem.STRING));
							//out.add(new CellItem("", CellItem.STRING));


						} else if(split_locn && values.type != null && values.type.equals("geopoint") ) {
							// Geopoint that needs to be split but there is no data
							Cell cell = dataRow.createCell(colNumber++);
							XLSUtilities.setCellValue(wb, dataSheet, cell, styles, "", 
									"string", embedImages, basePath, rowNumber, colNumber - 1, true);
							cell = dataRow.createCell(colNumber++);
							XLSUtilities.setCellValue(wb, dataSheet, cell, styles, "", 
									"string", embedImages, basePath, rowNumber, colNumber - 1, true);
							//out.add(new CellItem("", CellItem.STRING));
							//out.add(new CellItem("", CellItem.STRING));
						} else if(item.qType != null && item.qType.equals("select") && !merge_select_multiple && item.choices != null) {
							
							String [] vArray = null;
							if(values.value != null) {
								vArray = values.value.split(" ");
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
								Cell cell = dataRow.createCell(colNumber++);
								cell.setCellStyle(headerStyle);
								XLSUtilities.setCellValue(wb, dataSheet, cell, styles, v, 
										values.type, embedImages, basePath, rowNumber, colNumber - 1, true);
									
							}
						} else {
							Cell cell = dataRow.createCell(colNumber++);
							XLSUtilities.setCellValue(wb, dataSheet, cell, styles, values.value, 
									values.type, embedImages, basePath, rowNumber, colNumber - 1, true);
						}
					}
					
				}
				
				OutputStream outputStream = response.getOutputStream();
				wb.write(outputStream);
				wb.close();
				outputStream.close();
				((SXSSFWorkbook) wb).dispose();		// Dispose of temporary files


			} catch (ApplicationException e) {
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				// Return an OK status so the message gets added to the web page
				// Prepend the message with "Error: ", this will be removed by the client
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Error", e);
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				lm.writeLog(sd, sId, username, "error", e.getMessage());
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} finally {	

				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}


			}
		}

		return responseVal;
	}

}
