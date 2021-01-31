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
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.QueryGenerator;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.constants.SmapExportTypes;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QueryManager;
import org.smap.sdal.model.ColDesc;
import org.smap.sdal.model.ColValues;
import org.smap.sdal.model.OptionDesc;
import org.smap.sdal.model.QueryForm;
import org.smap.sdal.model.ReadData;
import org.smap.sdal.model.SqlDesc;
import org.smap.sdal.model.Transform;
import org.smap.sdal.model.TransformDetail;


/*
 * Manage exporting of custom TDH reports
 */

public class TDHReportsManager {
	
	private static Logger log =
			 Logger.getLogger(SurveyInfo.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	String tz;
	
	public TDHReportsManager(ResourceBundle l, String timezone) {
		localisation = l;
		tz = timezone;
	}

	/*
	 * Create the new style XLSX report
	 */
	public Response getIndividualReport(
			Connection sd,
			Connection cResults,
			String filename,
			String username,
			HttpServletRequest request,
			HttpServletResponse response,
			String beneficiaryCode) {
		
		Response responseVal = null;

		HashMap<ArrayList<OptionDesc>, String> labelListMap = new  HashMap<ArrayList<OptionDesc>, String> ();
		
		
		String urlprefix = request.getScheme() + "://" + request.getServerName() + "/";		

		

		String escapedFileName = null;
		try {
			escapedFileName = URLDecoder.decode(filename, "UTF-8");
			escapedFileName = URLEncoder.encode(escapedFileName, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}

		escapedFileName = escapedFileName.replace("+", " "); // Spaces ok for file name within quotes
		escapedFileName = escapedFileName.replace("%2C", ","); // Commas ok for file name within quotes
		
		if(beneficiaryCode != null) {

			PreparedStatement pstmt = null;
			Workbook wb = null;
			int rowNumber = 0;
			Sheet dataSheet = null;
			Sheet settingsSheet = null;
			CellStyle errorStyle = null;

			int oId = 0;
			
			try {	
			
				oId = GeneralUtilityMethods.getOrganisationId(sd, username);
				lm.writeLogOrganisation(sd, oId, username, LogManager.REPORT, " Export custom TDH individual Report ", 0);
				
				/*
				 * Create XLSX File
				 */
				log.info("####################### Create XLSX file");
				GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
				wb = new SXSSFWorkbook(10);		// Serialised output
				Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
				CellStyle headerStyle = styles.get("header");
				CellStyle wideStyle = styles.get("wide");
				errorStyle = styles.get("error");
						
				dataSheet = wb.createSheet(localisation.getString("rep_data"));
				settingsSheet = wb.createSheet(localisation.getString("rep_settings"));
				
				
				// Populate data sheet
				rowNumber = 0;		
				
				/*
				 * Write the labels if language has been set
				 */
				Row headerRow = dataSheet.createRow(rowNumber++);				
				int colNumber = 0;
				int dataColumn = 0;

				Cell cell = headerRow.createCell(colNumber++);
				cell.setCellStyle(headerStyle);
				cell.setCellValue("Question");
				
				cell = headerRow.createCell(colNumber++);
				cell.setCellStyle(headerStyle);
				cell.setCellValue("Pre");
				
				cell = headerRow.createCell(colNumber++);
				cell.setCellStyle(headerStyle);
				cell.setCellValue("Post");

			

			}  catch (Exception e) {
				try {cResults.setAutoCommit(true);} catch (Exception ex) {}
				log.log(Level.SEVERE, "Error", e);
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				lm.writeLogOrganisation(sd, oId, username, LogManager.REPORT, e.getMessage(), 0);
				
				String msg = e.getMessage();
				if(msg.contains("does not exist")) {
					msg = localisation.getString("msg_no_data");
				}
				Row dataRow = dataSheet.createRow(rowNumber + 1);	
				Cell cell = dataRow.createCell(0);
				cell.setCellStyle(errorStyle);
				cell.setCellValue(msg);
				
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} finally {	

				try {
					OutputStream outputStream = response.getOutputStream();
					wb.write(outputStream);
					wb.close();
					outputStream.close();
					((SXSSFWorkbook) wb).dispose();		// Dispose of temporary files
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
	
	private void writeOutData(ArrayList<ReadData> dataItems, Transform transform, 
			HashMap<String, HashMap<String, String>> transformData,
			Row dataRow,
			Workbook wb,
			Sheet dataSheet,
			Map<String, CellStyle> styles,
			boolean embedImages,
			String basePath,
			int rowNumber) {
		
		int colNumber = 0;
		if(dataItems != null) {
			for(ReadData item : dataItems) {
				if(item.isTransform) {
					int tdIndex = getTransformIndex(transform, item.name);	
					HashMap<String, String> itemTransform = transformData.get(item.name);
					for(String tc : transform.transforms.get(tdIndex).wideColumns) {
						for(String tv : transform.transforms.get(tdIndex).values) {
							Cell cell = dataRow.createCell(colNumber++);
							XLSUtilities.setCellValue(wb, dataSheet, cell, styles, itemTransform.get(tc + " - " + tv), 
									item.type, embedImages, basePath, rowNumber, colNumber - 1, true);
						}
					}
				} else {
					for(String v : item.values) {
						Cell cell = dataRow.createCell(colNumber++);
						XLSUtilities.setCellValue(wb, dataSheet, cell, styles, v, 
								item.type, embedImages, basePath, rowNumber, colNumber - 1, true);
					}
				}
			}
		}
	}
	
	private boolean isWideColumn(Transform transform, String name) {
		boolean val = false;
		
		if(transform != null && transform.enabled) {
			for(TransformDetail td : transform.transforms) {
				for(String col : td.wideColumns) {
					if(col.equals(name)) {
						val = true;
						break;
					}
				}
				if(val) {
					break;
				}

			}
		}
		return val;
	}
	
	private int getTransformIndex(Transform transform, String name) {
		int idx = -1;
		
		if(transform != null) {
			int count = 0;
			for(TransformDetail td : transform.transforms) {
				if(td.valuesQuestion.equals(name)) {
					idx = count;
					break;
				}
				count++;
			}
		}
		return idx;
	}
	
	private String getKeyValue(ResultSet rs, Transform transform) throws SQLException {
		StringBuilder key = new StringBuilder("");
		for(String c : transform.key_questions) {
			key.append(rs.getString(c));
		}
		return key.toString();
	}

}
