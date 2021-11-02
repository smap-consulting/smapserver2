package utilities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

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
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;
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
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.AR;
import org.smap.sdal.model.UserTrailFeature;
import org.smap.sdal.model.UserTrailPoint;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXAdminReportsManager {
	
	private static Logger log =
			 Logger.getLogger(SurveyInfo.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	
	public XLSXAdminReportsManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Write new background report
	 */
	public String writeNewReport(Connection sd, int pId, HashMap<String, String> params, String basePath) throws SQLException, IOException {
		
		String filename = String.valueOf(UUID.randomUUID()) + ".kml";
	
		GeneralUtilityMethods.createDirectory(basePath + "/reports");
		String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
		File tempFile = new File(filepath);
		
		//getNewReport(sd, tempFile, header, report, byProject, bySurvey, byDevice, year, month, orgName);

		

		
		return filename;
	
	}
	
	/*
	 * Create the new style XLSX report
	 */
	public void getNewReport(
			Connection sd,
			File tempFile,
			ArrayList<String> header,
			ArrayList <AR> report,
			boolean byProject,
			boolean bySurvey,
			boolean byDevice,
			int year,
			int month,
			String orgName) throws ApplicationException, FileNotFoundException {

		FileOutputStream outputStream = new FileOutputStream(tempFile);
		
		if(header != null) {

			Workbook wb = null;
			int rowNumber = 0;
			Sheet dataSheet = null;
			CellStyle errorStyle = null;

			try {	
				
				/*
				 * Create XLSX File
				 */
				wb = new SXSSFWorkbook(10);		// Serialised output
				dataSheet = wb.createSheet("data");
				rowNumber = 0;
				
				Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
				CellStyle headerStyle = styles.get("header");
				errorStyle = styles.get("error");
				
				/*
				 * Write the headers
				 */	
				Row yearRow = dataSheet.createRow(rowNumber++);		
				Cell cell = yearRow.createCell(0);	// Year
				cell.setCellValue(localisation.getString("bill_year"));
				cell = yearRow.createCell(1);	
				cell.setCellValue(year);
				
				Row monthRow = dataSheet.createRow(rowNumber++);		
				cell = monthRow.createCell(0);	// Month
				cell.setCellValue(localisation.getString("bill_month"));
				cell = monthRow.createCell(1);	
				cell.setCellValue(month);
				
				Row orgRow = dataSheet.createRow(rowNumber++);		
				cell = orgRow.createCell(0);
				cell.setCellValue(localisation.getString("bill_org"));
				cell = orgRow.createCell(1);	
				cell.setCellValue(orgName);
				
				rowNumber++;		// blank row
				Row headerRow = dataSheet.createRow(rowNumber++);				
				int colNumber = 0;
				while(colNumber < header.size()) {
					cell = headerRow.createCell(colNumber);
					cell.setCellStyle(headerStyle);
					cell.setCellValue(header.get(colNumber));
					colNumber++;
				}
				
				int monthlyCol = 0;
				int allTimeCol = 0;
				int firstDataRow = rowNumber + 1;
				for(AR ar : report) {
					if(ar.usageInPeriod > 0 || ar.allTimeUsage > 0) {
						colNumber = 0;
						Row row = dataSheet.createRow(rowNumber++);	
						cell = row.createCell(colNumber++);	// ident
						cell.setCellValue(ar.userIdent);
						
						cell = row.createCell(colNumber++);	// Name
						cell.setCellValue(ar.userName);
						
						cell = row.createCell(colNumber++);	// User created
						if(ar.created != null) {
							cell.setCellStyle(styles.get("date"));
							cell.setCellValue(ar.created);
						}
						
						if(byProject || bySurvey) {
							cell = row.createCell(colNumber++);	// Project
							cell.setCellValue(ar.p_id);
							
							cell = row.createCell(colNumber++);
							cell.setCellValue(ar.project);
						}
						
						if(bySurvey) {
							cell = row.createCell(colNumber++);	// Survey
							cell.setCellValue(ar.s_id);
							
							cell = row.createCell(colNumber++);	
							cell.setCellValue(ar.survey);
						}
						
						if(byDevice) {
							cell = row.createCell(colNumber++);	// Device
							cell.setCellValue(ar.device);
							
						}
						
						monthlyCol = colNumber;
						cell = row.createCell(colNumber++);	// Monthly Usage
						cell.setCellValue(ar.usageInPeriod);
						
						allTimeCol = colNumber;
						cell = row.createCell(colNumber++);	// All time Usage
						cell.setCellValue(ar.allTimeUsage);
					}
				}
				
				// Add totals
				Row row = dataSheet.createRow(rowNumber++);	
				
				// Monthly
				cell = row.createCell(monthlyCol);
				String colAlpha = getColAlpha(monthlyCol);
				String formula= "SUM(" + colAlpha + firstDataRow + ":" + colAlpha + (rowNumber - 1) + ")";
				cell.setCellStyle(styles.get("bold"));
				cell.setCellFormula(formula);
				
				// All time
				cell = row.createCell(allTimeCol);
				colAlpha = getColAlpha(allTimeCol);
				formula = "SUM(" + colAlpha + firstDataRow + ":" + colAlpha + (rowNumber - 1) + ")";
				cell.setCellStyle(styles.get("bold"));
				cell.setCellFormula(formula);

			} catch (Exception e) {
				log.log(Level.SEVERE, "Error", e);
				
				String msg = e.getMessage();
				if(msg.contains("does not exist")) {
					msg = localisation.getString("msg_no_data");
				}
				Row dataRow = dataSheet.createRow(rowNumber + 1);	
				Cell cell = dataRow.createCell(0);
				cell.setCellStyle(errorStyle);
				cell.setCellValue(msg);
				
				throw new ApplicationException("Error: " + e.getMessage());
			} finally {	

				try {
					wb.write(outputStream);
					wb.close();

					((SXSSFWorkbook) wb).dispose();		// Dispose of temporary files
				} catch (Exception ex) {
					log.log(Level.SEVERE, "Error", ex);
				}


			}
		}

		return;
	}
	
	private String getColAlpha(int col) {
		return "ABCDEFGHIJKLMNOPQRSTUVQXYZ".substring(col, col + 1);
	}

}
