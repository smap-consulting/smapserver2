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
import java.util.ArrayList;
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
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.AR;
import org.smap.sdal.model.HourlyLogSummaryItem;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXLogHourlyReportsManager {
	
	private static Logger log =
			 Logger.getLogger(XLSXLogHourlyReportsManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	
	public XLSXLogHourlyReportsManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Create the new style XLSX report
	 */
	public Response getNewReport(
			Connection sd,
			HttpServletRequest request,
			HttpServletResponse response,
			ArrayList<String> events,
			ArrayList<HourlyLogSummaryItem> logs,
			String filename,
			int year,
			int month,
			int day,
			String orgName) {
		
		Response responseVal = null;

		String escapedFileName = null;
		try {
			escapedFileName = URLDecoder.decode(filename, "UTF-8");
			escapedFileName = URLEncoder.encode(escapedFileName, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}

		escapedFileName = escapedFileName.replace("+", " "); // Spaces ok for file name within quotes
		escapedFileName = escapedFileName.replace("%2C", ","); // Commas ok for file name within quotes
		

		Workbook wb = null;
		int rowNumber = 0;
		Sheet dataSheet = null;
		CellStyle errorStyle = null;

		try {	

			/*
			 * Create XLSX File
			 */
			GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
			wb = new SXSSFWorkbook(10);		// Serialised output
			dataSheet = wb.createSheet("data");
			rowNumber = 0;

			Row row = dataSheet.createRow(rowNumber++);		// Organisation
			Cell cell = row.createCell(0);	
			cell.setCellValue(localisation.getString("bill_org"));
			cell = row.createCell(1);
			cell.setCellValue(orgName);
			
			row = dataSheet.createRow(rowNumber++);		// Year
			cell = row.createCell(0);	
			cell.setCellValue(localisation.getString("bill_year"));
			cell = row.createCell(1);
			cell.setCellValue(year);
			
			row = dataSheet.createRow(rowNumber++);		// Month
			cell = row.createCell(0);	
			cell.setCellValue(localisation.getString("bill_month"));
			cell = row.createCell(1);
			cell.setCellValue(month);
			
			row = dataSheet.createRow(rowNumber++);		// Day
			cell = row.createCell(0);	
			cell.setCellValue(localisation.getString("c_day"));
			cell = row.createCell(1);
			cell.setCellValue(day);
			
			Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
			CellStyle headerStyle = styles.get("header");
			errorStyle = styles.get("error");

			rowNumber++;
			row = dataSheet.createRow(rowNumber++);				
			int colNumber = 0;

			cell = row.createCell(colNumber++);	// Hour
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("c_hour"));

			for(String event : events) {
				cell = row.createCell(colNumber++);
				cell.setCellStyle(headerStyle);
				cell.setCellValue(event);
			}

			for(HourlyLogSummaryItem item : logs) {
				row = dataSheet.createRow(rowNumber++);	
				
				colNumber = 0;
				cell = row.createCell(colNumber++);	// Hour
				cell.setCellValue(item.hour);

				for(String event : events) {
					Integer count = item.events.get(event);
					if(count == null) {
						count = 0;
					}
					cell = row.createCell(colNumber++);	// Hour
					cell.setCellValue(count);
				}
			}




		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");

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


		}


		return responseVal;
	}


}
