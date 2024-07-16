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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.MediaItem;
import org.smap.sdal.model.Queue;
import org.smap.sdal.model.QueueTime;
import org.smap.sdal.model.Survey;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXAdminReportsQueues {
	
	private static Logger log =
			 Logger.getLogger(XLSXAdminReportsQueues.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	
	CellStyle good = null;
	CellStyle bad = null;
	
	boolean forDevice = false;
	
	public XLSXAdminReportsQueues(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Create the new style XLSX report
	 */
	public Response getNewReport(
			Connection sd,
			HttpServletRequest request,
			HttpServletResponse response,
			ArrayList<QueueTime> data,
			String filename,
			int oId) {
		
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
		Sheet dataSheet = null;
		CellStyle errorStyle = null;
		int rowNumber = 0;
		int colNumber;

		MediaInfo mediaInfo = new MediaInfo();
		mediaInfo.setServer(request.getRequestURL().toString());
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		PreparedStatement pstmt = null;
		
		try {
				
			/*
			 * Create XLSX File
			 */
			GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
			wb = new SXSSFWorkbook(10);		// Serialised output
			dataSheet = wb.createSheet("data");
			
			Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
			CellStyle headerStyle = styles.get("header");
			errorStyle = styles.get("errorStyle");
			good = styles.get("good");
			bad = styles.get("bad");
			
			if(data.size() > 0) {
				HashMap<String, Queue> h = data.get(0).data;
				Set<String> keys = h.keySet();
				
				/*
				 * Add the headings 
				 */
				colNumber = 0;
				Row row = dataSheet.createRow(rowNumber++);		
				Cell cell = null;
				
				cell = row.createCell(colNumber++);	
				cell.setCellStyle(headerStyle);
				cell.setCellValue("Time");
				
				for(String key : keys) {
					cell = row.createCell(colNumber++);	
					cell.setCellStyle(headerStyle);
					cell.setCellValue(key + " length");
					
					cell = row.createCell(colNumber++);	
					cell.setCellStyle(headerStyle);
					cell.setCellValue(key + " new");
					
					cell = row.createCell(colNumber++);
					cell.setCellStyle(headerStyle);
					cell.setCellValue(key + " procesed");
				}
				
				/*
				 * Add the data
				 */
				for(QueueTime elem : data) {
					row = dataSheet.createRow(rowNumber++);	
					colNumber = 0;
					
					cell = row.createCell(colNumber++);	
					cell.setCellValue(elem.recorded_at);
					
					for(String key : keys) {
						Queue q = elem.data.get(key);
						
						cell = row.createCell(colNumber++);
						cell.setCellValue(q.length);
						
						cell = row.createCell(colNumber++);
						cell.setCellValue(q.new_rpm);
						
						cell = row.createCell(colNumber++);	// Type
						cell.setCellValue(q.processed_rpm);
					}
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

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
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
