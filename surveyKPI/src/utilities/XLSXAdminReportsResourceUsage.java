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
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.MediaItem;
import org.smap.sdal.model.Survey;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXAdminReportsResourceUsage {
	
	private static Logger log =
			 Logger.getLogger(XLSXAdminReportsResourceUsage.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	
	CellStyle good = null;
	CellStyle bad = null;
	
	public XLSXAdminReportsResourceUsage(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Create the new style XLSX report
	 */
	public Response getNewReport(
			Connection sd,
			HttpServletRequest request,
			HttpServletResponse response,
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
								
			/*
			 * Add the headings 
			 */
			colNumber = 0;
			Row row = dataSheet.createRow(rowNumber++);		
			Cell cell = null;
	
			cell = row.createCell(colNumber++);	// Resource
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("ar_res"));
			
			cell = row.createCell(colNumber++);	// Type
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("ar_type"));
			
			cell = row.createCell(colNumber++);	// Project
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("ar_project"));
			
			cell = row.createCell(colNumber++);	// Survey
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("ar_survey"));
			
			cell = row.createCell(colNumber++);	// Blocked
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("ar_blocked"));		
			
			/*
			 * Add the data
			 */
			// Get the path to the media folder		
			mediaInfo.setFolder(basePath, request.getRemoteUser(), oId, false);				 
			log.info("Media query on: " + mediaInfo.getPath());
			ArrayList<MediaItem> media = mediaInfo.get(0, null);	
			
			for(MediaItem mi : media) {
				colNumber = 0;
				
				row = dataSheet.createRow(rowNumber++);	
				
				cell = row.createCell(colNumber++);	// Resource
				cell.setCellValue(mi.name);
				
				cell = row.createCell(colNumber++);	// Type
				cell.setCellValue(mi.type);
				
				/*
				 * Get the surveys that use this resource
				 */
				ArrayList<Survey> surveys = GeneralUtilityMethods.getResourceSurveys(sd, mi.name, oId, localisation);
				int sIndex = 0;
				for(Survey s : surveys) {
					
					if(sIndex++ > 0) {
						colNumber = 0;
						row = dataSheet.createRow(rowNumber++);	
						
						cell = row.createCell(colNumber++);	// Resource
						cell.setCellValue(mi.name);
						
						cell = row.createCell(colNumber++);	// Type
						cell.setCellValue(mi.type);
					}
					
					cell = row.createCell(colNumber++);	// Project
					cell.setCellValue(s.surveyData.projectName);
					
					cell = row.createCell(colNumber++);	// Survey
					cell.setCellValue(s.surveyData.displayName);
					
					cell = row.createCell(colNumber++);	// Blocked
					cell.setCellValue(s.surveyData.blocked);
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

	private boolean setCellGood(Cell cell) {
		cell.setCellStyle(good);
		cell.setCellValue(localisation.getString("rep_yes"));
		return true;
	}
	
	private boolean setCellBad(Cell cell) {
		cell.setCellStyle(bad);
		cell.setCellValue(localisation.getString("rep_no"));
		return false;
	}
}
