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
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.AR;
import org.smap.sdal.model.Survey;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXAdminReportsManagerFormAccess {
	
	private static Logger log =
			 Logger.getLogger(SurveyInfo.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	
	public XLSXAdminReportsManagerFormAccess(ResourceBundle l) {
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
			int oId,
			String formIdent) {
		
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
		Sheet overviewSheet = null;
		Sheet dataSheet = null;
		CellStyle errorStyle = null;

		try {
				
			/*
			 * Create XLSX File
			 */
			GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
			wb = new SXSSFWorkbook(10);		// Serialised output
			overviewSheet = wb.createSheet("overview");
			dataSheet = wb.createSheet("data");
			
			Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
			CellStyle headerStyle = styles.get("header");
			errorStyle = styles.get("errorStyle");
			
			/*
			 * Write the overview data
			 */
			rowNumber = 0;

			SurveyManager sm = new SurveyManager(localisation, "UTC");
			int sId = GeneralUtilityMethods.getSurveyId(sd, formIdent);
			Survey survey = sm.getById(
					sd, 
					null, 		// cResults
					request.getRemoteUser(), 
					sId, 
					false,		// full details
					null, 		// basePath
					null, 		// instance id
					false, 		// get results
					false, 		// generate dummmy values
					false, 		// get property type 
					true, 		// get soft deleted
					false, 		// get hrk
					"internal",	// get external options 
					false, 		// get changed history
					true, 		// get roles
					true, 		// Pretend to be super user
					null			// geom format
					);
			/*
			 * Write the headers
			 */	
			Row row = overviewSheet.createRow(rowNumber++);		
			
			Cell cell = row.createCell(0);	// Ident
			cell.setCellValue(localisation.getString("rep_form_ident"));
			cell = row.createCell(1);	
			cell.setCellStyle(styles.get("good"));
			cell.setCellValue(formIdent);
			
			row = overviewSheet.createRow(rowNumber++);		
			cell = row.createCell(0);	// Found
			cell.setCellValue(localisation.getString("rep_found"));
			cell = row.createCell(1);	
			if(survey != null) {
				cell.setCellStyle(styles.get("good"));
				cell.setCellValue(localisation.getString("rep_yes"));
			} else {
				cell.setCellStyle(errorStyle);
				cell.setCellValue(localisation.getString("rep_no"));
			}
			
			if(survey != null) {
				row = overviewSheet.createRow(rowNumber++);		
				cell = row.createCell(0);	// Survey Name
				cell.setCellValue(localisation.getString("name"));
				cell = row.createCell(1);	
				cell.setCellStyle(styles.get("good"));
				cell.setCellValue(survey.displayName);
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
