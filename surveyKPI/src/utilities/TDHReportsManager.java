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
import org.smap.sdal.custom.TDHIndividualReport;
import org.smap.sdal.custom.TDHIndividualValues;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QueryManager;
import org.smap.sdal.model.ColDesc;
import org.smap.sdal.model.ColValues;
import org.smap.sdal.model.OptionDesc;
import org.smap.sdal.model.QueryForm;
import org.smap.sdal.model.QuestionLite;
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
			CellStyle errorStyle = null;

			ArrayList<TDHIndividualReport> reports = null;
			ArrayList<String> values = null;
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
				CellStyle defaultStyle = styles.get("default");
				errorStyle = styles.get("error");			
				
				reports = getIndividualReports();
				for(TDHIndividualReport r : reports) {
					r.sheet = wb.createSheet(r.name);	
					values = getValues(cResults, r);
					
					/*
					 * Write the questions and create the rows
					 */
					rowNumber = 0;
					int colNumber = 0;
					r.sheet.setColumnWidth(0, 60 * 256);
					
					Row row = r.sheet.createRow(rowNumber++);					
					Cell cell = row.createCell(colNumber);
					cell.setCellStyle(headerStyle);				
					cell.setCellValue("Question");
					
					for(QuestionLite q : r.questions) {
						row = r.sheet.createRow(rowNumber++);
						cell = row.createCell(colNumber);
						cell.setCellStyle(defaultStyle);
						cell.setCellValue(q.name);		
					}
				}

			

			}  catch (Exception e) {
				try {cResults.setAutoCommit(true);} catch (Exception ex) {}
				log.log(Level.SEVERE, "Error", e);
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				lm.writeLogOrganisation(sd, oId, username, LogManager.REPORT, e.getMessage(), 0);
				
				String msg = e.getMessage();
				if(msg.contains("does not exist")) {
					msg = localisation.getString("msg_no_data");
				}
				if(reports != null && reports.size() > 0) {
					Sheet s1 = reports.get(0).sheet;
					if(s1 != null) {
						Row dataRow = s1.createRow(rowNumber + 1);	
						Cell cell = dataRow.createCell(0);
						cell.setCellStyle(errorStyle);
						cell.setCellValue(msg);
					}
				}
				
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
	
	private ArrayList<TDHIndividualReport> getIndividualReports()  {
		
		ArrayList<TDHIndividualReport> reports = new ArrayList<>();
		
		TDHIndividualReport r1 = new TDHIndividualReport("SDQ", "s640_main");
		r1.questions.add(new QuestionLite("string", "considerate_of_other_peoples_feelings", "considerate_of_other_peoples_feelings"));
		r1.questions.add(new QuestionLite("string", "restless_overactive_cannot_stay_still_for_long", "restless_overactive_cannot_stay_still_for_long"));
		reports.add(r1);
		
		return reports;
	}

	private ArrayList<TDHIndividualValues> getValues(Connection cResults, TDHIndividualReport report) {
		ArrayList<TDHIndividualValues> values = new ArrayList<TDHIndividualValues> ();
		return values;
	}
}
