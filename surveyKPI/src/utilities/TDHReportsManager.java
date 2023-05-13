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
import java.sql.ResultSet;
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

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.custom.TDHIndividualReport;
import org.smap.sdal.custom.TDHIndividualValues;
import org.smap.sdal.custom.TDHValue;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.QuestionLite;


/*
 * Manage exporting of custom TDH reports
 */

public class TDHReportsManager {
	
	private static Logger log =
			 Logger.getLogger(TDHReportsManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	String tz;
	
	private class TDHSurvey {
		String name;
		String table;
		String ident;
		public TDHSurvey(String name, String table, String ident) {
			this.name = name;
			this.table = table;
			this.ident = ident;
		}
	}
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
			int oId = 0;
			
			try {	
			
				oId = GeneralUtilityMethods.getOrganisationId(sd, username);
				lm.writeLogOrganisation(sd, oId, username, LogManager.REPORT, " Export custom TDH individual Report ", 0);
				
				/*
				 * Create XLSX File
				 */
				log.info("####################### Create XLSX file");
				GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
				wb = new XSSFWorkbook();		// Serialised output
				Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
				CellStyle headerStyle = styles.get("header");
				CellStyle defaultStyle = styles.get("default");
				CellStyle dateHeader = styles.get("dateHeader");
				errorStyle = styles.get("error");			
				
				reports = getIndividualReports(sd, cResults, beneficiaryCode);
				boolean worksheetWritten = false;
				for(TDHIndividualReport r : reports) {
					if(!r.reportEmpty) {
						r.sheet = wb.createSheet(r.name);	
						worksheetWritten = true;
						
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
						
						if(r.values.size() > 0) {
							TDHIndividualValues dataCol = r.values.get(0);
							int idx = 0;
							for(QuestionLite q : r.questions) {
								TDHValue tValue = dataCol.values.get(idx);
								if(tValue.rowExists) {
									row = r.sheet.createRow(rowNumber++);
									cell = row.createCell(colNumber);
									cell.setCellStyle(defaultStyle);
									cell.setCellValue(q.name);	
								}
								idx++;
							}
						}
						
						/*
						 * Write each column of values
						 */
						for(TDHIndividualValues dataCol : r.values) {
							colNumber++;
							rowNumber = 0;
							
							// Add date of record
							row = r.sheet.getRow(rowNumber++);
							
							cell = row.createCell(colNumber);
							cell.setCellStyle(dateHeader);
							cell.setCellValue(dataCol.date);	
							
							for(TDHValue tValue : dataCol.values) {
								if(tValue.rowExists) {
									row = r.sheet.getRow(rowNumber++);
									cell = row.createCell(colNumber);
									cell.setCellStyle(defaultStyle);
									cell.setCellValue(tValue.value);
								}
							}
						}
					}
				}
				if(!worksheetWritten) {
					Sheet sheet = wb.createSheet("no data");	
				}

			}  catch (Exception e) {
				try {cResults.setAutoCommit(true);} catch (Exception ex) {}
				log.log(Level.SEVERE, "Error", e);
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				lm.writeLogOrganisation(sd, oId, username, LogManager.REPORT, e.getMessage(), 0);
				
				String msg = e.getMessage();
				if(msg == null) {
					msg = "";
				}
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
				} catch (Exception ex) {
					log.log(Level.SEVERE, "Error", ex);
				}
				
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}


			}
		}

		return responseVal;
	}
	
	private ArrayList<TDHIndividualReport> getIndividualReports(Connection sd, Connection cResults, String beneficiaryCode) throws SQLException  {
		
		ArrayList<TDHIndividualReport> reports = new ArrayList<>();
		
		String questionGet = "select q.qname, q.column_name "
				+ "from question q "
				+ "where q.published "
				+ "and not q.soft_deleted "
				+ "and not q.readonly "
				+ "and q.source = 'user' "
				+ "and q.f_id in (select f_id from form where parentform = 0 and s_id in (select s_id from survey where ident = ?)) "
				+ "order by q.seq asc";
		
		PreparedStatement pstmt = null;
		
		// Add the surveys to be indluded in the report
		ArrayList<TDHSurvey> surveys = new ArrayList<>();
		surveys.add(new TDHSurvey("SDQ", "s640_main", "s22_640"));
		surveys.add(new TDHSurvey("PSS MGS دعم نفسي اجتماعي للاطفال", "s1085_main", "s106_1085"));
		surveys.add(new TDHSurvey("LS مهارات الحياة", "s1087_main", "s106_1087"));
		//surveys.add(new TDHSurvey("Awareness sessions training", "s523_main", "s43_523"));
		//surveys.add(new TDHSurvey("Case management training", "s520_main", "s43_520"));
		
		// Create the report definitions and add the questions
		try {
			pstmt = sd.prepareStatement(questionGet);
			
			for(TDHSurvey s : surveys) {
				TDHIndividualReport r = new TDHIndividualReport(s.name, s.table);
				reports.add(r);
				
				// Get questions
				pstmt.setString(1,  s.ident);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					r.questions.add(new QuestionLite("string", rs.getString("qname"), rs.getString("column_name")));
				}
				
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		
		// Populate the reports with data
		for(TDHIndividualReport r : reports) {
			r.values = getValues(cResults, r, beneficiaryCode);
			markMissingData(r);
		}
		
		return reports;
	}

	private void markMissingData(TDHIndividualReport r) {
		r.reportEmpty = true;
		if(r.values.size() > 0) {
			TDHIndividualValues firstCol = r.values.get(0);
			for(int i = 0; i <firstCol.values.size(); i++) {
	
				boolean nonNullRow = false;
				for(TDHIndividualValues col : r.values) {
					TDHValue colRow = col.values.get(i);
					if(colRow.value != null) {
						nonNullRow = true;
						r.reportEmpty = false;
						break;
					}
				}
				for(TDHIndividualValues col : r.values) {
					TDHValue colRow = col.values.get(i);
					colRow.rowExists = nonNullRow;
				}
			}
		}
	}
	
	private ArrayList<TDHIndividualValues> getValues(Connection cResults, 
			TDHIndividualReport report,
			String beneficiaryCode) throws SQLException {
		ArrayList<TDHIndividualValues> values = new ArrayList<TDHIndividualValues> ();
		
		StringBuilder sql = new StringBuilder("select _upload_time, ");
		sql.append(getColumnsFromReport(report));
		sql.append(" from ")
			.append(report.tableName)
			.append(" where barcode_registrationnumber = ? or manual_registrationnumber = ? ")
			.append(" order by _upload_time asc");

		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql.toString());
			pstmt.setString(1,  beneficiaryCode);
			pstmt.setString(2, beneficiaryCode);
			log.info(pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				TDHIndividualValues v = new TDHIndividualValues();
				values.add(v);
				v.date = rs.getTimestamp("_upload_time");
				for(QuestionLite q : report.questions) {
					String qValue = rs.getString(q.column_name);
					if(qValue != null) {
						qValue = qValue.trim();
						if(qValue.length() == 0) {
							qValue = null;
						}
					}					
					v.values.add(new TDHValue(qValue));
				}
			}
			
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}

		return values;
	}
	
	private String getColumnsFromReport(TDHIndividualReport report) {
		StringBuilder qList = new StringBuilder("");
		for(QuestionLite q : report.questions) {
			if(qList.length() > 0) {
				qList.append(", ");
			}
			qList.append(q.column_name);
		}
		return qList.toString();
	}
}
