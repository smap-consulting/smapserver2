package org.smap.sdal.managers;

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

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.model.AR;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXAttendanceReportsManager {
	
	private static Logger log =
			 Logger.getLogger(XLSXAttendanceReportsManager.class.getName());
	
	Authorise a = null;
	Authorise aOrg = null;
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	String tz;
	
	public XLSXAttendanceReportsManager(ResourceBundle l, String tz) {
		localisation = l;
		this.tz = tz;
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ANALYST);
		a = new Authorise(authorisations, null);
		
		ArrayList<String> authorisationsOrg = new ArrayList<String> ();	
		authorisationsOrg.add(Authorise.ORG);
		aOrg = new Authorise(authorisationsOrg, null);
	}
	
	/*
	 * Write new attendance background report
	 */
	public String writeNewAttendanceReport(Connection sd, String user, HashMap<String, String> params, String basePath) throws SQLException, IOException, ApplicationException {
		
		String filename = String.valueOf(UUID.randomUUID()) + ".xlsx";
	
		GeneralUtilityMethods.createDirectory(basePath + "/reports");
		String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
		File tempFile = new File(filepath);
		
		// Get params
		int oId = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_O_ID, params);
		int day = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_DAY, params);	
		int month = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_MONTH, params);	
		int year = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_YEAR, params);	
	
		
		// start validation			
		if(oId > 0) {
			aOrg.isAuthorisedNoClose(sd, user);
		} else {
			a.isAuthorisedNoClose(sd, user);
		}

		if(oId <= 0) {
			oId = GeneralUtilityMethods.getOrganisationId(sd, user);
		} 
					
		if(month < 1) {
			throw new ApplicationException(localisation.getString("ar_month_gt_0"));
		}
		// End Validation

		
		ArrayList<AR> report = null;
		report = getAttendanceReport(sd, oId, month, year, day);
		
		
		ArrayList<String> header = new ArrayList<String> ();
		header.add(localisation.getString("ar_ident"));
		header.add(localisation.getString("ar_user_name"));
		header.add(localisation.getString("a_st"));
		header.add(localisation.getString("a_et"));		
		header.add(localisation.getString("ar_sub"));
		
		getReport(sd, tempFile, header, report, year, month, day,
				GeneralUtilityMethods.getOrganisationName(sd, oId));

		return filename;
	
	}
	
	public ArrayList<AR> getAttendanceReport(Connection sd, int oId, int month, int year, int day) throws SQLException {
		ArrayList<AR> rows = new ArrayList<AR> ();
		StringBuilder sql = new StringBuilder("select users.id as id,users.ident as ident, users.name as name, users.created as created, "
				+ "(select count (*) from upload_event ue, subscriber_event se "
					+ "where ue.ue_id = se.ue_id "
					+ "and se.status = 'success' "
					+ "and se.subscriber = 'results_db' "
					+ "and upload_time >=  ? "		// current day
					+ "and upload_time < ? "		// next day
					+ "and ue.user_name = users.ident) as month "
				+ "from users, user_group "
				+ "where users.o_id = ? "
				+ "and users.id = user_group.u_id "
				+ "and user_group.g_id = ? "
				+ "and not users.temporary "
				+ "order by users.ident");
		
		PreparedStatement pstmt = null;
		
		String sqlDurn = "select min(timezone(?, refresh_time)), max(timezone(?, refresh_time)), age(max(refresh_time), min(refresh_time)) "
				+ "from last_refresh_log "
				+ "where refresh_time >= ? "
				+ "and refresh_time < ? "
				+ "and o_id = ? "
				+ "and user_ident = ?";
		PreparedStatement pstmtDurn = null;
		
		try {
			
			log.info("###### tx: " + tz);
			
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, day);
			Date d1 = Date.valueOf(t1.toLocalDateTime().toLocalDate());
			
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextDay(t1);
			Date d2 = Date.valueOf(t2.toLocalDateTime().toLocalDate());
			
			pstmtDurn = sd.prepareStatement(sqlDurn);
			pstmtDurn.setString(1, tz);
			pstmtDurn.setString(2, tz);
			pstmtDurn.setTimestamp(3, GeneralUtilityMethods.startOfDay(d1, tz));
			pstmtDurn.setTimestamp(4, GeneralUtilityMethods.startOfDay(d2, tz));
			pstmtDurn.setInt(5, oId);
			
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setTimestamp(1, GeneralUtilityMethods.startOfDay(d1, tz));
			pstmt.setTimestamp(2, GeneralUtilityMethods.startOfDay(d2, tz));
			pstmt.setInt(3, oId);
			pstmt.setInt(4,  Authorise.ENUM_ID);
			
			log.info("Attendance report: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				AR ar = new AR();
				ar.userIdent = rs.getString("ident");
				ar.userName = rs.getString("name");
				ar.created = rs.getDate("created");
				ar.usageInPeriod = rs.getInt("month");
				
				pstmtDurn.setString(6,  ar.userIdent);
				log.info("Get attendance: " + pstmtDurn.toString());
				ResultSet rsDurn = pstmtDurn.executeQuery();
				if(rsDurn.next()) {
					ar.firstRefresh = rsDurn.getTimestamp(1);
					ar.lastRefresh = rsDurn.getTimestamp(2);
					ar.duration = rsDurn.getString(3);
				}
				rows.add(ar);
				
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
			if(pstmtDurn != null) {try{pstmtDurn.close();}catch(Exception e) {}}
		}
		return rows;
	}
	

	
	/*
	 * Create the new style XLSX report
	 */
	public void getReport(
			Connection sd,
			File tempFile,
			ArrayList<String> header,
			ArrayList <AR> report,
			int year,
			int month,
			int day,
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
				
				Row dayRow = dataSheet.createRow(rowNumber++);		
				cell = dayRow.createCell(0);	// Day
				cell.setCellValue(localisation.getString("c_day"));
				cell = dayRow.createCell(1);	
				cell.setCellValue(day);
				
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

				for(AR ar : report) {
					colNumber = 0;
					Row row = dataSheet.createRow(rowNumber++);	
					cell = row.createCell(colNumber++);	// ident
					cell.setCellValue(ar.userIdent);

					cell = row.createCell(colNumber++);	// Name
					cell.setCellValue(ar.userName);

					cell = row.createCell(colNumber++);	// Start Time
					if(ar.firstRefresh != null) {
						cell.setCellStyle(styles.get("time"));
						
						Calendar cal = Calendar.getInstance();
						cal.setTime(ar.firstRefresh);
						cell.setCellValue(cal.get(Calendar.HOUR_OF_DAY)  
								+ ":" + cal.get(Calendar.MINUTE)
								+ ":" + cal.get(Calendar.SECOND));
					}

					cell = row.createCell(colNumber++);	// Start Time
					if(ar.lastRefresh != null) {
						cell.setCellStyle(styles.get("time"));
						
						Calendar cal = Calendar.getInstance();
						cal.setTime(ar.lastRefresh);
						cell.setCellValue(cal.get(Calendar.HOUR_OF_DAY)  
								+ ":" + cal.get(Calendar.MINUTE)
								+ ":" + cal.get(Calendar.SECOND));
					}	

					cell = row.createCell(colNumber++);	// Monthly Usage
					cell.setCellValue(ar.usageInPeriod);

				}


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


}
