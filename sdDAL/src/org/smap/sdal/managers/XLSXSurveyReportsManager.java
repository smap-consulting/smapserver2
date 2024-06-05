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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

public class XLSXSurveyReportsManager {
	
	private static Logger log =
			 Logger.getLogger(XLSXAdminReportsManager.class.getName());
	
	Authorise a = null;
	Authorise aOrg = null;
	
	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;
	boolean includeTemporaryUsers = false;
	boolean includeAllTime = false;
	String tz;
	
	public XLSXSurveyReportsManager(ResourceBundle l, String tz) {
		localisation = l;
		this.tz = tz;
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
		ArrayList<String> authorisationsOrg = new ArrayList<String> ();	
		authorisationsOrg.add(Authorise.ORG);
		aOrg = new Authorise(authorisationsOrg, null);
	}

	/*
	 * Write new background report
	 */
	public String writeNewReport(Connection sd, 
			Connection cResults, 
			int oId, 
			String user, 
			HashMap<String, String> params, String basePath) throws SQLException, IOException, ApplicationException {
		
		String filename = String.valueOf(UUID.randomUUID()) + ".xlsx";
	
		GeneralUtilityMethods.createDirectory(basePath + "/reports");
		String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
		File tempFile = new File(filepath);
		
		// start validation			
		if(oId > 0) {
			aOrg.isAuthorised(sd, user);
		} else {
			a.isAuthorised(sd, user);
		}
		// End Validation

		
		ArrayList<AR> report = getSurveyReport(sd, cResults, oId);
		
		ArrayList<String> header = new ArrayList<String> ();
		header.add(localisation.getString("ar_project_id"));
		header.add(localisation.getString("ar_project"));
		header.add(localisation.getString("ar_survey_id"));
		header.add(localisation.getString("ar_survey"));
		header.add(localisation.getString("c_del"));
		header.add(localisation.getString("c_records"));
		header.add(localisation.getString("c_marked_bad"));
		
		getNewReport(sd, tempFile, header, report,
				GeneralUtilityMethods.getOrganisationName(sd, oId));

		return filename;
	
	}
	
	private ArrayList<AR> getSurveyReport(Connection sd, Connection cResults, int oId) throws SQLException {
		
		ArrayList<AR> rows = new ArrayList<AR> ();
		StringBuilder sql = new StringBuilder("select p.id, p.name, s.ident, "
				+ "s.display_name, s.deleted, f.table_name "
				+ "from survey s, project p, form f "
				+ "where p.id = s.p_id "
				+ "and s.s_id = f.s_id "
				+ "and f.parentform = 0 "
				+ "and p.o_id = ? ");
		
		sql.append("order by p.name, s.display_name asc");
		PreparedStatement pstmt = null;
		
		PreparedStatement pstmtTable = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql.toString());		
			pstmt.setInt(1, oId);
			log.info("Survey report: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				AR ar = new AR();
				ar.p_id = rs.getInt("id");
				ar.project = rs.getString("name");
				ar.sIdent = rs.getString("ident");
				ar.survey = rs.getString("display_name");
				ar.deleted = rs.getBoolean("deleted");
				
				String tableName = rs.getString("table_name");
				if (GeneralUtilityMethods.tableExists(cResults, tableName)) {
					String sqlTable = "select count(*), _bad from " 
							+  tableName
							+ " group by _bad";
					pstmtTable = cResults.prepareStatement(sqlTable);
					ResultSet rx = pstmtTable.executeQuery();
					while(rx.next()) {
						ar.records += rx.getInt(1);
						if(rx.getBoolean("_bad")) {
							ar.bad = rx.getInt(1);
						}
					}
				}
				
				rows.add(ar);
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
			if(pstmtTable != null) {try{pstmtTable.close();}catch(Exception e) {}}
		}
		return rows;
	}
	
	/*
	 * Create the new style XLSX report
	 */
	private void getNewReport(
			Connection sd,
			File tempFile,
			ArrayList<String> header,
			ArrayList <AR> report,
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
				Row orgRow = dataSheet.createRow(rowNumber++);		
				Cell cell = orgRow.createCell(0);
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
					cell = row.createCell(colNumber++);	// Project Id
					cell.setCellValue(ar.p_id);
						
					cell = row.createCell(colNumber++);	// Project Name
					cell.setCellValue(ar.project);
						
					cell = row.createCell(colNumber++);	// Survey Ident
					cell.setCellValue(ar.sIdent);
					
					cell = row.createCell(colNumber++);	// Survey Name
					cell.setCellValue(ar.survey);
					
					cell = row.createCell(colNumber++);	// Deleted
					if(ar.deleted) {
						cell.setCellStyle(styles.get("bad"));
						cell.setCellValue(localisation.getString("rep_yes"));
					} else {
						cell.setCellStyle(styles.get("good"));
						cell.setCellValue(localisation.getString("rep_no"));
					}	
					
					cell = row.createCell(colNumber++);	// Records
					cell.setCellValue(ar.records);
					
					cell = row.createCell(colNumber++);	// Marked Bad
					cell.setCellValue(ar.bad);
						
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
