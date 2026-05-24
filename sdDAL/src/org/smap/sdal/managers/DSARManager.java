package org.smap.sdal.managers;

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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;

/*
 * Data Subject Access Request (DSAR) export — GDPR B2.3.1.
 *
 * Searches every PII-flagged column across all surveys accessible to the user,
 * collecting full submission rows that contain the supplied identifier value.
 * Includes active, soft-deleted, and edit-history rows.
 * Writes a single XLSX workbook with one sheet per matching survey/form pair.
 *
 * Each sheet has Project/Survey/Form/Status prepended as the first four columns.
 * Status values: "Live" (active), "History" (replaced by edit), "Deleted" (admin delete).
 * PII column headers and cells are highlighted in yellow.
 *
 * Search is case-insensitive exact match (ILIKE) by default.
 * Pass partial=true for a substring match (ILIKE '%value%') for free-text name fields.
 */
public class DSARManager {

	private static Logger log = Logger.getLogger(DSARManager.class.getName());

	// Number of context columns prepended before data columns
	private static final int CTX_COLS = 4;  // Project, Survey, Form, Status

	public void export(
			Connection sd,
			Connection cResults,
			OutputStream out,
			String user,
			String identifier,
			String fieldName,   // optional: restrict to this question name; null = all PII cols
			boolean partial,    // true = substring match; false = exact (case-insensitive)
			boolean superUser,
			ResourceBundle localisation) throws Exception {

		SXSSFWorkbook wb = new SXSSFWorkbook(100);
		try {
			Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
			CellStyle headerStyle    = styles.get("header");             // bold
			CellStyle piiHeaderStyle = styles.get("header_assignments"); // bold + yellow
			CellStyle plainStyle     = styles.get("default");
			CellStyle piiStyle       = styles.get("yellow");             // yellow background
			CellStyle liveStyle      = styles.get("good");               // green
			CellStyle historyStyle   = styles.get("yellow");             // yellow
			CellStyle deletedStyle   = styles.get("bad");                // coral

			StringBuilder sqlSurveys = new StringBuilder(
					"select s.s_id, s.display_name, p.name as project_name "
					+ "from survey s "
					+ "join user_project up on s.p_id = up.p_id "
					+ "join users u on u.id = up.u_id "
					+ "join project p on p.id = up.p_id and p.o_id = u.o_id "
					+ "where u.ident = ? "
					+ "and s.deleted = 'false' "
					+ "and s.blocked = 'false' ");
			if (!superUser) {
				sqlSurveys.append(GeneralUtilityMethods.getSurveyRBAC());
			}
			sqlSurveys.append("order by p.name, s.display_name");

			String sqlForms = "select f_id, name, table_name "
					+ "from form where s_id = ? "
					+ "order by parentform, f_id";

			String sqlPiiCols = "select column_name, qname "
					+ "from question "
					+ "where f_id = ? "
					+ "and pii is not null "
					+ "and soft_deleted = 'false' "
					+ "and column_name is not null";

			PreparedStatement pstmtSurveys = null;
			PreparedStatement pstmtForms   = null;
			PreparedStatement pstmtPiiCols = null;

			try {
				pstmtSurveys = sd.prepareStatement(sqlSurveys.toString());
				pstmtForms   = sd.prepareStatement(sqlForms);
				pstmtPiiCols = sd.prepareStatement(sqlPiiCols);

				int idx = 1;
				pstmtSurveys.setString(idx++, user);
				if (!superUser) {
					pstmtSurveys.setString(idx++, user);
				}

				ResultSet rsSurveys = pstmtSurveys.executeQuery();
				while (rsSurveys.next()) {
					int sId = rsSurveys.getInt("s_id");
					String surveyName  = rsSurveys.getString("display_name");
					String projectName = rsSurveys.getString("project_name");

					pstmtForms.setInt(1, sId);
					ResultSet rsForms = pstmtForms.executeQuery();
					while (rsForms.next()) {
						int    fId       = rsForms.getInt("f_id");
						String formName  = rsForms.getString("name");
						String tableName = rsForms.getString("table_name");

						if (tableName == null || !GeneralUtilityMethods.tableExists(cResults, tableName)) {
							continue;
						}

						// Collect PII column names that exist in the actual table
						pstmtPiiCols.setInt(1, fId);
						ResultSet rsPii = pstmtPiiCols.executeQuery();
						ArrayList<String> searchCols = new ArrayList<>();
						HashSet<String>   piiColSet  = new HashSet<>();
						while (rsPii.next()) {
							String colName = rsPii.getString("column_name");
							String qName   = rsPii.getString("qname");
							if (fieldName != null
									&& !fieldName.equals(qName)
									&& !fieldName.equals(colName)) {
								continue;
							}
							if (GeneralUtilityMethods.hasColumn(cResults, tableName, colName)) {
								searchCols.add(colName);
								piiColSet.add(colName);
							}
						}

						if (searchCols.isEmpty()) {
							continue;
						}

						writeSheet(wb, cResults, tableName, searchCols, piiColSet,
								identifier, partial,
								projectName, surveyName, formName,
								headerStyle, piiHeaderStyle, plainStyle, piiStyle,
								liveStyle, historyStyle, deletedStyle);
					}
				}

			} finally {
				try { if (pstmtSurveys != null) pstmtSurveys.close(); } catch (SQLException e) {}
				try { if (pstmtForms   != null) pstmtForms.close();   } catch (SQLException e) {}
				try { if (pstmtPiiCols != null) pstmtPiiCols.close(); } catch (SQLException e) {}
			}

			// If nothing matched, add a placeholder sheet so the file is valid
			if (wb.getNumberOfSheets() == 0) {
				Sheet empty = wb.createSheet("No results");
				Row r = empty.createRow(0);
				r.createCell(0).setCellValue("No submissions matched identifier: " + identifier);
			}

			wb.write(out);

		} finally {
			wb.close();
			wb.dispose();
		}
	}

	/*
	 * Execute the search for one form table and write a sheet if rows match.
	 * Sheet name is truncated to Excel's 31-char limit.
	 */
	private void writeSheet(
			SXSSFWorkbook wb,
			Connection cResults,
			String tableName,
			ArrayList<String> searchCols,
			HashSet<String> piiColSet,
			String identifier,
			boolean partial,
			String projectName,
			String surveyName,
			String formName,
			CellStyle headerStyle,
			CellStyle piiHeaderStyle,
			CellStyle plainStyle,
			CellStyle piiStyle,
			CellStyle liveStyle,
			CellStyle historyStyle,
			CellStyle deletedStyle) throws SQLException {

		String matchVal = partial ? "%" + identifier + "%" : identifier;

		// Include all rows — active, edit-history (_bad=true replaced by edit),
		// and admin-deleted — so the DPO sees the complete picture
		StringBuilder sql = new StringBuilder("select * from ");
		sql.append(tableName).append(" where (");
		for (int i = 0; i < searchCols.size(); i++) {
			if (i > 0) sql.append(" or ");
			sql.append(searchCols.get(i)).append("::text ilike ?");
		}
		sql.append(") order by prikey");

		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql.toString());
			for (int i = 0; i < searchCols.size(); i++) {
				pstmt.setString(i + 1, matchVal);
			}

			ResultSet rs = pstmt.executeQuery();
			if (!rs.next()) {
				return;  // no matching rows — skip sheet
			}

			ResultSetMetaData meta = rs.getMetaData();
			int colCount = meta.getColumnCount();

			String sheetName = sheetName(projectName, surveyName, formName);
			Sheet sheet = wb.createSheet(sheetName);
			int rowNum = 0;

			// Header row: Project, Survey, Form, Status, then data columns
			Row header = sheet.createRow(rowNum++);
			header.createCell(0).setCellValue("Project");
			header.getCell(0).setCellStyle(headerStyle);
			header.createCell(1).setCellValue("Survey");
			header.getCell(1).setCellStyle(headerStyle);
			header.createCell(2).setCellValue("Form");
			header.getCell(2).setCellStyle(headerStyle);
			header.createCell(3).setCellValue("Status");
			header.getCell(3).setCellStyle(headerStyle);

			for (int c = 1; c <= colCount; c++) {
				String colName = meta.getColumnName(c);
				Cell cell = header.createCell(CTX_COLS + c - 1);
				cell.setCellValue(colName);
				cell.setCellStyle(piiColSet.contains(colName) ? piiHeaderStyle : headerStyle);
			}

			// Data rows — first row already fetched above via rs.next()
			do {
				Row row = sheet.createRow(rowNum++);
				row.createCell(0).setCellValue(projectName);
				row.createCell(1).setCellValue(surveyName);
				row.createCell(2).setCellValue(formName);

				boolean isBad = rs.getBoolean("_bad");
				String badReason = rs.getString("_bad_reason");
				String status = recordStatus(isBad, badReason);
				CellStyle statusStyle = isBad
						? (status.equals("History") ? historyStyle : deletedStyle)
						: liveStyle;
				Cell statusCell = row.createCell(3);
				statusCell.setCellValue(status);
				statusCell.setCellStyle(statusStyle);

				for (int c = 1; c <= colCount; c++) {
					String colName = meta.getColumnName(c);
					String val = rs.getString(c);
					Cell cell = row.createCell(CTX_COLS + c - 1);
					cell.setCellValue(val != null ? val : "");
					if (piiColSet.contains(colName)) {
						cell.setCellStyle(piiStyle);
					}
				}
			} while (rs.next());

			log.info("DSAR: wrote sheet '" + sheetName + "' (" + (rowNum - 1) + " rows)");

		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}
	}

	/*
	 * Derive a human-readable record status from _bad and _bad_reason.
	 * "Replaced by N" / "Discarded in favour of N" are set by the submission
	 * processor when a newer version overwrites a record — these are History.
	 * Any other _bad=true reason (admin delete, incomplete) is Deleted.
	 */
	private String recordStatus(boolean isBad, String badReason) {
		if (!isBad) return "Live";
		if (badReason != null
				&& (badReason.startsWith("Replaced by")
						|| badReason.startsWith("Discarded in favour of"))) {
			return "History";
		}
		return "Deleted";
	}

	// Excel sheet names are max 31 chars and cannot contain : \ / ? * [ ]
	private String sheetName(String project, String survey, String form) {
		String raw = project + " - " + survey + (form.equals("main") ? "" : " - " + form);
		String safe = raw.replaceAll("[:\\\\/?*\\[\\]]", "_");
		return safe.length() > 31 ? safe.substring(0, 31) : safe;
	}
}
