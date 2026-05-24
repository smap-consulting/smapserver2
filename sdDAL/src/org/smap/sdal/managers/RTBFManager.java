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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

/*
 * Right to be Forgotten (RTBF) — GDPR B2.3.2.
 *
 * Replaces PII column values with "[REDACTED]" in every row (including
 * soft-deleted / edit-history rows) where any PII-flagged column matches the
 * identifier across all accessible surveys. Cascades to child form rows.
 *
 * Search uses ILIKE (case-insensitive). Pass partial=true for substring match.
 * Returns total number of top-level submissions affected.
 */
public class RTBFManager {

	private static final String REDACTED = "[REDACTED]";

	private static Logger log = Logger.getLogger(RTBFManager.class.getName());

	/*
	 * Process an RTBF request across all surveys accessible to the user.
	 * Returns total number of top-level submission rows affected (active + history).
	 */
	public int process(
			Connection sd,
			Connection cResults,
			String user,
			String identifier,
			String fieldName,   // optional: restrict to this question name; null = all PII cols
			boolean partial,
			boolean superUser,
			ResourceBundle localisation) throws Exception {

		int totalAffected = 0;
		String matchVal = partial ? "%" + identifier + "%" : identifier;

		StringBuilder sqlSurveys = new StringBuilder(
				"select s.s_id "
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

		String sqlForms = "select f_id, table_name, parentform "
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
				totalAffected += processSurvey(
						sd, cResults, sId, matchVal,
						fieldName,
						pstmtForms, pstmtPiiCols);
			}

		} finally {
			try { if (pstmtSurveys != null) pstmtSurveys.close(); } catch (SQLException e) {}
			try { if (pstmtForms   != null) pstmtForms.close();   } catch (SQLException e) {}
			try { if (pstmtPiiCols != null) pstmtPiiCols.close(); } catch (SQLException e) {}
		}

		return totalAffected;
	}

	/*
	 * Process one survey. Returns number of top-level submission rows affected.
	 * BFS traversal so parents are always processed before children.
	 */
	private int processSurvey(
			Connection sd,
			Connection cResults,
			int sId,
			String matchVal,
			String fieldName,
			PreparedStatement pstmtForms,
			PreparedStatement pstmtPiiCols) throws SQLException {

		pstmtForms.setInt(1, sId);
		ResultSet rsForms = pstmtForms.executeQuery();

		ArrayList<FormInfo> allForms = new ArrayList<>();
		while (rsForms.next()) {
			allForms.add(new FormInfo(
					rsForms.getInt("f_id"),
					rsForms.getString("table_name"),
					rsForms.getInt("parentform")));
		}

		Deque<QueueEntry> queue = new ArrayDeque<>();
		for (FormInfo fi : allForms) {
			if (fi.parentFormId == 0) {
				queue.add(new QueueEntry(fi, null));  // null = top-level: search by identifier
			}
		}

		int affected = 0;

		while (!queue.isEmpty()) {
			QueueEntry entry = queue.poll();
			FormInfo fi = entry.form;

			if (fi.tableName == null || !GeneralUtilityMethods.tableExists(cResults, fi.tableName)) {
				continue;
			}

			ArrayList<Integer> prikeys;

			if (entry.parentPrikeys == null) {
				// Top-level: find ALL rows (including _bad=true history rows) matching identifier
				ArrayList<String> searchCols = getPiiSearchCols(
						sd, cResults, fi, fieldName, pstmtPiiCols);
				if (searchCols.isEmpty()) continue;
				prikeys = findMatchingPrikeys(cResults, fi.tableName, searchCols, matchVal);
				if (prikeys.isEmpty()) continue;
				affected += prikeys.size();
			} else {
				// Child form: find all rows (including history) parented by the matched rows
				prikeys = getChildPrikeys(cResults, fi.tableName, entry.parentPrikeys);
				if (prikeys.isEmpty()) continue;
			}

			ArrayList<String> piiCols = getPiiCols(sd, cResults, fi, pstmtPiiCols);
			if (!piiCols.isEmpty()) {
				anonymiseRows(cResults, fi.tableName, piiCols, prikeys);
			}

			for (FormInfo child : allForms) {
				if (child.parentFormId == fi.fId) {
					queue.add(new QueueEntry(child, prikeys));
				}
			}
		}

		return affected;
	}

	// -------------------------------------------------------------------------
	// SQL helpers
	// -------------------------------------------------------------------------

	private ArrayList<String> getPiiSearchCols(
			Connection sd, Connection cResults,
			FormInfo fi, String fieldName,
			PreparedStatement pstmtPiiCols) throws SQLException {

		ArrayList<String> cols = new ArrayList<>();
		pstmtPiiCols.setInt(1, fi.fId);
		ResultSet rs = pstmtPiiCols.executeQuery();
		while (rs.next()) {
			String colName = rs.getString("column_name");
			String qName   = rs.getString("qname");
			if (fieldName != null && !fieldName.equals(qName) && !fieldName.equals(colName)) {
				continue;
			}
			if (GeneralUtilityMethods.hasColumn(cResults, fi.tableName, colName)) {
				cols.add(colName);
			}
		}
		return cols;
	}

	private ArrayList<String> getPiiCols(
			Connection sd, Connection cResults,
			FormInfo fi,
			PreparedStatement pstmtPiiCols) throws SQLException {

		ArrayList<String> cols = new ArrayList<>();
		pstmtPiiCols.setInt(1, fi.fId);
		ResultSet rs = pstmtPiiCols.executeQuery();
		while (rs.next()) {
			String colName = rs.getString("column_name");
			if (GeneralUtilityMethods.hasColumn(cResults, fi.tableName, colName)) {
				cols.add(colName);
			}
		}
		return cols;
	}

	// Finds ALL rows (active and soft-deleted) matching the identifier
	private ArrayList<Integer> findMatchingPrikeys(
			Connection cResults, String tableName,
			ArrayList<String> searchCols, String matchVal) throws SQLException {

		ArrayList<Integer> prikeys = new ArrayList<>();
		StringBuilder sql = new StringBuilder("select prikey from ");
		sql.append(tableName).append(" where (");
		for (int i = 0; i < searchCols.size(); i++) {
			if (i > 0) sql.append(" or ");
			sql.append(searchCols.get(i)).append("::text ilike ?");
		}
		sql.append(")");

		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql.toString());
			for (int i = 0; i < searchCols.size(); i++) {
				pstmt.setString(i + 1, matchVal);
			}
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				prikeys.add(rs.getInt(1));
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}
		return prikeys;
	}

	// Finds ALL child rows (active and soft-deleted) for the given parent prikeys
	private ArrayList<Integer> getChildPrikeys(
			Connection cResults, String tableName,
			ArrayList<Integer> parentPrikeys) throws SQLException {

		if (parentPrikeys.isEmpty()) return new ArrayList<>();
		String sql = "select prikey from " + tableName + " where parkey in " + inClause(parentPrikeys);

		ArrayList<Integer> prikeys = new ArrayList<>();
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql);
			for (int i = 0; i < parentPrikeys.size(); i++) {
				pstmt.setInt(i + 1, parentPrikeys.get(i));
			}
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) prikeys.add(rs.getInt(1));
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}
		return prikeys;
	}

	private void anonymiseRows(Connection cResults, String tableName,
			ArrayList<String> piiCols, ArrayList<Integer> prikeys) throws SQLException {

		StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
		for (int i = 0; i < piiCols.size(); i++) {
			if (i > 0) sql.append(", ");
			sql.append(piiCols.get(i)).append(" = ?");
		}
		sql.append(" where prikey in ").append(inClause(prikeys));

		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql.toString());
			int p = 1;
			for (int i = 0; i < piiCols.size(); i++) {
				pstmt.setString(p++, REDACTED);
			}
			for (int i = 0; i < prikeys.size(); i++) {
				pstmt.setInt(p++, prikeys.get(i));
			}
			int updated = pstmt.executeUpdate();
			log.info("RTBF anonymise: " + updated + " rows in " + tableName);
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}
	}

	private String inClause(ArrayList<Integer> ids) {
		StringBuilder sb = new StringBuilder("(");
		for (int i = 0; i < ids.size(); i++) {
			if (i > 0) sb.append(",");
			sb.append("?");
		}
		sb.append(")");
		return sb.toString();
	}

	// -------------------------------------------------------------------------
	// Inner helpers
	// -------------------------------------------------------------------------

	private static class FormInfo {
		final int    fId;
		final String tableName;
		final int    parentFormId;
		FormInfo(int fId, String tableName, int parentFormId) {
			this.fId          = fId;
			this.tableName    = tableName;
			this.parentFormId = parentFormId;
		}
	}

	private static class QueueEntry {
		final FormInfo           form;
		final ArrayList<Integer> parentPrikeys;  // null = top-level search phase
		QueueEntry(FormInfo form, ArrayList<Integer> parentPrikeys) {
			this.form          = form;
			this.parentPrikeys = parentPrikeys;
		}
	}
}
