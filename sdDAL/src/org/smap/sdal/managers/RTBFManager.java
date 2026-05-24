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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

/*
 * Right to be Forgotten (RTBF) — GDPR B2.3.2.
 *
 * Two-phase workflow:
 *   1. search()  — returns JSON array of matching records (active + history) for review
 *   2. process() — redacts a specific list of (tableName:prikey) targets chosen by the DPO
 *
 * Redaction replaces PII column values with "[REDACTED]" and cascades to child forms.
 * Search uses ILIKE (case-insensitive). partial=true wraps value in %.
 */
public class RTBFManager {

	private static final String REDACTED = "[REDACTED]";

	private static Logger log = Logger.getLogger(RTBFManager.class.getName());

	// -------------------------------------------------------------------------
	// Phase 1 — search
	// -------------------------------------------------------------------------

	/*
	 * Search all top-level forms accessible to the user for the identifier.
	 * Includes active, history, and soft-deleted rows.
	 * Returns a JSON array of match objects for review in the UI.
	 */
	public String search(
			Connection sd,
			Connection cResults,
			String user,
			String identifier,
			String fieldName,
			boolean partial,
			boolean superUser,
			ResourceBundle localisation) throws Exception {

		String matchVal = partial ? "%" + identifier + "%" : identifier;
		StringBuilder json = new StringBuilder("[");
		boolean firstEntry = true;

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

		// Only top-level forms — child rows are cascaded automatically during redact
		String sqlForms = "select f_id, name, table_name from form "
				+ "where s_id = ? and parentform = 0 order by f_id";

		String sqlPiiCols = "select column_name, qname from question "
				+ "where f_id = ? and pii is not null "
				+ "and soft_deleted = 'false' and column_name is not null";

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
				int    sId         = rsSurveys.getInt("s_id");
				String surveyName  = rsSurveys.getString("display_name");
				String projectName = rsSurveys.getString("project_name");

				pstmtForms.setInt(1, sId);
				ResultSet rsForms = pstmtForms.executeQuery();
				while (rsForms.next()) {
					int    fId       = rsForms.getInt("f_id");
					String formName  = rsForms.getString("name");
					String tableName = rsForms.getString("table_name");

					if (tableName == null
							|| !GeneralUtilityMethods.tableExists(cResults, tableName)) {
						continue;
					}

					pstmtPiiCols.setInt(1, fId);
					ResultSet rsPii = pstmtPiiCols.executeQuery();
					ArrayList<String> searchCols = new ArrayList<>();
					ArrayList<String> allPiiCols = new ArrayList<>();
					while (rsPii.next()) {
						String colName = rsPii.getString("column_name");
						String qName   = rsPii.getString("qname");
						if (!GeneralUtilityMethods.hasColumn(cResults, tableName, colName)) continue;
						if (fieldName == null
								|| fieldName.equals(qName)
								|| fieldName.equals(colName)) {
							searchCols.add(colName);
						}
						allPiiCols.add(colName);
					}

					if (searchCols.isEmpty()) continue;

					// Select prikey, _bad, _bad_reason, and all PII column values
					StringBuilder sql = new StringBuilder("select prikey, _bad, _bad_reason");
					for (String col : allPiiCols) {
						sql.append(", ").append(col);
					}
					sql.append(" from ").append(tableName).append(" where (");
					for (int i = 0; i < searchCols.size(); i++) {
						if (i > 0) sql.append(" or ");
						sql.append(searchCols.get(i)).append("::text ilike ?");
					}
					sql.append(") order by _bad, prikey");

					PreparedStatement pstmtSearch = null;
					try {
						pstmtSearch = cResults.prepareStatement(sql.toString());
						for (int i = 0; i < searchCols.size(); i++) {
							pstmtSearch.setString(i + 1, matchVal);
						}

						ResultSet rsRows = pstmtSearch.executeQuery();
						while (rsRows.next()) {
							int     prikey    = rsRows.getInt("prikey");
							boolean isBad     = rsRows.getBoolean("_bad");
							String  badReason = rsRows.getString("_bad_reason");
							String  status    = recordStatus(isBad, badReason);

							StringBuilder fields = new StringBuilder();
							boolean firstField = true;
							for (String col : allPiiCols) {
								String val = rsRows.getString(col);
								if (val != null && !val.isEmpty()) {
									if (!firstField) fields.append("; ");
									fields.append(col).append(": ").append(val);
									firstField = false;
								}
							}

							if (!firstEntry) json.append(",");
							json.append("{");
							json.append("\"target\":\"").append(je(tableName)).append(":").append(prikey).append("\"");
							json.append(",\"project\":\"").append(je(projectName)).append("\"");
							json.append(",\"survey\":\"").append(je(surveyName)).append("\"");
							json.append(",\"form\":\"").append(je(formName)).append("\"");
							json.append(",\"status\":\"").append(status).append("\"");
							json.append(",\"fields\":\"").append(je(fields.toString())).append("\"");
							json.append("}");
							firstEntry = false;
						}
					} finally {
						try { if (pstmtSearch != null) pstmtSearch.close(); } catch (SQLException e) {}
					}
				}
			}
		} finally {
			try { if (pstmtSurveys != null) pstmtSurveys.close(); } catch (SQLException e) {}
			try { if (pstmtForms   != null) pstmtForms.close();   } catch (SQLException e) {}
			try { if (pstmtPiiCols != null) pstmtPiiCols.close(); } catch (SQLException e) {}
		}

		json.append("]");
		return json.toString();
	}

	// -------------------------------------------------------------------------
	// Phase 2 — redact
	// -------------------------------------------------------------------------

	/*
	 * Redact a specific list of targets chosen by the DPO after reviewing search results.
	 * Each target is "tableName:prikey". Cascades to child forms.
	 * Verifies the requesting user has access to each table.
	 * Returns total number of top-level rows redacted.
	 */
	public int process(
			Connection sd,
			Connection cResults,
			String user,
			List<String> targets) throws Exception {

		// Parse targets: tableName -> prikeys
		Map<String, List<Integer>> byTable = new LinkedHashMap<>();
		for (String t : targets) {
			int colon = t.lastIndexOf(':');
			if (colon < 1) continue;
			String tbl = t.substring(0, colon);
			int pk;
			try { pk = Integer.parseInt(t.substring(colon + 1)); } catch (NumberFormatException e) { continue; }
			byTable.computeIfAbsent(tbl, k -> new ArrayList<>()).add(pk);
		}

		// Look up form info by table name, checking user access
		String sqlFormInfo =
				"select f.f_id, f.s_id, f.parentform from form f "
				+ "join survey s on s.s_id = f.s_id "
				+ "join user_project up on up.p_id = s.p_id "
				+ "join users u on u.id = up.u_id "
				+ "where f.table_name = ? and u.ident = ? "
				+ "and s.deleted = 'false' limit 1";

		String sqlAllForms = "select f_id, table_name, parentform from form "
				+ "where s_id = ? order by parentform, f_id";

		String sqlPiiCols = "select column_name from question "
				+ "where f_id = ? and pii is not null "
				+ "and soft_deleted = 'false' and column_name is not null";

		PreparedStatement pstmtFormInfo = null;
		PreparedStatement pstmtAllForms = null;
		PreparedStatement pstmtPiiCols  = null;

		int total = 0;

		try {
			pstmtFormInfo = sd.prepareStatement(sqlFormInfo);
			pstmtAllForms = sd.prepareStatement(sqlAllForms);
			pstmtPiiCols  = sd.prepareStatement(sqlPiiCols);

			for (Map.Entry<String, List<Integer>> entry : byTable.entrySet()) {
				String         tableName = entry.getKey();
				List<Integer>  prikeys   = entry.getValue();

				pstmtFormInfo.setString(1, tableName);
				pstmtFormInfo.setString(2, user);
				ResultSet rs = pstmtFormInfo.executeQuery();
				if (!rs.next()) continue; // no access or unknown table

				int fId        = rs.getInt("f_id");
				int sId        = rs.getInt("s_id");

				// Anonymise PII in the target table for these prikeys
				pstmtPiiCols.setInt(1, fId);
				ResultSet rsPii = pstmtPiiCols.executeQuery();
				ArrayList<String> piiCols = new ArrayList<>();
				while (rsPii.next()) {
					String col = rsPii.getString(1);
					if (GeneralUtilityMethods.hasColumn(cResults, tableName, col)) {
						piiCols.add(col);
					}
				}
				if (!piiCols.isEmpty()) {
					anonymiseRows(cResults, tableName, piiCols, new ArrayList<>(prikeys));
					total += prikeys.size();
				}

				// Load all forms for the survey and BFS-cascade to child forms
				pstmtAllForms.setInt(1, sId);
				ResultSet rsForms = pstmtAllForms.executeQuery();
				ArrayList<FormInfo> allForms = new ArrayList<>();
				while (rsForms.next()) {
					allForms.add(new FormInfo(
							rsForms.getInt("f_id"),
							rsForms.getString("table_name"),
							rsForms.getInt("parentform")));
				}

				Deque<QueueEntry> queue = new ArrayDeque<>();
				for (FormInfo fi : allForms) {
					if (fi.parentFormId == fId) {
						queue.add(new QueueEntry(fi, new ArrayList<>(prikeys)));
					}
				}

				while (!queue.isEmpty()) {
					QueueEntry qe = queue.poll();
					FormInfo   fi = qe.form;

					if (fi.tableName == null
							|| !GeneralUtilityMethods.tableExists(cResults, fi.tableName)) {
						continue;
					}

					ArrayList<Integer> childPrikeys =
							getChildPrikeys(cResults, fi.tableName, qe.parentPrikeys);
					if (childPrikeys.isEmpty()) continue;

					pstmtPiiCols.setInt(1, fi.fId);
					ResultSet rsPiiChild = pstmtPiiCols.executeQuery();
					ArrayList<String> childPiiCols = new ArrayList<>();
					while (rsPiiChild.next()) {
						String col = rsPiiChild.getString(1);
						if (GeneralUtilityMethods.hasColumn(cResults, fi.tableName, col)) {
							childPiiCols.add(col);
						}
					}
					if (!childPiiCols.isEmpty()) {
						anonymiseRows(cResults, fi.tableName, childPiiCols, childPrikeys);
					}

					for (FormInfo child : allForms) {
						if (child.parentFormId == fi.fId) {
							queue.add(new QueueEntry(child, childPrikeys));
						}
					}
				}
			}
		} finally {
			try { if (pstmtFormInfo != null) pstmtFormInfo.close(); } catch (SQLException e) {}
			try { if (pstmtAllForms != null) pstmtAllForms.close(); } catch (SQLException e) {}
			try { if (pstmtPiiCols  != null) pstmtPiiCols.close();  } catch (SQLException e) {}
		}

		return total;
	}

	// -------------------------------------------------------------------------
	// SQL helpers
	// -------------------------------------------------------------------------

	private ArrayList<Integer> getChildPrikeys(
			Connection cResults, String tableName,
			ArrayList<Integer> parentPrikeys) throws SQLException {

		if (parentPrikeys.isEmpty()) return new ArrayList<>();
		String sql = "select prikey from " + tableName
				+ " where parkey in " + inClause(parentPrikeys);

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
			for (int i = 0; i < piiCols.size(); i++) pstmt.setString(p++, REDACTED);
			for (int i = 0; i < prikeys.size(); i++) pstmt.setInt(p++, prikeys.get(i));
			int updated = pstmt.executeUpdate();
			log.info("RTBF redact: " + updated + " rows in " + tableName);
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
	// Helpers
	// -------------------------------------------------------------------------

	/*
	 * Derive status from _bad and _bad_reason.
	 * "Replaced by N" / "Discarded in favour of N" are set by the submission
	 * processor when a newer version overwrites a record.
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

	// Minimal JSON string escaping
	private String je(String s) {
		if (s == null) return "";
		return s.replace("\\", "\\\\")
				.replace("\"", "\\\"")
				.replace("\n", "\\n")
				.replace("\r", "\\r")
				.replace("\t", "\\t");
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
		final ArrayList<Integer> parentPrikeys;
		QueueEntry(FormInfo form, ArrayList<Integer> parentPrikeys) {
			this.form          = form;
			this.parentPrikeys = parentPrikeys;
		}
	}
}
