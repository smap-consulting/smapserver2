package org.smap.sdal.managers;

/*****************************************************************************

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

 ******************************************************************************/

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.smap.sdal.model.Dhis2Export;
import org.smap.sdal.model.Dhis2ExportItem;

/*
 * The export mappings belonging to a survey bundle
 *
 * Everything is scoped by organisation as well as by bundle, so a guessed id cannot reach
 * another tenant's mapping
 */
public class Dhis2ExportConfigManager {

	private static Logger log = Logger.getLogger(Dhis2ExportConfigManager.class.getName());

	private static final String COLS = "id, o_id, group_survey_ident, dataset_uid, dataset_name, "
			+ "period_type, period_question, orgunit_question, enabled, "
			+ "coalesce(auto_export, false) as auto_export, "
			+ "coalesce(schedule_minutes, 1440) as schedule_minutes, "
			+ "coalesce(periods_back, 1) as periods_back, "
			+ "to_char(last_auto_export, 'YYYY-MM-DD HH24:MI:SS') as last_auto_export, "
			+ "to_char(last_export, 'YYYY-MM-DD HH24:MI:SS') as last_export, last_export_result";

	/*
	 * The exports defined for one bundle
	 */
	public ArrayList<Dhis2Export> getExports(Connection sd, int oId, String groupSurveyIdent)
			throws SQLException {

		ArrayList<Dhis2Export> exports = new ArrayList<>();
		String sql = "select " + COLS + " from dhis2_export "
				+ "where o_id = ? and group_survey_ident = ? order by dataset_name asc";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, groupSurveyIdent);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				exports.add(fromResultSet(rs));
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		for(Dhis2Export e : exports) {
			e.items = getItems(sd, e.id);
		}

		return exports;
	}

	/*
	 * One export with its mapped values
	 */
	public Dhis2Export getExport(Connection sd, int oId, int id) throws SQLException {

		Dhis2Export export = null;
		String sql = "select " + COLS + " from dhis2_export where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, id);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				export = fromResultSet(rs);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		if(export != null) {
			export.items = getItems(sd, export.id);
		}

		return export;
	}

	public ArrayList<Dhis2ExportItem> getItems(Connection sd, int exportId) throws SQLException {

		ArrayList<Dhis2ExportItem> items = new ArrayList<>();
		String sql = "select id, e_id, question_name, aggregation, data_element, "
				+ "category_option_combo, seq from dhis2_export_item where e_id = ? order by seq asc, id asc";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, exportId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				Dhis2ExportItem item = new Dhis2ExportItem();
				item.id = rs.getInt("id");
				item.e_id = rs.getInt("e_id");
				item.question_name = rs.getString("question_name");
				item.aggregation = rs.getString("aggregation");
				item.data_element = rs.getString("data_element");
				item.category_option_combo = rs.getString("category_option_combo");
				item.seq = rs.getInt("seq");
				items.add(item);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return items;
	}

	/*
	 * Create or update an export and its values
	 *
	 * The values are replaced wholesale rather than diffed. They carry no state worth preserving,
	 * and a diff would be more code and more ways to leave an orphan behind
	 */
	public int save(Connection sd, int oId, Dhis2Export export) throws SQLException {

		boolean autoCommit = sd.getAutoCommit();
		sd.setAutoCommit(false);

		try {
			int id = export.id;

			if(id > 0) {
				String sql = "update dhis2_export set dataset_uid = ?, dataset_name = ?, "
						+ "period_type = ?, period_question = ?, orgunit_question = ?, enabled = ?, "
						+ "auto_export = ?, schedule_minutes = ?, periods_back = ? "
						+ "where o_id = ? and id = ?";
				PreparedStatement pstmt = null;
				try {
					pstmt = sd.prepareStatement(sql);
					pstmt.setString(1, export.dataset_uid);
					pstmt.setString(2, export.dataset_name);
					pstmt.setString(3, export.period_type);
					pstmt.setString(4, emptyToNull(export.period_question));
					pstmt.setString(5, export.orgunit_question);
					pstmt.setBoolean(6, export.enabled);
					pstmt.setBoolean(7, export.auto_export);
					pstmt.setInt(8, export.schedule_minutes > 0 ? export.schedule_minutes : 1440);
					pstmt.setInt(9, export.periods_back >= 0 ? export.periods_back : 1);
					pstmt.setInt(10, oId);
					pstmt.setInt(11, id);
					pstmt.executeUpdate();
				} finally {
					if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
				}
			} else {
				String sql = "insert into dhis2_export (o_id, group_survey_ident, dataset_uid, "
						+ "dataset_name, period_type, period_question, orgunit_question, enabled, "
						+ "auto_export, schedule_minutes, periods_back) "
						+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				PreparedStatement pstmt = null;
				try {
					pstmt = sd.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
					pstmt.setInt(1, oId);
					pstmt.setString(2, export.group_survey_ident);
					pstmt.setString(3, export.dataset_uid);
					pstmt.setString(4, export.dataset_name);
					pstmt.setString(5, export.period_type);
					pstmt.setString(6, emptyToNull(export.period_question));
					pstmt.setString(7, export.orgunit_question);
					pstmt.setBoolean(8, export.enabled);
					pstmt.setBoolean(9, export.auto_export);
					pstmt.setInt(10, export.schedule_minutes > 0 ? export.schedule_minutes : 1440);
					pstmt.setInt(11, export.periods_back >= 0 ? export.periods_back : 1);
					pstmt.executeUpdate();
					ResultSet rs = pstmt.getGeneratedKeys();
					if(rs.next()) {
						id = rs.getInt(1);
					}
				} finally {
					if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
				}
			}

			replaceItems(sd, id, export.items);

			sd.commit();
			return id;

		} catch (SQLException e) {
			try { sd.rollback(); } catch(SQLException ex) {}
			throw e;
		} finally {
			try { sd.setAutoCommit(autoCommit); } catch(SQLException e) {}
		}
	}

	private void replaceItems(Connection sd, int exportId, ArrayList<Dhis2ExportItem> items)
			throws SQLException {

		PreparedStatement pstmtDel = null;
		PreparedStatement pstmtIns = null;

		try {
			pstmtDel = sd.prepareStatement("delete from dhis2_export_item where e_id = ?");
			pstmtDel.setInt(1, exportId);
			pstmtDel.executeUpdate();

			if(items == null || items.isEmpty()) {
				return;
			}

			pstmtIns = sd.prepareStatement("insert into dhis2_export_item "
					+ "(e_id, question_name, aggregation, data_element, category_option_combo, seq) "
					+ "values (?, ?, ?, ?, ?, ?)");
			int seq = 0;
			for(Dhis2ExportItem item : items) {
				pstmtIns.setInt(1, exportId);
				pstmtIns.setString(2, emptyToNull(item.question_name));
				pstmtIns.setString(3, item.aggregation);
				pstmtIns.setString(4, item.data_element);
				pstmtIns.setString(5, emptyToNull(item.category_option_combo));
				pstmtIns.setInt(6, seq++);
				pstmtIns.addBatch();
			}
			pstmtIns.executeBatch();

		} finally {
			if(pstmtDel != null) {try{pstmtDel.close();} catch(SQLException e) {}}
			if(pstmtIns != null) {try{pstmtIns.close();} catch(SQLException e) {}}
		}
	}

	public void delete(Connection sd, int oId, int id) throws SQLException {

		String sql = "delete from dhis2_export where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}

	/*
	 * The exports that are due to run unattended
	 *
	 * Only exports that have been enabled and had auto export switched on, which is deliberate:
	 * a mapping is proved by hand with a dry run before it is left to write on its own
	 */
	public ArrayList<Dhis2Export> getDue(Connection sd) throws SQLException {

		ArrayList<Dhis2Export> exports = new ArrayList<>();
		String sql = "select " + COLS + " from dhis2_export "
				+ "where enabled = true and auto_export = true "
				+ "and (last_auto_export is null "
				+ "  or last_auto_export < now() - (coalesce(schedule_minutes, 1440) || ' minutes')::interval)";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				exports.add(fromResultSet(rs));
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		for(Dhis2Export e : exports) {
			e.items = getItems(sd, e.id);
		}

		return exports;
	}

	public void recordAutoExport(Connection sd, int id, String result) throws SQLException {

		String sql = "update dhis2_export set last_auto_export = now(), last_export = now(), "
				+ "last_export_result = ? where id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, result);
			pstmt.setInt(2, id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}

	/*
	 * Record what happened, so the result is visible without reading the log
	 */
	public void recordExport(Connection sd, int oId, int id, String result) throws SQLException {

		String sql = "update dhis2_export set last_export = now(), last_export_result = ? "
				+ "where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, result);
			pstmt.setInt(2, oId);
			pstmt.setInt(3, id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private Dhis2Export fromResultSet(ResultSet rs) throws SQLException {
		Dhis2Export e = new Dhis2Export();
		e.id = rs.getInt("id");
		e.o_id = rs.getInt("o_id");
		e.group_survey_ident = rs.getString("group_survey_ident");
		e.dataset_uid = rs.getString("dataset_uid");
		e.dataset_name = rs.getString("dataset_name");
		e.period_type = rs.getString("period_type");
		e.period_question = rs.getString("period_question");
		e.orgunit_question = rs.getString("orgunit_question");
		e.enabled = rs.getBoolean("enabled");
		e.auto_export = rs.getBoolean("auto_export");
		e.schedule_minutes = rs.getInt("schedule_minutes");
		e.periods_back = rs.getInt("periods_back");
		e.last_auto_export = rs.getString("last_auto_export");
		e.last_export = rs.getString("last_export");
		e.last_export_result = rs.getString("last_export_result");
		return e;
	}

	private String emptyToNull(String v) {
		return (v == null || v.trim().length() == 0) ? null : v.trim();
	}
}
