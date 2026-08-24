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
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.OrgCachedResource;
import org.smap.sdal.model.Dhis2Map;
import org.smap.sdal.model.Dhis2Server;

/*
 * DHIS2 reference data cached as organisation level CSV files
 *
 * Mirrors SharePointListMapManager, which does the same job for SharePoint lists.  The rows land
 * in a table in the csv schema through CsvTableManager, so everything downstream, the lookups,
 * the manifest and the offline delivery to FieldTask, already works
 */
public class Dhis2MapManager {

	private static Logger log = Logger.getLogger(Dhis2MapManager.class.getName());

	private static final String COLS = "id, o_id, smap_name, resource_type, "
			+ "dhis2_ref, ou_filter, refresh_minutes, "
			+ "to_char(last_sync, 'YYYY-MM-DD HH24:MI:SS') as last_sync, "
			+ "last_sync_result, coalesce(row_count, 0) as row_count, "
			+ "coalesce(csv_table_id, 0) as csv_table_id, enabled";

	public ArrayList<Dhis2Map> getMappings(Connection sd, int oId) throws SQLException {

		ArrayList<Dhis2Map> maps = new ArrayList<>();
		String sql = "select " + COLS + " from dhis2_map where o_id = ? order by smap_name asc";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				maps.add(fromResultSet(rs));
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return maps;
	}

	public Dhis2Map getMapping(Connection sd, int oId, int id) throws SQLException {

		Dhis2Map m = null;
		String sql = "select " + COLS + " from dhis2_map where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, id);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				m = fromResultSet(rs);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return m;
	}

	public int addMapping(Connection sd, int oId, Dhis2Map m) throws SQLException {

		int id = -1;
		String sql = "insert into dhis2_map (o_id, smap_name, resource_type, "
				+ "dhis2_ref, ou_filter, refresh_minutes, enabled) values (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, oId);
			pstmt.setString(2, m.smap_name);
			pstmt.setString(3, m.resource_type);
			pstmt.setString(4, m.dhis2_ref);
			pstmt.setString(5, m.ou_filter);
			pstmt.setInt(6, m.refresh_minutes > 0 ? m.refresh_minutes : 1440);
			pstmt.setBoolean(7, m.enabled);
			pstmt.executeUpdate();

			ResultSet rs = pstmt.getGeneratedKeys();
			if(rs.next()) {
				id = rs.getInt(1);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return id;
	}

	public void updateMapping(Connection sd, int oId, Dhis2Map m) throws SQLException {

		String sql = "update dhis2_map set smap_name = ?, resource_type = ?, dhis2_ref = ?, "
				+ "ou_filter = ?, refresh_minutes = ?, enabled = ? where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, m.smap_name);
			pstmt.setString(2, m.resource_type);
			pstmt.setString(3, m.dhis2_ref);
			pstmt.setString(4, m.ou_filter);
			pstmt.setInt(5, m.refresh_minutes > 0 ? m.refresh_minutes : 1440);
			pstmt.setBoolean(6, m.enabled);
			pstmt.setInt(7, oId);
			pstmt.setInt(8, m.id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}

	public void deleteMapping(Connection sd, int oId, int id) throws SQLException {

		String sql = "delete from dhis2_map where o_id = ? and id = ?";
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

	// -------------------------------------------------------------------------
	// Sync
	// -------------------------------------------------------------------------

	/*
	 * Sync every enabled mapping whose cache has expired
	 * Safe to call repeatedly from the background batch job
	 */
	public void syncDue(Connection sd, ResourceBundle localisation) {

		String sql = "select " + COLS + " from dhis2_map "
				+ "where enabled = true "
				+ "and (last_sync is null "
				+ "  or last_sync < now() - (refresh_minutes || ' minutes')::interval)";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				Dhis2Map m = fromResultSet(rs);
				try {
					syncOne(sd, m, localisation);
				} catch (Exception e) {
					log.log(Level.SEVERE, "DHIS2 sync failed for '" + m.smap_name + "': " + e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "DHIS2 syncDue error: " + e.getMessage(), e);
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}

	/*
	 * Sync one mapping immediately, also used by the sync now endpoint
	 * Returns the number of rows written
	 */
	public int syncOne(Connection sd, Dhis2Map m, ResourceBundle localisation) throws Exception {

		Dhis2Server server = new Dhis2ServerManager().getWithToken(sd, m.o_id);
		if(server == null) {
			throw new Exception("No DHIS2 connection has been set up for this organisation");
		}
		if(!server.enabled) {
			throw new Exception("The DHIS2 connection is disabled");
		}

		log.info("DHIS2 sync: " + m.resource_type + " into '" + m.smap_name + "' for organisation " + m.o_id);

		Dhis2Manager dhis2 = new Dhis2Manager();
		List<Map<String, String>> rows;

		if(Dhis2Map.TYPE_ORGUNITS.equals(m.resource_type)) {
			rows = dhis2.getOrgUnits(server, m.ou_filter);
		} else if(Dhis2Map.TYPE_OPTIONSET.equals(m.resource_type)) {
			rows = dhis2.getOptionSetOptions(server, m.dhis2_ref);
		} else {
			throw new Exception("DHIS2 resource type not supported yet: " + m.resource_type);
		}

		if(rows.isEmpty()) {
			/*
			 * Refuse to replace a working list with nothing.  An empty result is far more likely
			 * to be a filter that matches no org units, or a permissions change, than a hierarchy
			 * that genuinely became empty, and the cost of being wrong is every form losing its
			 * choice list
			 */
			throw new Exception("DHIS2 returned no rows, the cached list has been left unchanged");
		}

		String fileName = OrgCachedResource.DHIS2_PREFIX + m.smap_name;
		CsvTableManager csvMgr = new CsvTableManager(sd, localisation, m.o_id, 0, fileName);
		csvMgr.updateTableFromRows(rows);

		updateSyncStatus(sd, m.id, csvMgr.getTableId(), rows.size(), null);

		log.info("DHIS2 sync: wrote " + rows.size() + " rows to '" + fileName + "'");
		return rows.size();
	}

	/*
	 * Record the outcome so a failure is visible without reading the log
	 */
	public void updateSyncStatus(Connection sd, int id, int csvTableId, int rowCount, String error)
			throws SQLException {

		String sql = "update dhis2_map set last_sync = now(), last_sync_result = ?, "
				+ "row_count = ?, csv_table_id = case when ? > 0 then ? else csv_table_id end "
				+ "where id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, error == null ? "OK" : error);
			pstmt.setInt(2, rowCount);
			pstmt.setInt(3, csvTableId);
			pstmt.setInt(4, csvTableId);
			pstmt.setInt(5, id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}

	/*
	 * The csv table holding a named resource, used to resolve the dhis2_ prefix on lookup
	 */
	public int getCsvTableId(Connection sd, int oId, String smapName) throws SQLException {

		int id = 0;
		String sql = "select coalesce(csv_table_id, 0) from dhis2_map where o_id = ? and smap_name = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, smapName);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				id = rs.getInt(1);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return id;
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private Dhis2Map fromResultSet(ResultSet rs) throws SQLException {
		Dhis2Map m = new Dhis2Map();
		m.id = rs.getInt("id");
		m.o_id = rs.getInt("o_id");
		m.smap_name = rs.getString("smap_name");
		m.resource_type = rs.getString("resource_type");
		m.dhis2_ref = rs.getString("dhis2_ref");
		m.ou_filter = rs.getString("ou_filter");
		m.refresh_minutes = rs.getInt("refresh_minutes");
		m.last_sync = rs.getString("last_sync");
		m.last_sync_result = rs.getString("last_sync_result");
		m.row_count = rs.getInt("row_count");
		m.csv_table_id = rs.getInt("csv_table_id");
		m.enabled = rs.getBoolean("enabled");
		return m;
	}
}
