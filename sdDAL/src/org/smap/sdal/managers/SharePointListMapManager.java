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

import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.SharePointListMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Manages the sharepoint_list_map table and the periodic sync of SharePoint
 * list data into the local CSV cache.
 */
public class SharePointListMapManager {

	private static Logger log = Logger.getLogger(SharePointListMapManager.class.getName());

	private static final int MAX_ROWS = 5000;
	private static final String FILENAME_PREFIX = "sharepointlist_";

	// -------------------------------------------------------------------------
	// CRUD
	// -------------------------------------------------------------------------

	public ArrayList<SharePointListMap> getMappings(Connection sd, int oId) throws SQLException {
		ArrayList<SharePointListMap> maps = new ArrayList<>();
		String sql = "select id, o_id, smap_name, list_title, refresh_minutes, last_sync, "
				+ "coalesce(csv_table_id, 0) as csv_table_id, enabled "
				+ "from sharepoint_list_map where o_id = ? order by smap_name";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				SharePointListMap m = new SharePointListMap();
				m.id             = rs.getInt("id");
				m.o_id           = rs.getInt("o_id");
				m.smap_name      = rs.getString("smap_name");
				m.list_title     = rs.getString("list_title");
				m.refresh_minutes = rs.getInt("refresh_minutes");
				m.last_sync      = rs.getTimestamp("last_sync");
				m.csv_table_id   = rs.getInt("csv_table_id");
				m.enabled        = rs.getBoolean("enabled");
				maps.add(m);
			}
		} finally {
			if(pstmt != null) try { pstmt.close(); } catch(SQLException e) {}
		}
		return maps;
	}

	public int addMapping(Connection sd, int oId, SharePointListMap m) throws SQLException {
		String sql = "insert into sharepoint_list_map(o_id, smap_name, list_title, refresh_minutes, enabled) "
				+ "values(?, ?, ?, ?, ?)";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, oId);
			pstmt.setString(2, m.smap_name);
			pstmt.setString(3, m.list_title);
			pstmt.setInt(4, m.refresh_minutes > 0 ? m.refresh_minutes : 60);
			pstmt.setBoolean(5, m.enabled);
			pstmt.executeUpdate();
			ResultSet keys = pstmt.getGeneratedKeys();
			if(keys.next()) return keys.getInt(1);
		} finally {
			if(pstmt != null) try { pstmt.close(); } catch(SQLException e) {}
		}
		return -1;
	}

	public void updateMapping(Connection sd, SharePointListMap m) throws SQLException {
		String sql = "update sharepoint_list_map set smap_name = ?, list_title = ?, "
				+ "refresh_minutes = ?, enabled = ? where id = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, m.smap_name);
			pstmt.setString(2, m.list_title);
			pstmt.setInt(3, m.refresh_minutes > 0 ? m.refresh_minutes : 60);
			pstmt.setBoolean(4, m.enabled);
			pstmt.setInt(5, m.id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try { pstmt.close(); } catch(SQLException e) {}
		}
	}

	public void deleteMapping(Connection sd, int id) throws SQLException {
		String sql = "delete from sharepoint_list_map where id = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try { pstmt.close(); } catch(SQLException e) {}
		}
	}

	// -------------------------------------------------------------------------
	// Sync
	// -------------------------------------------------------------------------

	/*
	 * Sync all enabled mappings whose cache has expired.
	 * Safe to call repeatedly from the background batch job.
	 */
	public void syncDue(Connection sd, ServerData serverData, ResourceBundle localisation) {
		String sql = "select id, o_id, smap_name, list_title, refresh_minutes, "
				+ "coalesce(csv_table_id, 0) as csv_table_id "
				+ "from sharepoint_list_map "
				+ "where enabled = true "
				+ "and (last_sync is null "
				+ "  or last_sync < now() - (refresh_minutes || ' minutes')::interval)";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				SharePointListMap m = new SharePointListMap();
				m.id              = rs.getInt("id");
				m.o_id            = rs.getInt("o_id");
				m.smap_name       = rs.getString("smap_name");
				m.list_title      = rs.getString("list_title");
				m.refresh_minutes = rs.getInt("refresh_minutes");
				m.csv_table_id    = rs.getInt("csv_table_id");
				try {
					syncOne(sd, m, serverData, localisation);
				} catch(Exception e) {
					log.log(Level.SEVERE, "SharePoint sync failed for list '" + m.list_title + "': " + e.getMessage(), e);
				}
			}
		} catch(Exception e) {
			log.log(Level.SEVERE, "SharePoint syncDue error: " + e.getMessage(), e);
		} finally {
			if(pstmt != null) try { pstmt.close(); } catch(SQLException e) {}
		}
	}

	/*
	 * Sync a single mapping immediately (also used by the sync-now endpoint).
	 */
	public void syncOne(Connection sd, SharePointListMap m,
			ServerData serverData, ResourceBundle localisation) throws Exception {

		log.info("SharePoint sync: fetching list '" + m.list_title + "' for org " + m.o_id);

		List<Map<String, String>> rows = SharePointManager.getListItems(serverData, m.list_title, MAX_ROWS);

		String fileName = FILENAME_PREFIX + m.smap_name;

		// Get or create the csvtable entry
		CsvTableManager csvMgr = new CsvTableManager(sd, localisation, m.o_id, 0, fileName);
		csvMgr.updateTableFromRows(rows);

		// Update csv_table_id and last_sync
		int csvTableId = getCsvTableId(sd, m.o_id, fileName);
		updateSyncStatus(sd, m.id, csvTableId);

		log.info("SharePoint sync: wrote " + rows.size() + " rows to cache for '" + m.list_title + "'");
	}

	/*
	 * Look up the csv_table_id for a given smap_name within an org.
	 * Used by LookupManager to resolve the sharepointlist_ prefix.
	 */
	public static int getCsvTableId(Connection sd, int oId, String fileName) throws SQLException {
		String sql = "select csv_table_id from sharepoint_list_map "
				+ "where o_id = ? and smap_name = ? and enabled = true";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, fileName.startsWith(FILENAME_PREFIX)
					? fileName.substring(FILENAME_PREFIX.length()) : fileName);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) return rs.getInt(1);
		} finally {
			if(pstmt != null) try { pstmt.close(); } catch(SQLException e) {}
		}
		return -1;
	}

	// -------------------------------------------------------------------------
	// Private helpers
	// -------------------------------------------------------------------------

	private void updateSyncStatus(Connection sd, int id, int csvTableId) throws SQLException {
		String sql = "update sharepoint_list_map set last_sync = ?, csv_table_id = ? where id = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setTimestamp(1, Timestamp.from(Instant.now()));
			pstmt.setInt(2, csvTableId);
			pstmt.setInt(3, id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try { pstmt.close(); } catch(SQLException e) {}
		}
	}
}
