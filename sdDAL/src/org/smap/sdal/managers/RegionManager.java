package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.model.RegionGrid;

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

/*
 * Manage the hexagonal grids that map panels aggregate data over.  A grid belongs to a
 * survey bundle and is held in a table r_{organisation id}_{region id} in the results database.
 * The table name is only ever built from those two numbers.
 */
public class RegionManager {

	private static Logger log =
			 Logger.getLogger(RegionManager.class.getName());

	public static int MAX_CELLS = 20000;
	public static int MIN_CELL_SIZE = 10;			// metres

	private ResourceBundle localisation;

	public RegionManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * The bundle a survey's grids belong to, a survey that is not in a bundle is a bundle of its own
	 */
	public static String getBundleIdent(Connection sd, int sId) throws SQLException {
		String ident = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select coalesce(group_survey_ident, ident) from survey where s_id = ?");
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				ident = rs.getString(1);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return ident;
	}

	public static boolean isValidRegionTable(String tableName) {
		return tableName != null && tableName.matches("r_[0-9]+_[0-9]+");
	}

	/*
	 * Get the grids of a survey bundle
	 */
	public ArrayList<RegionGrid> getGrids(Connection sd, int oId, String groupIdent) throws SQLException {

		ArrayList<RegionGrid> grids = new ArrayList<> ();

		String sql = "select id, region_name, cell_size, created_by from regions "
				+ "where o_id = ? "
				+ "and group_survey_ident = ? "
				+ "order by region_name";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, groupIdent);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				RegionGrid g = new RegionGrid();
				g.id = rs.getInt(1);
				g.name = rs.getString(2);
				g.cell_size = rs.getInt(3);
				g.created_by = rs.getString(4);
				grids.add(g);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return grids;
	}

	/*
	 * Get the table holding a grid, null if the grid is not in this organisation and bundle
	 */
	public String getGridTable(Connection sd, int oId, String groupIdent, int id) throws SQLException {

		String tableName = null;
		String sql = "select table_name from regions "
				+ "where id = ? "
				+ "and o_id = ? "
				+ "and group_survey_ident = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.setInt(2, oId);
			pstmt.setString(3, groupIdent);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				tableName = rs.getString(1);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return isValidRegionTable(tableName) ? tableName : null;
	}

	/*
	 * Create a grid of hexagons covering the extent, which is in WGS84
	 * The whole extent is covered so that the grid suits data that has not been submitted yet
	 */
	public int createGrid(Connection sd, Connection cResults, int oId, String groupIdent, String name,
			int cellSize, double [] extent, String user) throws Exception {

		if(name == null || name.trim().length() == 0) {
			throw new ApplicationException(localisation.getString("rg_name"));
		}
		if(cellSize < MIN_CELL_SIZE) {
			throw new ApplicationException(localisation.getString("rg_size").replace("%s1", String.valueOf(MIN_CELL_SIZE)));
		}
		if(extent == null || extent.length != 4 || extent[0] >= extent[2] || extent[1] >= extent[3]
				|| extent[0] < -180 || extent[2] > 180 || extent[1] < -85 || extent[3] > 85) {
			throw new ApplicationException(localisation.getString("rg_extent"));
		}

		String sqlGrid = "ST_HexagonGrid(?, ST_Transform(ST_MakeEnvelope(?, ?, ?, ?, 4326), 3857))";
		PreparedStatement pstmt = null;
		boolean sdAutoCommit = sd.getAutoCommit();
		boolean resultsAutoCommit = cResults.getAutoCommit();
		try {
			/*
			 * Check the size of the grid before creating anything.  The number of cells is estimated from
			 * the areas, generating the cells to count them can need more temporary disk than the server has
			 */
			pstmt = cResults.prepareStatement("select ST_XMax(e) - ST_XMin(e), ST_YMax(e) - ST_YMin(e) "
					+ "from (select ST_Transform(ST_MakeEnvelope(?, ?, ?, ?, 4326), 3857) as e) as env");
			pstmt.setDouble(1, extent[0]);
			pstmt.setDouble(2, extent[1]);
			pstmt.setDouble(3, extent[2]);
			pstmt.setDouble(4, extent[3]);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			double edge = gridCellSize(cellSize, extent);
			double hexArea = 1.5 * Math.sqrt(3) * edge * edge;
			long count = Math.round(rs.getDouble(1) * rs.getDouble(2) / hexArea);
			if(count > MAX_CELLS) {
				throw new ApplicationException(localisation.getString("rg_many")
						.replace("%s1", String.valueOf(count))
						.replace("%s2", String.valueOf(MAX_CELLS)));
			}
			pstmt.close();

			sd.setAutoCommit(false);
			cResults.setAutoCommit(false);

			pstmt = sd.prepareStatement("insert into regions (o_id, group_survey_ident, region_name, cell_size, created_by, created) "
					+ "values (?, ?, ?, ?, ?, now()) returning id");
			pstmt.setInt(1, oId);
			pstmt.setString(2, groupIdent);
			pstmt.setString(3, name.trim());
			pstmt.setInt(4, cellSize);
			pstmt.setString(5, user);
			rs = pstmt.executeQuery();
			rs.next();
			int id = rs.getInt(1);
			pstmt.close();

			String tableName = "r_" + oId + "_" + id;
			pstmt = sd.prepareStatement("update regions set table_name = ?, geometry_column = 'the_geom' where id = ?");
			pstmt.setString(1, tableName);
			pstmt.setInt(2, id);
			pstmt.executeUpdate();
			pstmt.close();

			pstmt = cResults.prepareStatement("create table " + tableName
					+ " (gid serial primary key, name text, the_geom geometry(Polygon, 4326))");
			pstmt.executeUpdate();
			pstmt.close();

			pstmt = cResults.prepareStatement("insert into " + tableName + " (name, the_geom) "
					+ "select (row_number() over (order by i, j))::text, ST_Transform(geom, 4326) from " + sqlGrid);
			setGridParams(pstmt, 1, cellSize, extent);
			pstmt.executeUpdate();
			pstmt.close();

			pstmt = cResults.prepareStatement("create index " + tableName + "_geom on " + tableName + " using gist(the_geom)");
			pstmt.executeUpdate();

			cResults.commit();
			sd.commit();
			log.info("Created grid " + tableName + " of about " + count + " cells for " + groupIdent);
			return id;

		} catch (Exception e) {
			try {sd.rollback();} catch (Exception ex) {}
			try {cResults.rollback();} catch (Exception ex) {}
			throw e;
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {sd.setAutoCommit(sdAutoCommit);} catch (SQLException e) {}
			try {cResults.setAutoCommit(resultsAutoCommit);} catch (SQLException e) {}
		}
	}

	/*
	 * Delete a grid of a survey bundle
	 */
	public boolean deleteGrid(Connection sd, Connection cResults, int oId, String groupIdent, int id) throws SQLException {
		String tableName = getGridTable(sd, oId, groupIdent, id);
		if(tableName == null) {
			return false;
		}
		dropGrid(sd, cResults, id, tableName);
		return true;
	}

	/*
	 * Delete all the grids of a survey bundle, called when its last survey is erased
	 */
	public void deleteBundleRegions(Connection sd, Connection cResults, String groupIdent) throws SQLException {

		ArrayList<Integer> ids = new ArrayList<> ();
		ArrayList<String> tables = new ArrayList<> ();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select id, table_name from regions where group_survey_ident = ?");
			pstmt.setString(1, groupIdent);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				ids.add(rs.getInt(1));
				tables.add(rs.getString(2));
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		for(int i = 0; i < ids.size(); i++) {
			dropGrid(sd, cResults, ids.get(i), tables.get(i));
		}
	}

	private void dropGrid(Connection sd, Connection cResults, int id, String tableName) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			if(isValidRegionTable(tableName)) {
				pstmt = cResults.prepareStatement("drop table if exists " + tableName);
				pstmt.executeUpdate();
				pstmt.close();
			}
			pstmt = sd.prepareStatement("delete from regions where id = ?");
			pstmt.setInt(1, id);
			pstmt.executeUpdate();
			log.info("Deleted grid " + id + " " + tableName);
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}

	/*
	 * The grid is made in web mercator whose units stretch away from the equator, scale the
	 * cell size at the centre of the extent so the cells there are the requested size on the ground
	 */
	private double gridCellSize(int cellSize, double [] extent) {
		return cellSize / Math.cos(Math.toRadians((extent[1] + extent[3]) / 2));
	}

	private void setGridParams(PreparedStatement pstmt, int idx, int cellSize, double [] extent) throws SQLException {
		pstmt.setDouble(idx++, gridCellSize(cellSize, extent));
		pstmt.setDouble(idx++, extent[0]);
		pstmt.setDouble(idx++, extent[1]);
		pstmt.setDouble(idx++, extent[2]);
		pstmt.setDouble(idx++, extent[3]);
	}
}
