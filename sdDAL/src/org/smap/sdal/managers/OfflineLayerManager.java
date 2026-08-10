package org.smap.sdal.managers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.model.OfflineLayer;

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
 * Manage offline map layers.  These are large mbtiles files that are uploaded on the
 * shared resources page, assigned to projects, and downloaded by FieldTask.  A user gets
 * the layers belonging to the projects they have access to.
 */
public class OfflineLayerManager {

	private static Logger log =
			 Logger.getLogger(OfflineLayerManager.class.getName());

	// Offline layers are much larger than other shared resources
	public static long MAX_LAYER_SIZE = 524288000;		// 500 MB

	/*
	 * Get the layers for an organisation.  Includes the assigned projects so that the
	 * shared resources page can show and edit them.
	 */
	public ArrayList<OfflineLayer> getLayers(Connection sd, int oId) throws SQLException {

		ArrayList<OfflineLayer> layers = new ArrayList<OfflineLayer> ();

		String sql = "select id, name, file_name, file_size, md5, version, description, "
				+ "changed_by, changed_ts "
				+ "from offline_layer "
				+ "where o_id = ? "
				+ "order by name asc";

		String sqlProjects = "select p.id from project p, offline_layer_project olp "
				+ "where p.id = olp.p_id "
				+ "and olp.layer_id = ? "
				+ "order by p.name asc";

		// How many devices have reported holding the current version of the layer
		String sqlDevices = "select count(*) from offline_layer_device old, offline_layer ol "
				+ "where old.layer_id = ol.id "
				+ "and old.layer_id = ? "
				+ "and old.layer_version = ol.version";

		PreparedStatement pstmt = null;
		PreparedStatement pstmtProjects = null;
		PreparedStatement pstmtDevices = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();

			pstmtProjects = sd.prepareStatement(sqlProjects);
			pstmtDevices = sd.prepareStatement(sqlDevices);
			while(rs.next()) {
				OfflineLayer ol = new OfflineLayer();
				ol.id = rs.getInt("id");
				ol.name = rs.getString("name");
				ol.fileName = rs.getString("file_name");
				ol.size = rs.getLong("file_size");
				ol.md5 = rs.getString("md5");
				ol.version = rs.getInt("version");
				ol.description = rs.getString("description");
				ol.changedBy = rs.getString("changed_by");
				Timestamp ts = rs.getTimestamp("changed_ts");
				ol.changedTs = ts == null ? null : ts.toString();

				ol.projects = new ArrayList<Integer> ();
				pstmtProjects.setInt(1, ol.id);
				ResultSet rsProjects = pstmtProjects.executeQuery();
				while(rsProjects.next()) {
					ol.projects.add(rsProjects.getInt(1));
				}

				pstmtDevices.setInt(1, ol.id);
				ResultSet rsDevices = pstmtDevices.executeQuery();
				if(rsDevices.next()) {
					ol.devices = rsDevices.getInt(1);
				}

				layers.add(ol);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtProjects != null) {pstmtProjects.close();}} catch (SQLException e) {}
			try {if (pstmtDevices != null) {pstmtDevices.close();}} catch (SQLException e) {}
		}

		return layers;
	}

	/*
	 * Get the layers assigned to a user.  Called when a device refreshes.
	 */
	public ArrayList<OfflineLayer> getLayersForUser(Connection sd, String userIdent, String urlPrefix)
			throws SQLException {

		ArrayList<OfflineLayer> layers = new ArrayList<OfflineLayer> ();

		// A layer reaches a user through the projects they have access to
		String sql = "select distinct ol.id, ol.name, ol.file_name, ol.file_size, ol.md5, ol.version "
				+ "from offline_layer ol, offline_layer_project olp, user_project up, users u "
				+ "where ol.id = olp.layer_id "
				+ "and olp.p_id = up.p_id "
				+ "and up.u_id = u.id "
				+ "and u.ident = ? "
				+ "order by ol.name asc";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, userIdent);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				OfflineLayer ol = new OfflineLayer();
				ol.id = rs.getInt("id");
				ol.name = rs.getString("name");
				ol.fileName = rs.getString("file_name");
				ol.size = rs.getLong("file_size");
				ol.md5 = rs.getString("md5");
				ol.version = rs.getInt("version");
				// Same shape as the media urls built by MediaInfo for devices
				ol.url = urlPrefix + "/resource/"
						+ org.smap.sdal.Utilities.GeneralUtilityMethods.urlEncode(ol.fileName)
						+ "/layer/" + ol.id;
				layers.add(ol);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		return layers;
	}

	/*
	 * Check that a user has been assigned a layer and return its path.
	 * Returns null if the user has no access, so the caller can respond with not found.
	 */
	public String getLayerPathForUser(Connection sd, int layerId, String userIdent) throws SQLException {

		String path = null;

		// The layer must be assigned to a project that the user has access to
		String sql = "select distinct ol.file_path "
				+ "from offline_layer ol, offline_layer_project olp, user_project up, users u "
				+ "where ol.id = olp.layer_id "
				+ "and olp.p_id = up.p_id "
				+ "and up.u_id = u.id "
				+ "and ol.id = ? "
				+ "and u.ident = ?";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, layerId);
			pstmt.setString(2, userIdent);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				path = rs.getString(1);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		return path;
	}

	/*
	 * Get the checksum of a layer.  Used as the ETag when the file is downloaded.
	 */
	public String getMd5(Connection sd, int layerId) throws SQLException {

		String md5 = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select md5 from offline_layer where id = ?");
			pstmt.setInt(1, layerId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				md5 = rs.getString(1);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return md5;
	}

	/*
	 * Write an uploaded layer to disk and record it.  The file is streamed rather than held
	 * in memory as it can be hundreds of megabytes.  The checksum is calculated while writing
	 * so the file is only read once.
	 */
	public OfflineLayer save(Connection sd,
			int oId,
			String user,
			int id,					// 0 for a new layer
			String name,
			String description,
			String fileName,
			InputStream is,			// Null if only the details are being changed
			String basePath) throws SQLException, ApplicationException {

		OfflineLayer ol = new OfflineLayer();
		ol.id = id;
		ol.name = name;
		ol.description = description;

		if(is != null) {
			File dir = new File(basePath + "/media/organisation/" + oId + "/layers");
			if(!dir.exists()) {
				dir.mkdirs();
			}

			File target = new File(dir, fileName);
			MessageDigest md = null;
			try {
				md = MessageDigest.getInstance("MD5");
			} catch (Exception e) {
				throw new ApplicationException("MD5 not available: " + e.getMessage());
			}

			long size = 0;
			try (OutputStream os = new FileOutputStream(target)) {
				byte[] buffer = new byte[65536];
				int bytes;
				while((bytes = is.read(buffer)) != -1) {
					os.write(buffer, 0, bytes);
					md.update(buffer, 0, bytes);
					size += bytes;
					if(size > MAX_LAYER_SIZE) {
						os.close();
						target.delete();
						throw new ApplicationException("Layer exceeds the maximum size of "
								+ String.format("%,d", MAX_LAYER_SIZE) + " bytes");
					}
				}
			} catch (ApplicationException e) {
				throw e;
			} catch (Exception e) {
				target.delete();
				throw new ApplicationException("Failed to save layer: " + e.getMessage());
			}

			StringBuilder sb = new StringBuilder();
			for(byte b : md.digest()) {
				sb.append(String.format("%02x", b));
			}

			ol.fileName = fileName;
			ol.size = size;
			ol.md5 = sb.toString();
			ol.url = target.getAbsolutePath();		// Temporarily hold the path for the insert below
		}

		PreparedStatement pstmt = null;
		try {
			if(id <= 0) {
				String sql = "insert into offline_layer "
						+ "(o_id, name, file_name, file_path, file_size, md5, version, description, changed_by, changed_ts) "
						+ "values (?, ?, ?, ?, ?, ?, 1, ?, ?, now())";
				pstmt = sd.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
				pstmt.setInt(1, oId);
				pstmt.setString(2, ol.name);
				pstmt.setString(3, ol.fileName);
				pstmt.setString(4, ol.url);
				pstmt.setLong(5, ol.size);
				pstmt.setString(6, ol.md5);
				pstmt.setString(7, ol.description);
				pstmt.setString(8, user);
				log.info("Insert offline layer: " + pstmt.toString());
				pstmt.executeUpdate();

				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					ol.id = rs.getInt(1);
				}
			} else if(is != null) {
				// Replacing the file, bump the version so devices re-download
				String sql = "update offline_layer set "
						+ "name = ?, description = ?, file_name = ?, file_path = ?, file_size = ?, "
						+ "md5 = ?, version = version + 1, changed_by = ?, changed_ts = now() "
						+ "where id = ? and o_id = ?";
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, ol.name);
				pstmt.setString(2, ol.description);
				pstmt.setString(3, ol.fileName);
				pstmt.setString(4, ol.url);
				pstmt.setLong(5, ol.size);
				pstmt.setString(6, ol.md5);
				pstmt.setString(7, user);
				pstmt.setInt(8, id);
				pstmt.setInt(9, oId);
				log.info("Replace offline layer: " + pstmt.toString());
				pstmt.executeUpdate();
			} else {
				// Details only, the file is unchanged so the version stays as it is
				String sql = "update offline_layer set "
						+ "name = ?, description = ?, changed_by = ?, changed_ts = now() "
						+ "where id = ? and o_id = ?";
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, ol.name);
				pstmt.setString(2, ol.description);
				pstmt.setString(3, user);
				pstmt.setInt(4, id);
				pstmt.setInt(5, oId);
				log.info("Update offline layer: " + pstmt.toString());
				pstmt.executeUpdate();
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		ol.url = null;		// Only used to carry the path into the insert
		return ol;
	}

	/*
	 * Set the projects that a layer is assigned to.  Everybody with access to one of these
	 * projects gets the layer.
	 */
	public void setProjects(Connection sd, int oId, int layerId, ArrayList<Integer> projects) throws SQLException {

		String sqlCheck = "select count(*) from offline_layer where id = ? and o_id = ?";
		String sqlDelete = "delete from offline_layer_project where layer_id = ?";
		// The project must belong to the same organisation as the layer
		String sqlInsert = "insert into offline_layer_project (layer_id, p_id) "
				+ "select ?, p.id from project p where p.id = ? and p.o_id = ?";

		PreparedStatement pstmtCheck = null;
		PreparedStatement pstmtDelete = null;
		PreparedStatement pstmtInsert = null;
		try {
			pstmtCheck = sd.prepareStatement(sqlCheck);
			pstmtCheck.setInt(1, layerId);
			pstmtCheck.setInt(2, oId);
			ResultSet rs = pstmtCheck.executeQuery();
			if(!rs.next() || rs.getInt(1) == 0) {
				return;
			}

			pstmtDelete = sd.prepareStatement(sqlDelete);
			pstmtDelete.setInt(1, layerId);
			pstmtDelete.executeUpdate();

			if(projects != null) {
				pstmtInsert = sd.prepareStatement(sqlInsert);
				for(Integer pId : projects) {
					pstmtInsert.setInt(1, layerId);
					pstmtInsert.setInt(2, pId);
					pstmtInsert.setInt(3, oId);
					pstmtInsert.executeUpdate();
				}
			}
		} finally {
			try {if (pstmtCheck != null) {pstmtCheck.close();}} catch (SQLException e) {}
			try {if (pstmtDelete != null) {pstmtDelete.close();}} catch (SQLException e) {}
			try {if (pstmtInsert != null) {pstmtInsert.close();}} catch (SQLException e) {}
		}
	}

	/*
	 * Record that a device holds a layer.  Called when a device reports its state on refresh.
	 * Layers that the device no longer holds are removed from the report.
	 */
	public void setDownloadedLayers(Connection sd, String userIdent, String deviceId,
			ArrayList<Integer> layerIds) throws SQLException {

		/*
		 * A user can have several devices and each is tracked separately.  Without a device id
		 * the reports from those devices would share a key and overwrite each other, so ignore
		 * the report rather than record it against the wrong device.
		 */
		if(deviceId == null || deviceId.trim().length() == 0) {
			log.info("Ignoring offline layer report from " + userIdent + " as it has no device id");
			return;
		}

		String sqlDelete = "delete from offline_layer_device "
				+ "where device_id = ? "
				+ "and u_id in (select id from users where ident = ?)";
		// A repeated layer id in the report must not fail the whole assignment update
		String sqlInsert = "insert into offline_layer_device "
				+ "(layer_id, u_id, device_id, layer_version, downloaded_ts) "
				+ "select ol.id, u.id, ?, ol.version, now() "
				+ "from offline_layer ol, users u "
				+ "where ol.id = ? and u.ident = ? and ol.o_id = u.o_id "
				+ "on conflict do nothing";

		PreparedStatement pstmtDelete = null;
		PreparedStatement pstmtInsert = null;
		try {
			pstmtDelete = sd.prepareStatement(sqlDelete);
			pstmtDelete.setString(1, deviceId);
			pstmtDelete.setString(2, userIdent);
			pstmtDelete.executeUpdate();

			if(layerIds != null) {
				pstmtInsert = sd.prepareStatement(sqlInsert);
				for(Integer layerId : layerIds) {
					pstmtInsert.setString(1, deviceId);
					pstmtInsert.setInt(2, layerId);
					pstmtInsert.setString(3, userIdent);
					pstmtInsert.executeUpdate();
				}
			}
		} finally {
			try {if (pstmtDelete != null) {pstmtDelete.close();}} catch (SQLException e) {}
			try {if (pstmtInsert != null) {pstmtInsert.close();}} catch (SQLException e) {}
		}
	}

	/*
	 * Delete a layer and its file
	 */
	public void delete(Connection sd, int oId, int layerId) throws SQLException {

		String filePath = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select file_path from offline_layer where id = ? and o_id = ?");
			pstmt.setInt(1, layerId);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				filePath = rs.getString(1);
			}
			pstmt.close();

			// Assignments and device records are removed by the foreign key cascades
			pstmt = sd.prepareStatement("delete from offline_layer where id = ? and o_id = ?");
			pstmt.setInt(1, layerId);
			pstmt.setInt(2, oId);
			log.info("Delete offline layer: " + pstmt.toString());
			pstmt.executeUpdate();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		if(filePath != null) {
			try {
				File f = new File(filePath);
				if(f.exists()) {
					f.delete();
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Failed to delete layer file: " + filePath, e);
			}
		}
	}

	/*
	 * Return true if offline maps are managed on the server for this organisation
	 */
	public boolean isEnabled(Connection sd, int oId) throws SQLException {

		boolean enabled = false;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select ft_offline_maps from organisation where id = ?");
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				enabled = rs.getBoolean(1);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return enabled;
	}
}
