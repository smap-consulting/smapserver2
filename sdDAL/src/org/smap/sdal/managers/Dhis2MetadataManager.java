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
import java.util.logging.Logger;

import org.smap.sdal.model.Dhis2Object;
import org.smap.sdal.model.Dhis2Server;

import com.google.gson.JsonObject;

/*
 * DHIS2 metadata cached for the mapping screens
 *
 * This is configuration time data, read by someone building a mapping, not by a form and not by
 * a device.  So it is refreshed when asked for rather than on a timer: a schedule would pull
 * thousands of rows nobody is looking at.
 *
 * Two levels, because they cost very different amounts to fetch:
 *
 *  - the summary list of data sets or programs, which is small and populates a picker
 *  - the detail of one of them, which carries every data element and category option combo and
 *    is only worth fetching for the one being mapped
 *
 * The detail is stored as the JSON DHIS2 returned. Modelling DHIS2's structure again in tables
 * here would buy nothing: nothing queries inside it, the mapping screen reads it whole
 */
public class Dhis2MetadataManager {

	private static Logger log = Logger.getLogger(Dhis2MetadataManager.class.getName());

	/*
	 * Refresh the summary list of one object type from DHIS2
	 * Returns the number of objects found
	 */
	public int refreshList(Connection sd, int oId, String objectType) throws Exception {

		Dhis2Server server = new Dhis2ServerManager().getWithToken(sd, oId);
		if(server == null) {
			throw new Exception("No DHIS2 connection has been set up for this organisation");
		}

		Dhis2Manager dhis2 = new Dhis2Manager();
		List<Map<String, String>> items;

		if(Dhis2Object.TYPE_DATASET.equals(objectType)) {
			items = dhis2.listDataSets(server);
		} else if(Dhis2Object.TYPE_PROGRAM.equals(objectType)) {
			items = dhis2.listPrograms(server);
		} else {
			throw new Exception("Unknown DHIS2 metadata type: " + objectType);
		}

		/*
		 * Upsert rather than replace, so that detail already fetched for an object survives a
		 * list refresh.  Objects that have gone from DHIS2 are removed afterwards
		 */
		String sqlUpsert = "insert into dhis2_metadata (o_id, object_type, uid, code, name, last_listed) "
				+ "values (?, ?, ?, ?, ?, now()) "
				+ "on conflict (o_id, object_type, uid) do update "
				+ "set code = excluded.code, name = excluded.name, last_listed = now()";

		String sqlPurge = "delete from dhis2_metadata "
				+ "where o_id = ? and object_type = ? "
				+ "and (last_listed is null or last_listed < ?)";

		PreparedStatement pstmtUpsert = null;
		PreparedStatement pstmtPurge = null;

		try {
			java.sql.Timestamp startedAt = new java.sql.Timestamp(System.currentTimeMillis());

			pstmtUpsert = sd.prepareStatement(sqlUpsert);
			for(Map<String, String> item : items) {
				pstmtUpsert.setInt(1, oId);
				pstmtUpsert.setString(2, objectType);
				pstmtUpsert.setString(3, item.get("uid"));
				pstmtUpsert.setString(4, item.get("code"));
				pstmtUpsert.setString(5, item.get("name"));
				pstmtUpsert.executeUpdate();
			}

			// Anything not seen in this pass no longer exists in DHIS2
			pstmtPurge = sd.prepareStatement(sqlPurge);
			pstmtPurge.setInt(1, oId);
			pstmtPurge.setString(2, objectType);
			pstmtPurge.setTimestamp(3, startedAt);
			pstmtPurge.executeUpdate();

		} finally {
			if(pstmtUpsert != null) {try{pstmtUpsert.close();} catch(SQLException e) {}}
			if(pstmtPurge != null) {try{pstmtPurge.close();} catch(SQLException e) {}}
		}

		log.info("DHIS2 metadata: listed " + items.size() + " " + objectType
				+ " for organisation " + oId);
		return items.size();
	}

	/*
	 * The cached summary list, for a picker
	 */
	public ArrayList<Dhis2Object> getList(Connection sd, int oId, String objectType) throws SQLException {

		ArrayList<Dhis2Object> items = new ArrayList<>();
		String sql = "select uid, code, name, "
				+ "to_char(last_fetched, 'YYYY-MM-DD HH24:MI:SS') as last_fetched "
				+ "from dhis2_metadata where o_id = ? and object_type = ? order by name asc";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, objectType);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				Dhis2Object m = new Dhis2Object();
				m.object_type = objectType;
				m.uid = rs.getString("uid");
				m.code = rs.getString("code");
				m.name = rs.getString("name");
				m.last_fetched = rs.getString("last_fetched");
				items.add(m);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return items;
	}

	/*
	 * The detail of one object, fetching it from DHIS2 if it has not been fetched before or a
	 * refresh has been asked for
	 *
	 * Returns the payload as DHIS2 gave it, so the caller sees DHIS2's own structure rather than
	 * a translation of it that would have to be kept in step
	 */
	public String getDetail(Connection sd, int oId, String objectType, String uid, boolean refresh)
			throws Exception {

		if(!refresh) {
			String cached = readPayload(sd, oId, objectType, uid);
			if(cached != null) {
				return cached;
			}
		}

		Dhis2Server server = new Dhis2ServerManager().getWithToken(sd, oId);
		if(server == null) {
			throw new Exception("No DHIS2 connection has been set up for this organisation");
		}

		Dhis2Manager dhis2 = new Dhis2Manager();
		JsonObject detail;

		if(Dhis2Object.TYPE_DATASET.equals(objectType)) {
			detail = dhis2.getDataSet(server, uid);
		} else if(Dhis2Object.TYPE_PROGRAM.equals(objectType)) {
			detail = dhis2.getProgram(server, uid);
		} else {
			throw new Exception("Unknown DHIS2 metadata type: " + objectType);
		}

		String payload = detail.toString();
		writePayload(sd, oId, objectType, uid, payload);

		return payload;
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private String readPayload(Connection sd, int oId, String objectType, String uid) throws SQLException {

		String payload = null;
		String sql = "select payload from dhis2_metadata "
				+ "where o_id = ? and object_type = ? and uid = ? and payload is not null";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, objectType);
			pstmt.setString(3, uid);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				payload = rs.getString(1);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return payload;
	}

	/*
	 * The row may not exist yet if the detail was asked for before a list refresh, so this
	 * inserts as well as updates
	 */
	private void writePayload(Connection sd, int oId, String objectType, String uid, String payload)
			throws SQLException {

		String sql = "insert into dhis2_metadata (o_id, object_type, uid, payload, last_fetched) "
				+ "values (?, ?, ?, ?::jsonb, now()) "
				+ "on conflict (o_id, object_type, uid) do update "
				+ "set payload = excluded.payload, last_fetched = now()";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, objectType);
			pstmt.setString(3, uid);
			pstmt.setString(4, payload);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}
}
