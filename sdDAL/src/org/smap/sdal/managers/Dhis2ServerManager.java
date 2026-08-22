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

import org.smap.sdal.model.Dhis2Server;

/*
 * DHIS2 connections held by an organisation
 *
 * The token is write only.  It is never returned by get(), because anything the API returns can
 * end up in a browser, a log or a support ticket.  Callers that need to make a request use
 * getWithToken(), which is package visible to the managers that talk to DHIS2
 */
public class Dhis2ServerManager {

	private static Logger log = Logger.getLogger(Dhis2ServerManager.class.getName());

	private static final String COLS = "id, o_id, label, base_url, api_version, "
			+ "to_char(last_tested, 'YYYY-MM-DD HH24:MI:SS') as last_tested, "
			+ "last_test_result, enabled, (api_token is not null and api_token != '') as token_set";

	/*
	 * All the connections held by an organisation, without their tokens
	 */
	public ArrayList<Dhis2Server> getServers(Connection sd, int oId) throws SQLException {

		ArrayList<Dhis2Server> servers = new ArrayList<>();
		String sql = "select " + COLS + " from dhis2_server where o_id = ? order by label asc";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				servers.add(fromResultSet(rs));
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return servers;
	}

	/*
	 * One connection, without its token
	 * Scoped by organisation so a request cannot reach another tenant's connection by guessing an id
	 */
	public Dhis2Server getServer(Connection sd, int oId, int id) throws SQLException {

		Dhis2Server server = null;
		String sql = "select " + COLS + " from dhis2_server where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, id);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				server = fromResultSet(rs);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return server;
	}

	/*
	 * One connection including its token, for making a request
	 * Never hand the result of this to anything that serialises to a client
	 */
	public Dhis2Server getServerWithToken(Connection sd, int oId, int id) throws SQLException {

		Dhis2Server server = getServer(sd, oId, id);
		if(server == null) {
			return null;
		}

		String sql = "select api_token from dhis2_server where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, id);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				server.api_token = rs.getString(1);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return server;
	}

	public int addServer(Connection sd, int oId, Dhis2Server server) throws SQLException {

		int id = -1;
		String sql = "insert into dhis2_server (o_id, label, base_url, api_token, api_version, enabled) "
				+ "values (?, ?, ?, ?, ?, ?)";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, oId);
			pstmt.setString(2, server.label);
			pstmt.setString(3, normaliseUrl(server.base_url));
			pstmt.setString(4, server.api_token);
			pstmt.setString(5, server.api_version);
			pstmt.setBoolean(6, server.enabled);
			log.info("Add DHIS2 connection: " + pstmt.toString());
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

	/*
	 * Update a connection
	 * An empty token means "leave the one you have".  The UI never receives the token, so it
	 * cannot send it back, and without this rule every edit would wipe it
	 */
	public void updateServer(Connection sd, int oId, Dhis2Server server) throws SQLException {

		boolean setToken = server.api_token != null && server.api_token.trim().length() > 0;

		StringBuilder sql = new StringBuilder("update dhis2_server set label = ?, base_url = ?, "
				+ "api_version = ?, enabled = ?");
		if(setToken) {
			sql.append(", api_token = ?");
		}
		sql.append(" where o_id = ? and id = ?");

		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			pstmt.setString(idx++, server.label);
			pstmt.setString(idx++, normaliseUrl(server.base_url));
			pstmt.setString(idx++, server.api_version);
			pstmt.setBoolean(idx++, server.enabled);
			if(setToken) {
				pstmt.setString(idx++, server.api_token.trim());
			}
			pstmt.setInt(idx++, oId);
			pstmt.setInt(idx++, server.id);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}
	}

	public void deleteServer(Connection sd, int oId, int id) throws SQLException {

		String sql = "delete from dhis2_server where o_id = ? and id = ?";
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
	 * Record the outcome of a connection test so it can be shown in the list
	 */
	public void recordTest(Connection sd, int oId, int id, String summary) throws SQLException {

		String sql = "update dhis2_server set last_tested = now(), last_test_result = ? "
				+ "where o_id = ? and id = ?";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, summary);
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

	private Dhis2Server fromResultSet(ResultSet rs) throws SQLException {
		Dhis2Server s = new Dhis2Server();
		s.id = rs.getInt("id");
		s.o_id = rs.getInt("o_id");
		s.label = rs.getString("label");
		s.base_url = rs.getString("base_url");
		s.api_version = rs.getString("api_version");
		s.last_tested = rs.getString("last_tested");
		s.last_test_result = rs.getString("last_test_result");
		s.enabled = rs.getBoolean("enabled");
		s.token_set = rs.getBoolean("token_set");
		return s;
	}

	/*
	 * Accept whatever the user typed.  A trailing slash or an included /api is the common mistake
	 * and there is no reason to make them fix it
	 */
	private String normaliseUrl(String url) {
		if(url == null) {
			return null;
		}
		String u = url.trim();
		while(u.endsWith("/")) {
			u = u.substring(0, u.length() - 1);
		}
		if(u.endsWith("/api")) {
			u = u.substring(0, u.length() - "/api".length());
		}
		return u;
	}
}
