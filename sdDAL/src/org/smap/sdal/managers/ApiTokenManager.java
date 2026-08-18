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

package org.smap.sdal.managers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.model.ApiToken;

/*
 * Tokens used to authenticate the API and fieldTask.
 *
 * The value is 32 bytes of SecureRandom, and only its sha256 is stored, so a database
 * dump does not yield working credentials.  A fast hash is the right one here - unlike a
 * password, the value has enough entropy that guessing it is not a threat, and hashing it
 * with a single round keeps the lookup one indexed equality rather than a scan with a
 * bcrypt comparison per row.
 *
 * The value is returned exactly once, by create().  There is deliberately no way to read
 * it back: a token that has been lost is replaced, not recovered.
 */
public class ApiTokenManager {

	private static Logger log = Logger.getLogger(ApiTokenManager.class.getName());

	public static final String SCOPE_API = "api";
	public static final String SCOPE_APP = "app";

	private static final int VALUE_BYTES = 32;
	private static final int PREFIX_LENGTH = 8;

	/*
	 * How stale last_used is allowed to get.  Without this every token authenticated
	 * request would also be a write, which on the device endpoints is most of the traffic.
	 */
	private static final long LAST_USED_RESOLUTION_MS = 5 * 60 * 1000L;

	private static final SecureRandom random = new SecureRandom();

	private final LogManager lm = new LogManager();

	/*
	 * What a token resolved to
	 */
	public static class Resolved {
		public final String ident;
		public final int tokenId;
		public final String scope;

		Resolved(String ident, int tokenId, String scope) {
			this.ident = ident;
			this.tokenId = tokenId;
			this.scope = scope;
		}
	}

	/*
	 * Identify the holder of a token.  Returns null for a value that is unknown, revoked
	 * or expired - the caller cannot tell which, and should not be able to.
	 */
	public Resolved resolve(Connection sd, String value) {

		if(value == null || value.length() == 0) {
			return null;
		}

		String sql = "select t.id, t.scope, u.ident "
				+ "from api_token t "
				+ "inner join users u on u.id = t.u_id "
				+ "where t.token_hash = ? "
				+ "and t.revoked is null "
				+ "and (t.expires is null or t.expires > now())";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, hash(value));
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return new Resolved(rs.getString("ident"), rs.getInt("id"), rs.getString("scope"));
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Resolving api token", e);
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}

		return null;
	}

	/*
	 * Record that a token was used.  Skipped when last_used is already recent, so this is
	 * not a write on every request.
	 */
	public void touch(Connection sd, int tokenId, String ip) {

		String sql = "update api_token set last_used = now(), last_used_ip = ? "
				+ "where id = ? "
				+ "and (last_used is null or last_used < now() - interval '"
				+ (LAST_USED_RESOLUTION_MS / 1000) + " seconds')";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ip);
			pstmt.setInt(2, tokenId);
			pstmt.executeUpdate();
		} catch (Exception e) {
			// Never fail a request because the usage stamp could not be written
			log.log(Level.WARNING, "Recording api token use", e);
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
	}

	/*
	 * Issue a token.  The returned ApiToken is the only time the value exists outside the
	 * caller's hands.
	 */
	public ApiToken create(Connection sd, String ident, String scope, String name,
			Timestamp expires, String createdBy, String serverName) throws SQLException, ApplicationException {

		checkScope(scope);

		String value = newValue(scope);

		String sql = "insert into api_token (u_id, scope, name, token_hash, prefix, created, created_by, expires) "
				+ "select id, ?, ?, ?, ?, now(), ?, ? from users where ident = ?";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, scope);
			pstmt.setString(2, name);
			pstmt.setString(3, hash(value));
			pstmt.setString(4, prefix(value));
			pstmt.setString(5, createdBy);
			pstmt.setTimestamp(6, expires);
			pstmt.setString(7, ident);

			if(pstmt.executeUpdate() == 0) {
				throw new ApplicationException("Unknown user: " + ident);
			}

			ApiToken token = new ApiToken();
			token.scope = scope;
			token.name = name;
			token.prefix = prefix(value);
			token.expires = expires;
			token.value = value;

			ResultSet rs = pstmt.getGeneratedKeys();
			if(rs.next()) {
				token.id = rs.getInt(1);
			}

			lm.writeLog(sd, 0, createdBy, LogManager.API_TOKEN,
					"Created " + scope + " token " + token.prefix + "... for " + ident, 0, serverName);

			return token;
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
	}

	/*
	 * A user's tokens, most recent first.  Revoked tokens stay in the list so that the
	 * history of what was issued is visible.
	 */
	public ArrayList<ApiToken> getTokens(Connection sd, String ident, String scope) throws SQLException {

		StringBuilder sql = new StringBuilder("select t.id, t.scope, t.name, t.prefix, t.created, "
				+ "t.created_by, t.expires, t.last_used, t.revoked, t.revoked_by "
				+ "from api_token t "
				+ "inner join users u on u.id = t.u_id "
				+ "where u.ident = ?");
		if(scope != null) {
			sql.append(" and t.scope = ?");
		}
		sql.append(" order by t.created desc");

		ArrayList<ApiToken> tokens = new ArrayList<>();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setString(1, ident);
			if(scope != null) {
				pstmt.setString(2, scope);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				ApiToken t = new ApiToken();
				t.id = rs.getInt("id");
				t.scope = rs.getString("scope");
				t.name = rs.getString("name");
				t.prefix = rs.getString("prefix");
				t.created = rs.getTimestamp("created");
				t.created_by = rs.getString("created_by");
				t.expires = rs.getTimestamp("expires");
				t.last_used = rs.getTimestamp("last_used");
				t.revoked = rs.getTimestamp("revoked");
				t.revoked_by = rs.getString("revoked_by");
				tokens.add(t);
			}
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}

		return tokens;
	}

	/*
	 * Revoke one token.  The ident is part of the where clause, so a caller cannot revoke
	 * a token belonging to somebody they were not authorised for by guessing its id.
	 */
	public boolean revoke(Connection sd, int tokenId, String ownerIdent, String revokedBy,
			String serverName) throws SQLException {

		String sql = "update api_token set revoked = now(), revoked_by = ? "
				+ "where id = ? "
				+ "and revoked is null "
				+ "and u_id = (select id from users where ident = ?)";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, revokedBy);
			pstmt.setInt(2, tokenId);
			pstmt.setString(3, ownerIdent);

			boolean revoked = pstmt.executeUpdate() > 0;
			if(revoked) {
				lm.writeLog(sd, 0, revokedBy, LogManager.API_TOKEN,
						"Revoked token " + tokenId + " belonging to " + ownerIdent, 0, serverName);
			}
			return revoked;
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
	}

	/*
	 * Revoke every token a user holds in one scope.  Used when an administrator resets
	 * somebody's device access.
	 */
	public int revokeAll(Connection sd, String ownerIdent, String scope, String revokedBy,
			String serverName) throws SQLException, ApplicationException {

		checkScope(scope);

		String sql = "update api_token set revoked = now(), revoked_by = ? "
				+ "where revoked is null "
				+ "and scope = ? "
				+ "and u_id = (select id from users where ident = ?)";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, revokedBy);
			pstmt.setString(2, scope);
			pstmt.setString(3, ownerIdent);

			int count = pstmt.executeUpdate();
			if(count > 0) {
				lm.writeLog(sd, 0, revokedBy, LogManager.API_TOKEN,
						"Revoked " + count + " " + scope + " tokens belonging to " + ownerIdent, 0, serverName);
			}
			return count;
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
	}

	/*
	 * smap_a_ or smap_p_ then 32 random bytes.  The prefix makes a leaked token
	 * recognisable for what it is, which is what lets secret scanners find one.
	 */
	private String newValue(String scope) {
		byte[] bytes = new byte[VALUE_BYTES];
		random.nextBytes(bytes);
		return "smap_" + (SCOPE_API.equals(scope) ? "a" : "p") + "_"
				+ Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
	}

	private String prefix(String value) {
		return value.length() <= PREFIX_LENGTH ? value : value.substring(0, PREFIX_LENGTH);
	}

	public static String hash(String value) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
			StringBuilder hex = new StringBuilder(bytes.length * 2);
			for(byte b : bytes) {
				hex.append(Character.forDigit((b >> 4) & 0xf, 16));
				hex.append(Character.forDigit(b & 0xf, 16));
			}
			return hex.toString();
		} catch (Exception e) {
			// SHA-256 is required of every JVM, so this cannot happen
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
	}

	private void checkScope(String scope) throws ApplicationException {
		if(!SCOPE_API.equals(scope) && !SCOPE_APP.equals(scope)) {
			throw new ApplicationException("Unknown token scope: " + scope);
		}
	}
}
