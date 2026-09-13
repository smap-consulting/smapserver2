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

import java.security.SecureRandom;
import java.sql.Connection;
import java.util.ArrayList;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Access and refresh tokens for MCP clients.
 *
 * Opaque values, 32 bytes of SecureRandom, stored only as a sha256 so a database dump yields no
 * working credentials.  Not JWTs: this server is both the authorization server and the only
 * resource server, so validating a token is one indexed lookup and there are no keys to publish,
 * rotate or get wrong.  It also means revocation is immediate rather than waiting out a token
 * lifetime, which matters because the MCP switch is meant to work as a kill switch.
 *
 * Refresh tokens rotate.  Using one revokes it and issues a replacement, and presenting one that
 * has already been used revokes the whole chain, on the assumption that a token being replayed has
 * been copied.  parent_id holds the id of the first token in the chain, not the immediate
 * predecessor, so revoking the chain is one statement.
 */
public class OAuthTokenManager {

	private static Logger log = Logger.getLogger(OAuthTokenManager.class.getName());

	public static final String TYPE_ACCESS = "access";
	public static final String TYPE_REFRESH = "refresh";

	private static final int VALUE_BYTES = 32;
	private static final SecureRandom random = new SecureRandom();

	/*
	 * How stale last_used may get before it is written again.  Without this every MCP call would
	 * also be a write.
	 */
	private static final long LAST_USED_RESOLUTION_SECONDS = 300;

	/*
	 * What a token resolved to.  Everything the dispatcher needs to decide what the caller may do.
	 */
	public static class Resolved {
		public final int tokenId;
		public final String ident;
		public final int uId;
		public final int oId;
		public final String scope;
		public final String resource;
		public final String clientId;

		Resolved(int tokenId, String ident, int uId, int oId, String scope, String resource, String clientId) {
			this.tokenId = tokenId;
			this.ident = ident;
			this.uId = uId;
			this.oId = oId;
			this.scope = scope;
			this.resource = resource;
			this.clientId = clientId;
		}
	}

	/*
	 * A newly issued pair.  The values exist here and nowhere else.
	 */
	public static class Issued {
		public final String accessToken;
		public final String refreshToken;
		public final int expiresIn;
		public final String scope;

		Issued(String accessToken, String refreshToken, int expiresIn, String scope) {
			this.accessToken = accessToken;
			this.refreshToken = refreshToken;
			this.expiresIn = expiresIn;
			this.scope = scope;
		}
	}

	/*
	 * Identify the holder of a bearer token.
	 *
	 * Returns null for anything unknown, expired or revoked; the caller cannot tell which and
	 * should not be able to.  The checks that follow in the caller - that MCP is still switched on,
	 * that the user still holds the mcp access group, and that they have not moved organisation -
	 * are what make switching MCP off, or removing the group, take effect immediately rather than
	 * when the token would have expired.
	 */
	public Resolved resolve(Connection sd, String value) {

		if(value == null || value.trim().length() == 0) {
			return null;
		}

		String sql = "select t.id, t.u_id, t.o_id, t.scope, t.resource, t.client_id, u.ident "
				+ "from oauth_token t "
				+ "inner join users u on u.id = t.u_id "
				+ "where t.token_hash = ? "
				+ "and t.type = 'access' "
				+ "and t.revoked is null "
				+ "and (t.expires is null or t.expires > now())";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ApiTokenManager.hash(value.trim()));
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return new Resolved(rs.getInt("id"), rs.getString("ident"), rs.getInt("u_id"),
						rs.getInt("o_id"), rs.getString("scope"), rs.getString("resource"),
						rs.getString("client_id"));
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Resolving oauth token", e);
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}

		return null;
	}

	public void touch(Connection sd, int tokenId, String ip) {

		String sql = "update oauth_token set last_used = now(), last_used_ip = ? "
				+ "where id = ? "
				+ "and (last_used is null or last_used < now() - interval '"
				+ LAST_USED_RESOLUTION_SECONDS + " seconds')";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ip);
			pstmt.setInt(2, tokenId);
			pstmt.executeUpdate();
		} catch (Exception e) {
			// Never fail a request because the usage stamp could not be written
			log.log(Level.WARNING, "Recording oauth token use", e);
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
	}

	/*
	 * Issue an access token and a refresh token.  chainRoot is null for a first issue and the root
	 * of the existing chain when rotating.
	 */
	public Issued issue(Connection sd, String clientId, int uId, int oId, String scope,
			String resource, int ttlSeconds, Integer chainRoot) throws SQLException {

		String access = newValue();
		String refresh = newValue();

		int accessId = insert(sd, access, TYPE_ACCESS, clientId, uId, oId, scope, resource,
				ttlSeconds, null, null);
		/*
		 * A refresh token outlives its access token by a long way, otherwise refreshing would stop
		 * working at the moment it becomes useful.  It is bounded rather than eternal so an
		 * abandoned client eventually falls out of the table.
		 */
		int refreshId = insert(sd, refresh, TYPE_REFRESH, clientId, uId, oId, scope, resource,
				ttlSeconds * 24 * 30, chainRoot, null);

		if(chainRoot == null) {
			// The first refresh token of a chain is its own root
			setChainRoot(sd, refreshId, refreshId);
		}
		log.info("Issued oauth tokens " + accessId + "/" + refreshId + " for client " + clientId);

		return new Issued(access, refresh, ttlSeconds, scope);
	}

	/*
	 * Mint a token in the console, for a headless caller.  No client, no refresh token: the user
	 * asked for a value they can paste into a script, and a script that can hold a refresh token
	 * can hold an access token with a long life just as easily.
	 */
	public String mint(Connection sd, int uId, int oId, String scope, String resource,
			String name, Integer daysValid) throws SQLException {

		String value = newValue();
		insert(sd, value, TYPE_ACCESS, null, uId, oId, scope, resource,
				daysValid == null ? 0 : daysValid * 24 * 3600, null, name);
		return value;
	}

	/*
	 * Redeem a refresh token, rotating it.
	 *
	 * Returns null when the value is not a live refresh token.  If it names one that has already
	 * been used, the whole chain is revoked before returning: the legitimate client would still be
	 * holding the replacement, so a replay means the value has been copied and the safe reading is
	 * that both copies are now suspect.
	 */
	public Resolved redeemRefresh(Connection sd, String value, String clientId) throws SQLException {

		String hash = ApiTokenManager.hash(value.trim());

		String sql = "select t.id, t.u_id, t.o_id, t.scope, t.resource, t.client_id, t.revoked, "
				+ "coalesce(t.parent_id, t.id) as chain_root, t.expires, u.ident "
				+ "from oauth_token t "
				+ "inner join users u on u.id = t.u_id "
				+ "where t.token_hash = ? and t.type = 'refresh'";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, hash);
			ResultSet rs = pstmt.executeQuery();
			if(!rs.next()) {
				return null;
			}

			int chainRoot = rs.getInt("chain_root");
			boolean revoked = rs.getTimestamp("revoked") != null;
			java.sql.Timestamp expires = rs.getTimestamp("expires");

			if(revoked) {
				log.warning("Refresh token replayed, revoking chain " + chainRoot);
				revokeChain(sd, chainRoot, "replay");
				return null;
			}
			if(expires != null && expires.before(new java.sql.Timestamp(System.currentTimeMillis()))) {
				return null;
			}
			/*
			 * A refresh token belongs to the client it was issued to.  Letting another client
			 * present it would be a way to launder a stolen token through a different registration.
			 */
			String owner = rs.getString("client_id");
			if(owner != null && !owner.equals(clientId)) {
				log.warning("Refresh token presented by the wrong client");
				return null;
			}

			// Single use: consume it, and only proceed if this call is the one that did
			try (PreparedStatement pstmtUse = sd.prepareStatement(
					"update oauth_token set revoked = now(), revoked_by = 'rotated' "
					+ "where id = ? and revoked is null")) {
				pstmtUse.setInt(1, rs.getInt("id"));
				if(pstmtUse.executeUpdate() == 0) {
					return null;
				}
			}

			return new Resolved(chainRoot, rs.getString("ident"), rs.getInt("u_id"),
					rs.getInt("o_id"), rs.getString("scope"), rs.getString("resource"), owner);
		}
	}

	/*
	 * Revoke one token by value, whatever its type.  RFC 7009 wants this to look the same whether
	 * or not the value was real, so nothing is reported back.
	 */
	/*
	 * One application's live access for one person: what it is, when it was first allowed, when it
	 * was last used, and how much it can do.
	 *
	 * Grouped by client and person rather than listed token by token, because a client holding four
	 * refreshed tokens is not four grants - it is one application somebody allowed once, and the
	 * question being asked is always about the application.
	 */
	public static class Grant {
		public String clientId;
		public String clientName;
		public boolean selfRegistered;
		public String user;
		public String firstIssued;
		public String lastUsed;
		public String scopes;
		public int tokens;
	}

	/*
	 * The live grants, either for one person or across an organisation.
	 *
	 * Extracted from the AI access page so that page and MCP answer this the same way.  Whether a
	 * caller may see other people's grants is the caller's question, not this one's: it is decided
	 * where the caller is known and passed in, so there is no way for this to be asked org wide by
	 * something that has not checked.
	 */
	public ArrayList<Grant> getGrants(Connection sd, int oId, int uId, boolean orgWide)
			throws SQLException {

		String sql = "select t.client_id, c.client_name, c.source, u.ident, "
				+ "min(t.issued) as first_issued, max(t.last_used) as last_used, "
				+ "string_agg(distinct t.scope, ' ') as scopes, count(*) as tokens "
				+ "from oauth_token t "
				+ "inner join users u on u.id = t.u_id "
				+ "left outer join oauth_client c on c.client_id = t.client_id "
				+ "where t.revoked is null and t.client_id is not null "
				+ (orgWide ? "and t.o_id = ? " : "and t.u_id = ? ")
				+ "group by t.client_id, c.client_name, c.source, u.ident "
				+ "order by max(t.last_used) desc nulls last";

		ArrayList<Grant> grants = new ArrayList<>();
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, orgWide ? oId : uId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				Grant g = new Grant();
				g.clientId = rs.getString("client_id");
				g.clientName = rs.getString("client_name");
				g.selfRegistered = !"preregistered".equals(rs.getString("source"));
				g.user = rs.getString("ident");
				g.firstIssued = rs.getString("first_issued");
				g.lastUsed = rs.getString("last_used");
				g.scopes = rs.getString("scopes");
				g.tokens = rs.getInt("tokens");
				grants.add(g);
			}
		}
		return grants;
	}

	public void revokeByValue(Connection sd, String value, String revokedBy) {

		String sql = "update oauth_token set revoked = now(), revoked_by = ? "
				+ "where token_hash = ? and revoked is null";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, revokedBy);
			pstmt.setString(2, ApiTokenManager.hash(value.trim()));
			pstmt.executeUpdate();
		} catch (Exception e) {
			log.log(Level.WARNING, "Revoking oauth token", e);
		}
	}

	public void revokeChain(Connection sd, int chainRoot, String revokedBy) throws SQLException {
		String sql = "update oauth_token set revoked = now(), revoked_by = ? "
				+ "where (id = ? or parent_id = ?) and revoked is null";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, revokedBy);
			pstmt.setInt(2, chainRoot);
			pstmt.setInt(3, chainRoot);
			pstmt.executeUpdate();
		}
	}

	/*
	 * Everything issued to one user for one client.  Used when a user withdraws a client's access,
	 * and when the server owner switches MCP off for good.
	 */
	public int revokeForUserAndClient(Connection sd, int uId, String clientId, String revokedBy)
			throws SQLException {

		StringBuilder sql = new StringBuilder("update oauth_token set revoked = now(), revoked_by = ? "
				+ "where u_id = ? and revoked is null");
		if(clientId != null) {
			sql.append(" and client_id = ?");
		}
		try (PreparedStatement pstmt = sd.prepareStatement(sql.toString())) {
			pstmt.setString(1, revokedBy);
			pstmt.setInt(2, uId);
			if(clientId != null) {
				pstmt.setString(3, clientId);
			}
			return pstmt.executeUpdate();
		}
	}

	private int insert(Connection sd, String value, String type, String clientId, int uId, int oId,
			String scope, String resource, int ttlSeconds, Integer chainRoot, String name)
			throws SQLException {

		String sql = "insert into oauth_token (token_hash, type, client_id, u_id, o_id, scope, "
				+ "resource, name, issued, expires, parent_id) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, now(), "
				+ (ttlSeconds > 0 ? "now() + interval '" + ttlSeconds + " seconds'" : "null")
				+ ", ?)";

		try (PreparedStatement pstmt = sd.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
			pstmt.setString(1, ApiTokenManager.hash(value));
			pstmt.setString(2, type);
			pstmt.setString(3, clientId);
			pstmt.setInt(4, uId);
			pstmt.setInt(5, oId);
			pstmt.setString(6, scope);
			pstmt.setString(7, resource);
			pstmt.setString(8, name);
			if(chainRoot == null) {
				pstmt.setNull(9, java.sql.Types.INTEGER);
			} else {
				pstmt.setInt(9, chainRoot);
			}
			pstmt.executeUpdate();
			ResultSet keys = pstmt.getGeneratedKeys();
			if(keys.next()) {
				return keys.getInt(1);
			}
		}
		throw new SQLException("Could not issue token");
	}

	private void setChainRoot(Connection sd, int id, int root) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement(
				"update oauth_token set parent_id = ? where id = ?")) {
			pstmt.setInt(1, root);
			pstmt.setInt(2, id);
			pstmt.executeUpdate();
		}
	}

	/*
	 * The prefix makes a leaked token recognisable for what it is, which is what lets a secret
	 * scanner find one before somebody else does.
	 */
	private String newValue() {
		byte[] bytes = new byte[VALUE_BYTES];
		random.nextBytes(bytes);
		return "smap_mcp_" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
	}
}
