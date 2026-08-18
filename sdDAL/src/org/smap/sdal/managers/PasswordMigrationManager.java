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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;

/*
 * Moves accounts off the old device password hash without anyone having to do anything.
 *
 * The device and API locations authenticate against users.password, an unsalted MD5
 * digest HA1.  The console authenticates against users.basic_password, which now holds
 * bcrypt.  We want to drop the MD5, but an account created before basic_password existed,
 * whose password has never been changed since, has only the MD5 - and basic_password
 * cannot be derived from it.
 *
 * Asking those users to reset is the obvious answer and the wrong one: they are precisely
 * the people who only ever use fieldTask, never see the console, and may be working
 * somewhere with no practical way to arrange a new password.
 *
 * So take the opportunity the device itself provides.  fieldTask sends the password with
 * every request, as Basic credentials, and Apache has already checked it against the MD5
 * before the request reaches us.  When such a user appears with no basic_password, hash
 * what they just sent with bcrypt and store it.  They migrate by carrying on working.
 *
 * Once no account is left without a basic_password the device locations can move to it
 * and users.password can go - and only then does the bcrypt actually buy anything, since
 * until then the same password is still recoverable from the MD5 sitting beside it.
 *
 * The plaintext is never logged and never leaves this class.  It is checked against the
 * stored digest before anything is written, so a mismatched header can never overwrite
 * somebody's credential.
 */
public class PasswordMigrationManager {

	private static Logger log = Logger.getLogger(PasswordMigrationManager.class.getName());

	private static final String REALM = "smap";

	/*
	 * Who still has no basic_password.  Normally empty, and then this costs one small
	 * query a minute per JVM and nothing at all per request.  Same shape as the cache in
	 * TwoFactorSession.
	 */
	private static volatile Set<String> pending = null;
	private static volatile long loaded = 0;
	private static final long RELOAD_INTERVAL_MS = 60000;
	private static final Object lock = new Object();

	private final LogManager lm = new LogManager();

	/*
	 * Called for every request Apache authenticated.  Does nothing at all unless this
	 * particular user is one of the few that still needs migrating.
	 */
	public void migrateIfNeeded(Connection sd, HttpServletRequest request, String ident) {

		if(ident == null || request == null) {
			return;
		}

		try {
			if(!isPending(sd, ident)) {
				return;
			}

			String password = basicPassword(request);
			if(password == null) {
				return;			// Form authenticated, or a token - no plaintext to work with
			}

			if(!matchesDigest(sd, ident, password)) {
				// Apache authenticates against the same digest, so this should not happen.
				// If it does, the header does not belong to the authenticated user and is
				// not something to derive a credential from.
				log.warning("Password migration skipped for " + ident + ": credentials do not match the stored digest");
				return;
			}

			store(sd, ident, password);
			invalidate();

			lm.writeLog(sd, 0, ident, LogManager.USER,
					"Password migrated to the current hash on device sign in", 0, request.getServerName());

		} catch (Exception e) {
			// Never fail a request because the migration could not be done
			log.log(Level.WARNING, "Migrating password for " + ident, e);
		}
	}

	private boolean isPending(Connection sd, String ident) throws SQLException {

		long now = System.currentTimeMillis();
		if(pending == null || (now - loaded) > RELOAD_INTERVAL_MS) {
			load(sd, now);
		}
		Set<String> current = pending;
		return current != null && current.contains(ident);
	}

	private void load(Connection sd, long now) throws SQLException {

		synchronized(lock) {
			if(pending != null && (now - loaded) <= RELOAD_INTERVAL_MS) {
				return;
			}

			Set<String> idents = new HashSet<>();
			PreparedStatement pstmt = null;
			try {
				pstmt = sd.prepareStatement("select ident from users where basic_password is null");
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					idents.add(rs.getString(1));
				}
			} finally {
				try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			}

			pending = Collections.unmodifiableSet(idents);
			loaded = now;
			if(!idents.isEmpty()) {
				log.info("Accounts still to move off the old password hash: " + idents.size());
			}
		}
	}

	public static void invalidate() {
		synchronized(lock) {
			loaded = 0;
		}
	}

	/*
	 * The password from a Basic Authorization header, or null if there is not one.
	 */
	private String basicPassword(HttpServletRequest request) {

		String header = request.getHeader("authorization");
		if(header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) {
			return null;
		}

		try {
			byte[] decoded = Base64.getDecoder().decode(header.substring(6).trim());
			String pair = new String(decoded, StandardCharsets.UTF_8);
			int colon = pair.indexOf(':');
			if(colon < 0 || colon == pair.length() - 1) {
				return null;
			}
			return pair.substring(colon + 1);
		} catch (Exception e) {
			return null;		// Not something we can read, so not something to act on
		}
	}

	/*
	 * Confirm the plaintext really is this user's password before deriving anything from it
	 */
	private boolean matchesDigest(Connection sd, String ident, String password) throws SQLException {

		String sql = "select count(*) from users where ident = ? and password = md5(?)";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);
			pstmt.setString(2, ident + ":" + REALM + ":" + password);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() && rs.getInt(1) > 0;
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}

	private void store(Connection sd, String ident, String password) throws SQLException {

		String sql = "update users set basic_password = crypt(?, gen_salt('bf', 10)) "
				+ "where ident = ? and basic_password is null";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, password);
			pstmt.setString(2, ident);
			if(pstmt.executeUpdate() > 0) {
				log.info("Password migrated to the current hash for " + ident);
			}
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}
}
