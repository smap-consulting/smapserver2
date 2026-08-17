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

package org.smap.sdal.Utilities;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;

/*
 * The two factor "step up".
 *
 * Apache authenticates the password and hands Tomcat a REMOTE_USER; the application never
 * sees the password and cannot hook the login itself.  So the second factor is applied
 * afterwards: once a user with two factor turned on has supplied a valid code we set a
 * signed cookie, and every other service call is refused until that cookie is present.
 *
 * The cookie is signed rather than stored, so validating it costs no database round trip -
 * which matters because it is checked on every request.  Rotating server.two_factor_key
 * invalidates every outstanding step up at once.
 *
 * It is a session cookie with no Max-Age, so it dies when the browser closes.  MAX_AGE_MS
 * is a backstop for a browser that is never closed.
 */
public class TwoFactorSession {

	private static Logger log = Logger.getLogger(TwoFactorSession.class.getName());

	public static final String COOKIE_NAME = "smap_2fa";

	private static final String CONNECTION_NAME = "TwoFactorSession";
	private static final String VERSION = "v1";
	private static final long MAX_AGE_MS = 24 * 60 * 60 * 1000L;	// Backstop for a browser that is never closed

	private static final SecureRandom random = new SecureRandom();

	/*
	 * The signing key, read once from the server table.  Both surveyKPI and any other web
	 * app that gates on two factor read the same row, so a cookie issued by one is valid
	 * in the other.
	 */
	private static volatile byte[] signingKey = null;
	private static final Object keyLock = new Object();

	/*
	 * The idents that currently have two factor turned on.  Held for a short time so the
	 * common case - a user who has never enrolled - does not cost a query per request.
	 * The recent work to cut per request user lookups makes adding one back unattractive.
	 */
	private static volatile Set<String> enabledIdents = null;
	private static volatile long enabledLoaded = 0;
	private static final long ENABLED_RELOAD_INTERVAL_MS = 60000;
	private static final Object enabledLock = new Object();

	private TwoFactorSession() {
	}

	/*
	 * True if this request must supply a code before it is served.  False when the user has
	 * not enrolled, or has already stepped up in this browser session.
	 *
	 * For callers that already hold a connection.
	 */
	public static boolean isRequired(Connection sd, HttpServletRequest request) {

		String ident = request == null ? null : request.getRemoteUser();
		if(ident == null) {
			return false;			// Not authenticated by Apache - the service does its own checks
		}

		if(!isEnabled(sd, ident)) {
			return false;
		}

		return !hasValidCookie(sd, request, ident);
	}

	/*
	 * The same check for callers that do not hold a connection - the request filter, which
	 * runs before any service has opened one.
	 *
	 * Both the enabled user set and the signing key are cached, so a warm request answers
	 * this without touching the database at all.  A connection is taken only when a cache
	 * has to be filled, which for the enabled set is once a minute per web app.
	 */
	public static boolean isRequired(HttpServletRequest request) {
		return isRequired(null, request);
	}

	/*
	 * True if the user has completed enrolment.
	 *
	 * sd may be null, in which case a connection is taken only if the cached set has gone
	 * stale - which is once a minute per web app, not once a request.
	 */
	public static boolean isEnabled(Connection sd, String ident) {

		if(ident == null) {
			return false;
		}

		Set<String> idents = enabledIdents;
		if(idents == null || (System.currentTimeMillis() - enabledLoaded) > ENABLED_RELOAD_INTERVAL_MS) {
			idents = loadEnabled(sd, System.currentTimeMillis());
		}
		return idents.contains(ident);
	}

	/*
	 * Called when a user enrols or their two factor is removed, so the change is seen at
	 * once in this web app rather than after the reload interval
	 */
	public static void invalidateEnabledCache() {
		enabledLoaded = 0;
	}

	private static Set<String> loadEnabled(Connection sd, long now) {

		synchronized(enabledLock) {
			// Another thread may have loaded it while we waited
			if(enabledIdents != null && (now - enabledLoaded) <= ENABLED_RELOAD_INTERVAL_MS) {
				return enabledIdents;
			}

			Connection conn = sd;
			boolean owned = false;
			if(conn == null) {
				conn = SDDataSource.getConnection(CONNECTION_NAME);
				owned = true;
			}

			Set<String> idents = new HashSet<>();
			String sql = "select ident from users where totp_confirmed and totp_secret is not null";
			PreparedStatement pstmt = null;
			try {
				pstmt = conn.prepareStatement(sql);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					idents.add(rs.getString(1));
				}
				enabledIdents = Collections.unmodifiableSet(idents);
				enabledLoaded = System.currentTimeMillis();
			} catch (SQLException e) {
				log.log(Level.SEVERE, "Error loading two factor users", e);
				// Fail closed only if we have never loaded - an empty set would otherwise
				// turn two factor off for everyone until the next successful load
				if(enabledIdents == null) {
					throw new ServerException();
				}
			} finally {
				try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				if(owned) {
					SDDataSource.closeConnection(CONNECTION_NAME, conn);
				}
			}
			return enabledIdents;
		}
	}

	/*
	 * Build the cookie value to hand back after a code has been verified.  The caller sets
	 * it with cookieHeader().
	 */
	public static String issue(Connection sd, String ident) {

		String payload = payload(ident, System.currentTimeMillis(), newNonce());
		String signature = sign(sd, payload);
		if(signature == null) {
			// Handing back an unsigned cookie would leave the user stuck at a challenge they
			// can never pass, with no indication why
			throw new ServerException();
		}
		return payload + "." + signature;
	}

	/*
	 * The Set-Cookie header for the step up cookie.
	 *
	 * Built by hand rather than with NewCookie because SameSite is not in the JAX-RS cookie
	 * API.  No Max-Age, so it lasts for the browser session and no longer.  Secure is always
	 * set - the console is only served over https.
	 */
	public static String cookieHeader(String value) {
		return COOKIE_NAME + "=" + value + "; Path=/; HttpOnly; Secure; SameSite=Strict";
	}

	/*
	 * Header that removes the cookie
	 */
	public static String clearCookieHeader() {
		return COOKIE_NAME + "=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0";
	}

	/*
	 * True if the request carries a step up cookie that is properly signed, belongs to this
	 * user and is not older than the backstop
	 */
	public static boolean hasValidCookie(Connection sd, HttpServletRequest request, String ident) {

		Cookie[] cookies = request.getCookies();
		if(cookies == null || ident == null) {
			return false;
		}

		for(Cookie cookie : cookies) {
			if(COOKIE_NAME.equals(cookie.getName()) && isValid(sd, cookie.getValue(), ident)) {
				return true;
			}
		}
		return false;
	}

	private static boolean isValid(Connection sd, String value, String ident) {

		if(value == null) {
			return false;
		}

		// version . ident . issued . nonce . signature
		int lastDot = value.lastIndexOf('.');
		if(lastDot < 0) {
			return false;
		}
		String payload = value.substring(0, lastDot);
		String signature = value.substring(lastDot + 1);

		String expected = sign(sd, payload);
		if(expected == null || !constantTimeEquals(expected, signature)) {
			return false;
		}

		String[] parts = payload.split("\\.");
		if(parts.length != 4 || !VERSION.equals(parts[0])) {
			return false;
		}

		// The cookie names the user it was issued to, so it cannot be moved to another account
		if(!encode(ident).equals(parts[1])) {
			return false;
		}

		try {
			long issued = Long.parseLong(parts[2]);
			return (System.currentTimeMillis() - issued) <= MAX_AGE_MS;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	private static String payload(String ident, long issued, String nonce) {
		return VERSION + "." + encode(ident) + "." + issued + "." + nonce;
	}

	private static String newNonce() {
		byte[] buf = new byte[9];
		random.nextBytes(buf);
		return Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
	}

	private static String sign(Connection sd, String payload) {
		try {
			Mac mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec(getKey(sd), "HmacSHA256"));
			byte[] hash = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
			return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			log.log(Level.SEVERE, "Error signing two factor cookie", e);
			return null;
		}
	}

	/*
	 * The signing key, generated on first use.  Held in the server table so every web app
	 * and every Tomcat restart uses the same one, and read only once per web app - after
	 * that, checking a cookie costs no database access.
	 *
	 * sd may be null, in which case a connection is taken for the one time read.
	 */
	private static byte[] getKey(Connection sd) {

		byte[] key = signingKey;
		if(key != null) {
			return key;
		}

		synchronized(keyLock) {
			if(signingKey != null) {
				return signingKey;
			}

			Connection conn = sd;
			boolean owned = false;
			if(conn == null) {
				conn = SDDataSource.getConnection(CONNECTION_NAME);
				owned = true;
			}
			try {
				signingKey = Base64.getDecoder().decode(loadOrCreateKey(conn));
				return signingKey;
			} finally {
				if(owned) {
					SDDataSource.closeConnection(CONNECTION_NAME, conn);
				}
			}
		}
	}

	private static String loadOrCreateKey(Connection sd) {

		String stored = readKey(sd);
		if(stored == null) {
			byte[] generated = new byte[32];
			random.nextBytes(generated);
			String encoded = Base64.getEncoder().encodeToString(generated);

			// "where two_factor_key is null" so a second server generating at the same
			// time does not overwrite the first one's key
			PreparedStatement pstmt = null;
			try {
				pstmt = sd.prepareStatement("update server set two_factor_key = ? where two_factor_key is null");
				pstmt.setString(1, encoded);
				pstmt.executeUpdate();
			} catch (SQLException e) {
				log.log(Level.SEVERE, "Error creating two factor key", e);
				throw new ServerException();
			} finally {
				try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			}
			stored = readKey(sd);
		}

		if(stored == null) {
			log.log(Level.SEVERE, "No two factor key and none could be created");
			throw new ServerException();
		}
		return stored;
	}

	private static String readKey(Connection sd) {
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select two_factor_key from server");
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return rs.getString(1);
			}
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Error reading two factor key", e);
			throw new ServerException();
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return null;
	}

	private static String encode(String v) {
		return Base64.getUrlEncoder().withoutPadding().encodeToString(v.getBytes(StandardCharsets.UTF_8));
	}

	private static boolean constantTimeEquals(String a, String b) {
		if(a.length() != b.length()) {
			return false;
		}
		int diff = 0;
		for(int i = 0; i < a.length(); i++) {
			diff |= a.charAt(i) ^ b.charAt(i);
		}
		return diff == 0;
	}
}
