package org.smap.sdal.managers;

/*
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

*/

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Base64;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Totp;
import org.smap.sdal.Utilities.TwoFactorSession;
import org.smap.sdal.model.TwoFactorStatus;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

/*
 * Two factor authentication - enrolment, verification and removal.
 *
 * The password is checked by Apache before any of this runs, so these methods only ever
 * deal with the second factor.
 */
public class TwoFactorManager {

	private static Logger log = Logger.getLogger(TwoFactorManager.class.getName());

	private ResourceBundle localisation;
	private LogManager lm = new LogManager();

	private static final int QR_SIZE = 240;

	/*
	 * Codes are only six digits, so an attacker who already has the password could guess
	 * one in a few thousand tries.  One bucket per user, shared by enrolment confirmation,
	 * the login challenge and removal.
	 */
	private static final int ATTEMPTS_PER_MINUTE = 5;
	private static final ConcurrentHashMap<String, Bucket> attempts = new ConcurrentHashMap<>();

	public TwoFactorManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Whether the user has finished enrolling, and whether they have a half finished
	 * enrolment sitting there
	 */
	public TwoFactorStatus getStatus(Connection sd, String ident) throws SQLException {

		TwoFactorStatus status = new TwoFactorStatus();

		String sql = "select totp_secret, totp_confirmed, totp_enrolled from users where ident = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String secret = rs.getString("totp_secret");
				status.enabled = secret != null && rs.getBoolean("totp_confirmed");
				status.pending = secret != null && !rs.getBoolean("totp_confirmed");
				Timestamp enrolled = rs.getTimestamp("totp_enrolled");
				status.enrolled = enrolled == null ? null : enrolled.toString();
			}
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return status;
	}

	/*
	 * Start enrolment.  Stores a new unconfirmed secret and returns what the user needs to
	 * add it to their authenticator app.
	 *
	 * The secret is stored straight away rather than held in a session, so the flow works
	 * across the several requests it takes and survives a Tomcat restart.  It does nothing
	 * until a code confirms it, and starting again simply replaces it.
	 */
	public TwoFactorStatus enrol(Connection sd, String ident, String issuer) throws SQLException, ApplicationException {

		TwoFactorStatus existing = getStatus(sd, ident);
		if(existing.enabled) {
			throw new ApplicationException(localisation.getString("c_2fa_already"));
		}

		String secret = Totp.generateSecret();

		String sql = "update users set totp_secret = ?, totp_confirmed = false, "
				+ "totp_enrolled = null, totp_last_counter = null where ident = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, secret);
			pstmt.setString(2, ident);
			pstmt.executeUpdate();
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		TwoFactorStatus status = new TwoFactorStatus();
		status.pending = true;
		status.secret = secret;
		status.otpauthUrl = Totp.otpAuthUrl(issuer, ident, secret);
		status.qrPng = qrCode(status.otpauthUrl);
		return status;
	}

	/*
	 * Finish enrolment.  The code proves the user really did add the secret to their app,
	 * so they cannot lock themselves out by mistyping the manual entry.
	 */
	public void confirm(Connection sd, String ident, String code, int oId)
			throws SQLException, ApplicationException {

		if(!verifyCode(sd, ident, code, false)) {
			throw new ApplicationException(localisation.getString("c_2fa_bad"));
		}

		String sql = "update users set totp_confirmed = true, totp_enrolled = now() where ident = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);
			pstmt.executeUpdate();
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		TwoFactorSession.invalidateEnabledCache();
		lm.writeLogOrganisation(sd, oId, ident, LogManager.TWO_FACTOR,
				localisation.getString("c_2fa_log_on"), 0);
	}

	/*
	 * The login challenge.  Throws if the code is wrong so the caller does not have to
	 * decide what an unsuccessful verification means.
	 */
	public void verify(Connection sd, String ident, String code, int oId)
			throws SQLException, ApplicationException {

		if(!verifyCode(sd, ident, code, true)) {
			lm.writeLogOrganisation(sd, oId, ident, LogManager.TWO_FACTOR,
					localisation.getString("c_2fa_log_fail"), 0);
			throw new ApplicationException(localisation.getString("c_2fa_bad"));
		}
	}

	/*
	 * A user turning their own two factor off.  A current code is required: the session has
	 * already been stepped up, but if it were hijacked we do not want the attacker to be
	 * able to quietly remove the second factor.
	 */
	public void remove(Connection sd, String ident, String code, int oId)
			throws SQLException, ApplicationException {

		if(!verifyCode(sd, ident, code, true)) {
			throw new ApplicationException(localisation.getString("c_2fa_bad"));
		}

		clear(sd, ident);
		lm.writeLogOrganisation(sd, oId, ident, LogManager.TWO_FACTOR,
				localisation.getString("c_2fa_log_off"), 0);
	}

	/*
	 * An administrator clearing another user's two factor.  This is the only way back in
	 * for a user who has lost their phone, so it is deliberately logged against both.
	 */
	public void reset(Connection sd, String adminIdent, String ident, int oId) throws SQLException {

		clear(sd, ident);

		String msg = localisation.getString("c_2fa_log_reset");
		msg = msg.replace("%s1", ident);
		msg = msg.replace("%s2", adminIdent);
		lm.writeLogOrganisation(sd, oId, adminIdent, LogManager.TWO_FACTOR, msg, 0);
	}

	private void clear(Connection sd, String ident) throws SQLException {

		String sql = "update users set totp_secret = null, totp_confirmed = false, "
				+ "totp_enrolled = null, totp_last_counter = null where ident = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);
			pstmt.executeUpdate();
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		TwoFactorSession.invalidateEnabledCache();
	}

	/*
	 * Check a code and, if it is good, record the time step it used so the same code cannot
	 * be presented again.
	 *
	 * mustBeConfirmed is false while confirming an enrolment, when the secret exists but is
	 * not yet active.
	 */
	private boolean verifyCode(Connection sd, String ident, String code, boolean mustBeConfirmed)
			throws SQLException, ApplicationException {

		throttle(ident);

		String secret = null;
		Long lastCounter = null;
		boolean confirmed = false;

		String sql = "select totp_secret, totp_confirmed, totp_last_counter from users where ident = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				secret = rs.getString("totp_secret");
				confirmed = rs.getBoolean("totp_confirmed");
				long c = rs.getLong("totp_last_counter");
				if(!rs.wasNull()) {
					lastCounter = c;
				}
			}
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		if(secret == null || (mustBeConfirmed && !confirmed)) {
			return false;
		}

		Long matched = Totp.verify(secret, code, lastCounter, System.currentTimeMillis() / 1000);
		if(matched == null) {
			return false;
		}

		// Record the step that was used.  The "greater than" guard means two requests racing
		// cannot move the counter backwards.
		String update = "update users set totp_last_counter = ? "
				+ "where ident = ? and (totp_last_counter is null or totp_last_counter < ?)";
		PreparedStatement pstmtUpdate = null;
		try {
			pstmtUpdate = sd.prepareStatement(update);
			pstmtUpdate.setLong(1, matched);
			pstmtUpdate.setString(2, ident);
			pstmtUpdate.setLong(3, matched);
			if(pstmtUpdate.executeUpdate() == 0) {
				return false;		// Another request used this code first
			}
		} finally {
			try {if(pstmtUpdate != null) {pstmtUpdate.close();}} catch (SQLException e) {}
		}

		return true;
	}

	private void throttle(String ident) throws ApplicationException {

		Bucket bucket = attempts.computeIfAbsent(ident, k ->
			Bucket.builder()
				.addLimit(Bandwidth.classic(ATTEMPTS_PER_MINUTE,
						Refill.greedy(ATTEMPTS_PER_MINUTE, Duration.ofMinutes(1))))
				.build()
		);

		if(!bucket.tryConsume(1)) {
			throw new ApplicationException(localisation.getString("c_2fa_throttled"));
		}
	}

	/*
	 * The otpauth URL as a PNG data URI.
	 *
	 * Rendered here rather than in the browser because the console pages do not all load a
	 * QR library, and zxing is already a dependency.
	 */
	private String qrCode(String url) {

		try {
			QRCodeWriter writer = new QRCodeWriter();
			BitMatrix matrix = writer.encode(url, BarcodeFormat.QR_CODE, QR_SIZE, QR_SIZE);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			MatrixToImageWriter.writeToStream(matrix, "PNG", out);
			return "data:image/png;base64," + Base64.getEncoder().encodeToString(out.toByteArray());
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error creating two factor QR code", e);
			return null;		// The page still shows the secret for manual entry
		}
	}
}
