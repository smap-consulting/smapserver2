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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base32;

/*
 * Time based one time passwords - RFC 6238 with the parameters every authenticator app
 * defaults to: HMAC-SHA1, 6 digits, a 30 second step.  Sticking to the defaults is what
 * makes the same secret work in Google Authenticator, Microsoft Authenticator, Authy,
 * 1Password, Bitwarden, FreeOTP and Aegis without any per app handling.
 */
public class Totp {

	public static final int DIGITS = 6;
	public static final int STEP_SECONDS = 30;

	/*
	 * How many steps either side of the current one are accepted.  One step covers the
	 * common case of a phone clock that is slightly out, or a user who types the code as
	 * it rolls over.
	 */
	private static final int WINDOW = 1;

	private static final int SECRET_BYTES = 20;		// 160 bits, the size RFC 4226 recommends for SHA1

	private static final SecureRandom random = new SecureRandom();

	private Totp() {
	}

	/*
	 * A new base32 secret to give to the user's authenticator app
	 */
	public static String generateSecret() {
		byte[] buf = new byte[SECRET_BYTES];
		random.nextBytes(buf);
		// No padding - authenticator apps vary in how they treat "=" in a scanned secret
		return new Base32(false).encodeAsString(buf).replace("=", "");
	}

	/*
	 * The time step the given instant falls in.  Stored after a successful verification so
	 * the same code cannot be used twice.
	 */
	public static long counter(long epochSeconds) {
		return epochSeconds / STEP_SECONDS;
	}

	/*
	 * Check a user supplied code against the secret.
	 *
	 * lastCounter is the step of the last code this user successfully used, or null if they
	 * have never used one.  Codes at or before it are refused, so an attacker who sees a code
	 * cannot replay it inside its 30 second life.
	 *
	 * Returns the counter the code matched, or null if it did not match.
	 */
	public static Long verify(String secret, String code, Long lastCounter, long epochSeconds) {

		if(secret == null || code == null) {
			return null;
		}

		String trimmed = code.trim().replace(" ", "");
		if(trimmed.length() != DIGITS) {
			return null;
		}

		long current = counter(epochSeconds);
		for(long c = current - WINDOW; c <= current + WINDOW; c++) {
			if(lastCounter != null && c <= lastCounter) {
				continue;			// Already used, or older than one that was used
			}
			String expected = generate(secret, c);
			if(expected != null && constantTimeEquals(expected, trimmed)) {
				return c;
			}
		}
		return null;
	}

	/*
	 * The code for a given secret and time step
	 */
	public static String generate(String secret, long counter) {

		try {
			byte[] key = new Base32().decode(padForDecode(secret));
			if(key.length == 0) {
				return null;
			}

			byte[] data = ByteBuffer.allocate(8).putLong(counter).array();

			Mac mac = Mac.getInstance("HmacSHA1");
			mac.init(new SecretKeySpec(key, "HmacSHA1"));
			byte[] hash = mac.doFinal(data);

			// Dynamic truncation - RFC 4226 section 5.3
			int offset = hash[hash.length - 1] & 0x0f;
			int binary = ((hash[offset] & 0x7f) << 24)
					| ((hash[offset + 1] & 0xff) << 16)
					| ((hash[offset + 2] & 0xff) << 8)
					| (hash[offset + 3] & 0xff);

			int otp = binary % (int) Math.pow(10, DIGITS);
			return String.format("%0" + DIGITS + "d", otp);

		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			return null;
		}
	}

	/*
	 * The URI an authenticator app expects from a QR code.
	 *
	 * issuer is shown as the account's heading in the app, so it needs to identify this
	 * server - a user with accounts on more than one Smap server has to be able to tell
	 * the entries apart.
	 */
	public static String otpAuthUrl(String issuer, String ident, String secret) {
		StringBuilder sb = new StringBuilder("otpauth://totp/");
		sb.append(encode(issuer)).append(":").append(encode(ident));
		sb.append("?secret=").append(secret);
		sb.append("&issuer=").append(encode(issuer));
		sb.append("&algorithm=SHA1");
		sb.append("&digits=").append(DIGITS);
		sb.append("&period=").append(STEP_SECONDS);
		return sb.toString();
	}

	/*
	 * Base32 secrets are stored without padding, but the decoder wants a multiple of 8
	 * characters
	 */
	private static String padForDecode(String secret) {
		String s = secret.trim().replace(" ", "").toUpperCase();
		int remainder = s.length() % 8;
		if(remainder == 0) {
			return s;
		}
		StringBuilder sb = new StringBuilder(s);
		for(int i = remainder; i < 8; i++) {
			sb.append('=');
		}
		return sb.toString();
	}

	/*
	 * Compare without leaking, through timing, how many leading digits were right
	 */
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

	private static String encode(String v) {
		try {
			return URLEncoder.encode(v == null ? "" : v, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}
}
