package org.smap.sdal.managers;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.logging.Logger;

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
 * The token in the reply+ address of a notification email
 *
 * A reply is written to the record the token stands for, so the token is what decides where
 * an inbound email lands.  It used to be the record's ids in plain base64, which anyone who
 * knew them could write for themselves, for any record on the server, and which never expired.
 * Now it is random, only its sha256 is kept, and it lapses after REPLY_DAYS.
 */
public class ReplyTokenManager {

	private static Logger log = Logger.getLogger(ReplyTokenManager.class.getName());

	private static final SecureRandom random = new SecureRandom();

	// How long a reply to a notification is accepted
	public static final int REPLY_DAYS = 90;

	// 16 bytes, base64url without padding.  Short, as it goes in the local part of an address
	private static final int TOKEN_LENGTH = 22;

	public static class ReplyToken {
		public int oId;
		public String instanceId;
		public String surveyIdent;
		public int messageId;
	}

	/*
	 * Create a token for replies to an email about a record
	 */
	public String create(Connection sd, int oId, String instanceId, String surveyIdent, int messageId) throws SQLException {

		byte[] bytes = new byte[16];
		random.nextBytes(bytes);
		String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);

		String sql = "insert into reply_token (token_hash, o_id, instanceid, survey_ident, message_id) "
				+ "values(?, ?, ?, ?, ?)";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ApiTokenManager.hash(token));
			pstmt.setInt(2, oId);
			pstmt.setString(3, instanceId);
			pstmt.setString(4, surveyIdent);
			pstmt.setInt(5, messageId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		return token;
	}

	/*
	 * The record a token stands for, or null if it is not one we issued or it has lapsed
	 */
	public ReplyToken get(Connection sd, String token) throws SQLException {

		if(token == null || token.length() != TOKEN_LENGTH || !token.matches("[A-Za-z0-9_-]+")) {
			return null;
		}

		ReplyToken rt = null;
		String sql = "select o_id, instanceid, survey_ident, message_id from reply_token "
				+ "where token_hash = ? "
				+ "and created > now() - interval '" + REPLY_DAYS + " days'";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ApiTokenManager.hash(token));
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				rt = new ReplyToken();
				rt.oId = rs.getInt(1);
				rt.instanceId = rs.getString(2);
				rt.surveyIdent = rs.getString(3);
				rt.messageId = rs.getInt(4);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		return rt;
	}

	/*
	 * Remove tokens that have lapsed
	 */
	public void deleteExpired(Connection sd) throws SQLException {
		String sql = "delete from reply_token where created < now() - interval '" + REPLY_DAYS + " days'";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			int count = pstmt.executeUpdate();
			if(count > 0) {
				log.fine("Deleted " + count + " lapsed reply tokens");
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
	}
}
