package org.smap.sdal.managers;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Logger;

import com.google.gson.Gson;

/*
 * A confirmation that has been put to a person but not yet given back.
 *
 * The specification carries this as requestState, an opaque string the client echoes back, and
 * allows the server to encode the state into it so nothing has to be stored.  This keeps it in a
 * table instead, for two reasons.
 *
 * The state decides whether a send happens or is refused, so it is exactly the case the
 * specification says must be integrity protected against the client - a forged one would skip the
 * approval it exists to obtain.  Signing it would mean a key to generate, store and rotate, where
 * Smap already has the shape of this table in oauth_grant and no key at all.
 *
 * And a row can be consumed.  The specification points out that signing bounds the replay window
 * without making a state single use, and an approval to send two emails must not be redeemable
 * twice.  Deleting the row on use is the whole of that guarantee.
 *
 * Nothing about the call is stored beyond a digest.  The arguments come back on the retry, and are
 * checked against what was shown, so tampering with a row can cause a refusal and nothing else.
 */
public class McpConfirmationManager {

	private static Logger log = Logger.getLogger(McpConfirmationManager.class.getName());

	/*
	 * Long enough for somebody to read what they are approving and decide, short enough that an
	 * approval cannot be left lying around to be used against a survey that has since changed.
	 */
	private static final int TTL_SECONDS = 600;

	/* Why a state was refused.  The caller says what to do about it; this only says what happened */
	public enum Outcome { VALID, UNKNOWN, EXPIRED, WRONG_USER, WRONG_CALL }

	/*
	 * Remember that this exact call was put to this person, and return the handle they send back.
	 */
	public String create(Connection sd, int uId, String clientId, String tool,
			Map<String, Object> arguments) throws Exception {

		String stateId = UUID.randomUUID().toString();
		String sql = "insert into mcp_pending_action "
				+ "(state_id, u_id, client_id, tool, arguments_hash, expires) "
				+ "values(?, ?, ?, ?, ?, now() + interval '" + TTL_SECONDS + " seconds')";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, stateId);
			pstmt.setInt(2, uId);
			pstmt.setString(3, clientId);
			pstmt.setString(4, tool);
			pstmt.setString(5, digest(arguments));
			pstmt.executeUpdate();
		}
		return stateId;
	}

	/*
	 * Redeem a handle, once.
	 *
	 * Everything the specification asks to be checked is checked here: that the person presenting it
	 * is the person it was issued to, that it has not lapsed, and that it belongs to this call
	 * rather than some other one.  The row is deleted whatever the verdict, because a state that has
	 * been presented has been spent - offering a second attempt would let a client keep trying a
	 * state against different arguments until one matched.
	 */
	public Outcome consume(Connection sd, String stateId, int uId, String tool,
			Map<String, Object> arguments) throws Exception {

		if(stateId == null || stateId.trim().isEmpty()) {
			return Outcome.UNKNOWN;
		}

		String sql = "delete from mcp_pending_action where state_id = ? "
				+ "returning u_id, tool, arguments_hash, expires < now() as lapsed";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, stateId.trim());
			ResultSet rs = pstmt.executeQuery();
			if(!rs.next()) {
				return Outcome.UNKNOWN;
			}
			if(rs.getBoolean("lapsed")) {
				return Outcome.EXPIRED;
			}
			if(rs.getInt("u_id") != uId) {
				log.warning("MCP confirmation presented by a different user than it was issued to");
				return Outcome.WRONG_USER;
			}
			if(!tool.equals(rs.getString("tool"))) {
				return Outcome.WRONG_CALL;
			}
			if(!digest(arguments).equals(rs.getString("arguments_hash"))) {
				/*
				 * The retry is not the call that was approved.  Refusing rather than proceeding is
				 * the point of storing the digest: otherwise a confirmation shown for one record
				 * could be redeemed against another.
				 */
				log.warning("MCP confirmation presented with arguments that differ from those shown");
				return Outcome.WRONG_CALL;
			}
			return Outcome.VALID;
		}
	}

	/* Old rows are worthless the moment they lapse, and nothing reads them again */
	public void deleteExpired(Connection sd) throws Exception {
		try (PreparedStatement pstmt = sd.prepareStatement(
				"delete from mcp_pending_action where expires < now()")) {
			pstmt.executeUpdate();
		}
	}

	/*
	 * A digest of the arguments that does not depend on how they were written.
	 *
	 * Sorted before hashing, because the same call can arrive with its keys in a different order and
	 * two orderings of one call must not read as two different calls.
	 */
	private String digest(Map<String, Object> arguments) throws Exception {
		Map<String, Object> sorted = new TreeMap<>(arguments == null ? new TreeMap<>() : arguments);
		byte[] bytes = MessageDigest.getInstance("SHA-256")
				.digest(new Gson().toJson(sorted).getBytes("UTF-8"));
		StringBuilder sb = new StringBuilder();
		for(byte b : bytes) {
			sb.append(String.format("%02x", b));
		}
		return sb.toString();
	}
}
