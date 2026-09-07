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

import java.net.URI;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.CimdFetcher;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.model.OAuthClient;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/*
 * Clients, consent and authorization codes for the MCP authorization server.
 *
 * The 2026-07-28 MCP specification makes Client ID Metadata Documents the standard way a client
 * identifies itself: the client id is an https URL and this server reads the client's metadata from
 * it.  Dynamic registration still works for clients that cannot do that yet, but it is deprecated
 * and will go.
 *
 * Either way the client is only ever half the story.  A client id is not access: nothing happens
 * until a user who holds the mcp access group sits in front of the consent screen and approves it.
 */
public class OAuthManager {

	private static Logger log = Logger.getLogger(OAuthManager.class.getName());

	public static final String REGISTRATION_CIMD = "cimd";
	public static final String REGISTRATION_CIMD_DCR = "cimd+dcr";
	public static final String REGISTRATION_OFF = "off";

	private static final int CODE_BYTES = 32;
	private static final int CODE_TTL_SECONDS = 60;
	private static final long METADATA_TTL_MS = 60 * 60 * 1000L;
	private static final int MAX_CLIENTS = 5000;

	private static final SecureRandom random = new SecureRandom();
	private static final Gson gson = new Gson();

	/*
	 * A redeemed authorization code
	 */
	public static class Grant {
		public String clientId;
		public int uId;
		public int oId;
		public String scope;
		public String resource;
		public String redirectUri;
	}

	/* ------------------------------------------------------------------ clients */

	/*
	 * Find the client behind a client id, reading its metadata document if the id is a URL.
	 *
	 * The document is cached in oauth_client and re-read when it goes stale, so an authorize
	 * request does not usually make an outbound call, and a client that changes its redirect URIs
	 * is picked up within the hour.
	 */
	public OAuthClient resolveClient(Connection sd, String clientId, String registrationPolicy)
			throws ApplicationException {

		if(clientId == null || clientId.trim().length() == 0) {
			throw new ApplicationException("A client id is required");
		}

		if(CimdFetcher.isMetadataUrl(clientId)) {
			return resolveCimdClient(sd, clientId);
		}

		if(REGISTRATION_CIMD.equals(registrationPolicy)) {
			throw new ApplicationException("This server only accepts client id metadata documents");
		}

		OAuthClient client = read(sd, clientId);
		if(client == null) {
			throw new ApplicationException("Unknown client");
		}
		if("disabled".equals(client.status)) {
			throw new ApplicationException("This client has been disabled");
		}
		if("pending".equals(client.status)) {
			throw new ApplicationException("This client is waiting to be approved by an administrator");
		}
		return client;
	}

	private OAuthClient resolveCimdClient(Connection sd, String clientId) throws ApplicationException {

		OAuthClient cached = read(sd, clientId);
		if(cached != null && cached.fetchedWithin(METADATA_TTL_MS)) {
			if("disabled".equals(cached.status)) {
				throw new ApplicationException("This client has been disabled");
			}
			return cached;
		}

		String body;
		try {
			body = CimdFetcher.fetch(clientId);
		} catch (CimdFetcher.NotAllowed e) {
			log.warning("Client metadata refused for " + clientId + ": " + e.getMessage());
			/*
			 * Fall back to whatever was read last time rather than locking out a working client
			 * because its metadata host had a bad minute.  A client that has never been seen has
			 * nothing to fall back to and is refused.
			 */
			if(cached != null) {
				return cached;
			}
			throw new ApplicationException(e.getMessage());
		}

		OAuthClient client = parseMetadata(clientId, body);

		for(String uri : client.redirect_uris) {
			checkRedirectUriShape(uri, client.isNative());
			checkRedirectUriOrigin(clientId, uri, client.isNative());
		}

		client.source = "cimd";
		client.status = "active";
		upsert(sd, client);
		return client;
	}

	private OAuthClient parseMetadata(String clientId, String body) throws ApplicationException {

		OAuthClient client = new OAuthClient();
		client.client_id = clientId;

		try {
			JsonObject o = JsonParser.parseString(body).getAsJsonObject();

			/*
			 * A document that names a different client id is either misconfigured or is trying to
			 * pass itself off as somebody else
			 */
			if(o.has("client_id") && !clientId.equals(o.get("client_id").getAsString())) {
				throw new ApplicationException("The client metadata names a different client id");
			}

			client.client_name = optString(o, "client_name");
			client.token_endpoint_auth_method = optString(o, "token_endpoint_auth_method");
			client.application_type = optString(o, "application_type");
			client.scope = optString(o, "scope");
			client.software_id = optString(o, "software_id");
			client.redirect_uris = optArray(o, "redirect_uris");
			client.grant_types = optArray(o, "grant_types");

		} catch (ApplicationException e) {
			throw e;
		} catch (Exception e) {
			log.log(Level.WARNING, "Parsing client metadata from " + clientId, e);
			throw new ApplicationException("The client metadata document could not be read");
		}

		if(client.redirect_uris.isEmpty()) {
			throw new ApplicationException("The client metadata names no redirect uri");
		}
		return client;
	}

	/*
	 * Dynamic registration.  Deprecated by the specification and kept only for clients that cannot
	 * present a metadata document.
	 */
	public OAuthClient register(Connection sd, String body, String registrationPolicy, String ip)
			throws ApplicationException, SQLException {

		if(!REGISTRATION_CIMD_DCR.equals(registrationPolicy)) {
			throw new ApplicationException("Dynamic client registration is not enabled on this server");
		}
		if(countClients(sd) >= MAX_CLIENTS) {
			throw new ApplicationException("This server is not accepting further client registrations");
		}

		OAuthClient client = new OAuthClient();
		try {
			JsonObject o = JsonParser.parseString(body).getAsJsonObject();
			client.client_name = optString(o, "client_name");
			client.token_endpoint_auth_method = optString(o, "token_endpoint_auth_method");
			client.application_type = optString(o, "application_type");
			client.scope = optString(o, "scope");
			client.software_id = optString(o, "software_id");
			client.redirect_uris = optArray(o, "redirect_uris");
			client.grant_types = optArray(o, "grant_types");
		} catch (Exception e) {
			throw new ApplicationException("The registration request could not be read");
		}

		if(client.redirect_uris.isEmpty()) {
			throw new ApplicationException("At least one redirect uri is required");
		}
		for(String uri : client.redirect_uris) {
			checkRedirectUriShape(uri, client.isNative());
		}
		if(client.grant_types.isEmpty()) {
			client.grant_types.add("authorization_code");
			client.grant_types.add("refresh_token");
		}

		client.client_id = "smap_client_" + randomValue();
		client.source = "dcr";
		client.status = "active";
		client.registration_ip = ip;

		/*
		 * A secret is only useful to a client that can keep one.  A CLI or desktop client cannot,
		 * which is what token_endpoint_auth_method none means, and PKCE is what protects it.
		 */
		if(!client.isPublic()) {
			client.client_secret = randomValue();
		}

		upsert(sd, client);
		log.info("Registered oauth client " + client.client_id + " from " + ip);
		return client;
	}

	public OAuthClient read(Connection sd, String clientId) {

		String sql = "select id, client_id, source, client_name, redirect_uris, grant_types, "
				+ "token_endpoint_auth_method, application_type, scope, software_id, status, "
				+ "registration_ip, client_secret_hash, "
				+ "extract(epoch from (now() - metadata_fetched)) * 1000 as age_ms "
				+ "from oauth_client where client_id = ?";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, clientId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				OAuthClient c = new OAuthClient();
				c.id = rs.getInt("id");
				c.client_id = rs.getString("client_id");
				c.source = rs.getString("source");
				c.client_name = rs.getString("client_name");
				c.redirect_uris = fromJsonArray(rs.getString("redirect_uris"));
				c.grant_types = fromJsonArray(rs.getString("grant_types"));
				c.token_endpoint_auth_method = rs.getString("token_endpoint_auth_method");
				c.application_type = rs.getString("application_type");
				c.scope = rs.getString("scope");
				c.software_id = rs.getString("software_id");
				c.status = rs.getString("status");
				c.registration_ip = rs.getString("registration_ip");
				c.secretHash = rs.getString("client_secret_hash");
				double age = rs.getDouble("age_ms");
				c.metadataAgeMs = rs.wasNull() ? Long.MAX_VALUE : (long) age;
				return c;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Reading oauth client", e);
		}
		return null;
	}

	private void upsert(Connection sd, OAuthClient c) throws ApplicationException {

		String sql = "insert into oauth_client (client_id, source, client_secret_hash, client_name, "
				+ "redirect_uris, grant_types, token_endpoint_auth_method, application_type, scope, "
				+ "software_id, status, registration_ip, metadata_fetched) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) "
				+ "on conflict (client_id) do update set "
				+ "client_name = excluded.client_name, "
				+ "redirect_uris = excluded.redirect_uris, "
				+ "grant_types = excluded.grant_types, "
				+ "token_endpoint_auth_method = excluded.token_endpoint_auth_method, "
				+ "application_type = excluded.application_type, "
				+ "scope = excluded.scope, "
				+ "software_id = excluded.software_id, "
				+ "metadata_fetched = now()";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, c.client_id);
			pstmt.setString(2, c.source);
			pstmt.setString(3, c.client_secret == null ? null : ApiTokenManager.hash(c.client_secret));
			pstmt.setString(4, c.client_name);
			pstmt.setString(5, gson.toJson(c.redirect_uris));
			pstmt.setString(6, gson.toJson(c.grant_types));
			pstmt.setString(7, c.token_endpoint_auth_method);
			pstmt.setString(8, c.application_type);
			pstmt.setString(9, c.scope);
			pstmt.setString(10, c.software_id);
			pstmt.setString(11, c.status);
			pstmt.setString(12, c.registration_ip);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Saving oauth client", e);
			throw new ApplicationException("The client could not be saved");
		}
	}

	private int countClients(Connection sd) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement("select count(*) from oauth_client")) {
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	/*
	 * Clients that registered and were never used.  Registration is open, so without this the table
	 * is a place anyone can write to for ever.
	 */
	public int reapUnusedClients(Connection sd) throws SQLException {
		String sql = "delete from oauth_client "
				+ "where source = 'dcr' "
				+ "and last_grant is null "
				+ "and client_id_issued_at < now() - interval '7 days'";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			return pstmt.executeUpdate();
		}
	}

	/* ------------------------------------------------------- redirect uri checks */

	/*
	 * Exact match against what the client registered.  No wildcards and no prefix matching: an open
	 * redirect here hands the authorization code to whoever asked for it.
	 */
	public boolean isRegisteredRedirectUri(OAuthClient client, String redirectUri) {
		if(redirectUri == null) {
			return false;
		}
		for(String uri : client.redirect_uris) {
			if(uri.equals(redirectUri)) {
				return true;
			}
		}
		return false;
	}

	/*
	 * A metadata document can claim any redirect uri it likes, so the claim is only worth anything
	 * if it is tied back to where the document came from.  A web client's redirect must sit on the
	 * same origin as its client id; a native client is exempt because it redirects to loopback or
	 * to its own private scheme, neither of which can belong to an origin.
	 *
	 * Without this, anyone who can host a document could name somebody else's callback and collect
	 * their authorization codes.
	 */
	public void checkRedirectUriOrigin(String clientId, String redirectUri, boolean isNative)
			throws ApplicationException {

		if(isNative) {
			return;
		}
		try {
			URI id = new URI(clientId);
			URI redirect = new URI(redirectUri);
			boolean sameOrigin = id.getScheme().equalsIgnoreCase(redirect.getScheme())
					&& id.getHost().equalsIgnoreCase(redirect.getHost())
					&& id.getPort() == redirect.getPort();
			if(!sameOrigin) {
				throw new ApplicationException(
						"Redirect uri must be on the same origin as the client id: " + redirectUri);
			}
		} catch (ApplicationException e) {
			throw e;
		} catch (Exception e) {
			throw new ApplicationException("Redirect uri could not be checked against the client id");
		}
	}

	/*
	 * https everywhere, except a loopback address for a client that runs on the user's own machine
	 * and has nowhere else to listen.  application_type native is how such a client says so, which
	 * is why the specification added it.
	 */
	public void checkRedirectUriShape(String uri, boolean isNative) throws ApplicationException {

		URI parsed;
		try {
			parsed = new URI(uri);
		} catch (Exception e) {
			throw new ApplicationException("Redirect uri is not a valid URI: " + uri);
		}
		if(parsed.getFragment() != null) {
			throw new ApplicationException("Redirect uri must not contain a fragment");
		}

		String scheme = parsed.getScheme() == null ? "" : parsed.getScheme().toLowerCase();
		String host = parsed.getHost() == null ? "" : parsed.getHost().toLowerCase();

		if("https".equals(scheme)) {
			return;
		}
		if("http".equals(scheme) && ("localhost".equals(host) || "127.0.0.1".equals(host) || "[::1]".equals(host))) {
			return;
		}
		/*
		 * A private scheme, com.example.app:/callback, is how a desktop or mobile client is handed
		 * back control.  Only accepted from a client that declared itself native.
		 */
		if(isNative && scheme.length() > 0 && !"http".equals(scheme)) {
			return;
		}
		throw new ApplicationException("Redirect uri must use https, or loopback for a native client: " + uri);
	}

	/* --------------------------------------------------------------- consent */

	public String getConsentedScope(Connection sd, int uId, String clientId) {
		try (PreparedStatement pstmt = sd.prepareStatement(
				"select scope from oauth_consent where u_id = ? and client_id = ?")) {
			pstmt.setInt(1, uId);
			pstmt.setString(2, clientId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return rs.getString("scope");
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Reading oauth consent", e);
		}
		return null;
	}

	/*
	 * Remember what was approved so a returning client is not a fresh decision every time.
	 * smap:access is deliberately never remembered: granting the ability to change who can reach
	 * what should be a decision taken each time it is made, not one inherited from last month.
	 */
	public void recordConsent(Connection sd, int uId, String clientId, String scope) throws SQLException {

		ArrayList<String> keep = new ArrayList<>();
		for(String s : MCPScope.parse(scope)) {
			if(!MCPScope.isAlwaysReconsented(s)) {
				keep.add(s);
			}
		}
		String remembered = MCPScope.join(keep);

		String sql = "insert into oauth_consent (u_id, client_id, scope, updated) values (?, ?, ?, now()) "
				+ "on conflict (u_id, client_id) do update set scope = excluded.scope, updated = now()";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, uId);
			pstmt.setString(2, clientId);
			pstmt.setString(3, remembered);
			pstmt.executeUpdate();
		}
	}

	public void forgetConsent(Connection sd, int uId, String clientId) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement(
				"delete from oauth_consent where u_id = ? and client_id = ?")) {
			pstmt.setInt(1, uId);
			pstmt.setString(2, clientId);
			pstmt.executeUpdate();
		}
	}

	/* ------------------------------------------------------ authorization codes */

	/*
	 * Issue a one time code.  Short lived because it only has to survive a redirect back to the
	 * client, and it travels through the user's browser where it can be logged or shoulder read.
	 */
	public String issueCode(Connection sd, String clientId, int uId, int oId, String scope,
			String resource, String redirectUri, String codeChallenge, String codeChallengeMethod)
			throws SQLException {

		String code = randomValue();

		String sql = "insert into oauth_grant (code_hash, client_id, u_id, o_id, scope, resource, "
				+ "redirect_uri, code_challenge, code_challenge_method, created, expires) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now() + interval '"
				+ CODE_TTL_SECONDS + " seconds')";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, ApiTokenManager.hash(code));
			pstmt.setString(2, clientId);
			pstmt.setInt(3, uId);
			pstmt.setInt(4, oId);
			pstmt.setString(5, scope);
			pstmt.setString(6, resource);
			pstmt.setString(7, redirectUri);
			pstmt.setString(8, codeChallenge);
			pstmt.setString(9, codeChallengeMethod);
			pstmt.executeUpdate();
		}

		try (PreparedStatement pstmt = sd.prepareStatement(
				"update oauth_client set last_grant = now() where client_id = ?")) {
			pstmt.setString(1, clientId);
			pstmt.executeUpdate();
		}

		return code;
	}

	/*
	 * Redeem a code exactly once.  Returns null for anything that is not a live code for this
	 * client, this redirect uri and this verifier.
	 */
	public Grant redeemCode(Connection sd, String code, String clientId, String redirectUri,
			String codeVerifier) throws SQLException {

		String sql = "select id, client_id, u_id, o_id, scope, resource, redirect_uri, "
				+ "code_challenge, code_challenge_method "
				+ "from oauth_grant "
				+ "where code_hash = ? and consumed is null and expires > now()";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, ApiTokenManager.hash(code.trim()));
			ResultSet rs = pstmt.executeQuery();
			if(!rs.next()) {
				return null;
			}

			int id = rs.getInt("id");
			if(!rs.getString("client_id").equals(clientId)) {
				log.warning("Authorization code presented by the wrong client");
				return null;
			}
			if(redirectUri != null && !redirectUri.equals(rs.getString("redirect_uri"))) {
				log.warning("Authorization code redeemed against a different redirect uri");
				return null;
			}
			if(!verifyPkce(rs.getString("code_challenge"), rs.getString("code_challenge_method"), codeVerifier)) {
				log.warning("PKCE verification failed");
				return null;
			}

			// Single use.  Only the call that consumes it gets the grant
			try (PreparedStatement pstmtUse = sd.prepareStatement(
					"update oauth_grant set consumed = now() where id = ? and consumed is null")) {
				pstmtUse.setInt(1, id);
				if(pstmtUse.executeUpdate() == 0) {
					return null;
				}
			}

			Grant g = new Grant();
			g.clientId = rs.getString("client_id");
			g.uId = rs.getInt("u_id");
			g.oId = rs.getInt("o_id");
			g.scope = rs.getString("scope");
			g.resource = rs.getString("resource");
			g.redirectUri = rs.getString("redirect_uri");
			return g;
		}
	}

	/*
	 * S256 only.  The plain method exists in the specification for clients that cannot hash, and
	 * offers no protection against an intercepted authorization request.
	 */
	private boolean verifyPkce(String challenge, String method, String verifier) {

		if(challenge == null || verifier == null) {
			return false;
		}
		if(!"S256".equals(method)) {
			return false;
		}
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashed = digest.digest(verifier.getBytes(StandardCharsets.US_ASCII));
			String computed = Base64.getUrlEncoder().withoutPadding().encodeToString(hashed);
			return MessageDigest.isEqual(
					computed.getBytes(StandardCharsets.US_ASCII),
					challenge.getBytes(StandardCharsets.US_ASCII));
		} catch (Exception e) {
			log.log(Level.SEVERE, "Verifying PKCE", e);
			return false;
		}
	}

	public int reapExpiredCodes(Connection sd) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement(
				"delete from oauth_grant where expires < now() - interval '1 day'")) {
			return pstmt.executeUpdate();
		}
	}

	/* --------------------------------------------------------------- helpers */

	private String randomValue() {
		byte[] bytes = new byte[CODE_BYTES];
		random.nextBytes(bytes);
		return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
	}

	private static String optString(JsonObject o, String name) {
		JsonElement e = o.get(name);
		return e == null || e.isJsonNull() ? null : e.getAsString();
	}

	private static ArrayList<String> optArray(JsonObject o, String name) {
		ArrayList<String> out = new ArrayList<>();
		JsonElement e = o.get(name);
		if(e != null && e.isJsonArray()) {
			JsonArray a = e.getAsJsonArray();
			for(int i = 0; i < a.size(); i++) {
				out.add(a.get(i).getAsString());
			}
		}
		return out;
	}

	private static ArrayList<String> fromJsonArray(String json) {
		ArrayList<String> out = new ArrayList<>();
		if(json != null) {
			try {
				JsonArray a = JsonParser.parseString(json).getAsJsonArray();
				for(int i = 0; i < a.size(); i++) {
					out.add(a.get(i).getAsString());
				}
			} catch (Exception e) {
				// A malformed list is an empty list, which refuses every redirect uri
			}
		}
		return out;
	}
}
