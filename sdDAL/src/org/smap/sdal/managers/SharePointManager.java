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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.SharePointField;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Manages communication with SharePoint via the SharePoint REST API.
 *
 * Supported authentication modes (sharepoint_auth_type):
 *   "s2s"  — S2S high-trust app-only. A signed RS256 JWT is sent as Bearer token.
 *             Requires sharepoint_client_id, sharepoint_realm, sharepoint_cert_pem.
 *   "ntlm" — Windows NTLM/Negotiate. Java's built-in HttpURLConnection NTLM support
 *             is activated via java.net.Authenticator.
 *             Requires sharepoint_username, sharepoint_password, sharepoint_domain.
 */
public class SharePointManager {

	private static Logger log = Logger.getLogger(SharePointManager.class.getName());

	// SharePoint's well-known app principal ID
	private static final String SP_PRINCIPAL = "00000003-0000-0ff1-ce00-000000000000";

	private static final int CONNECT_TIMEOUT_MS = 15_000;
	private static final int READ_TIMEOUT_MS    = 30_000;

	/*
	 * Discover the SharePoint farm realm GUID without credentials.
	 *
	 * Hits a protected SP endpoint with no auth token. SharePoint responds with
	 * HTTP 401 and includes the realm in the WWW-Authenticate header:
	 *   Bearer realm="<guid>",client_id="...",trusted_issuers="..."
	 * This is the standard no-auth discovery approach and works regardless of
	 * whether the SP.OAuth.NTRealm API endpoint is publicly accessible.
	 */
	public static String getRealm(String serverUrl) throws Exception {
		// Accept any URL the user has entered — extract only scheme+host+port
		java.net.URL parsed = new java.net.URL(serverUrl);
		String base = parsed.getProtocol() + "://" + parsed.getHost()
				+ (parsed.getPort() > 0 ? ":" + parsed.getPort() : "");
		String url  = base + "/_vti_bin/client.svc";

		HttpURLConnection conn = openConnection(url, "GET", null, false);
		int status = conn.getResponseCode();
		readResponse(conn);  // consume body to free the connection

		// SharePoint may return multiple WWW-Authenticate headers (e.g. NTLM + Bearer).
		// getHeaderField() only returns the last value, so scan all header fields.
		Pattern realmPattern = Pattern.compile("realm=\"([0-9a-fA-F\\-]{36})\"");
		for (Map.Entry<String, java.util.List<String>> entry : conn.getHeaderFields().entrySet()) {
			if ("WWW-Authenticate".equalsIgnoreCase(entry.getKey())) {
				for (String val : entry.getValue()) {
					if (val != null) {
						Matcher m = realmPattern.matcher(val);
						if (m.find()) {
							return m.group(1);
						}
					}
				}
			}
		}

		// Check whether the response used NTLM/Negotiate — if so, OAuth is not yet
		// configured on this SharePoint server and the realm must be obtained manually.
		boolean ntlmOnly = false;
		for (Map.Entry<String, java.util.List<String>> entry : conn.getHeaderFields().entrySet()) {
			if ("WWW-Authenticate".equalsIgnoreCase(entry.getKey())) {
				for (String val : entry.getValue()) {
					if (val != null && (val.startsWith("NTLM") || val.startsWith("Negotiate"))) {
						ntlmOnly = true;
					}
				}
			}
		}
		if (ntlmOnly) {
			throw new Exception("SharePoint at " + base + " is using Windows (NTLM/Negotiate) authentication only. "
					+ "OAuth/S2S has not been configured, so the realm cannot be discovered automatically. "
					+ "Obtain the realm GUID manually by running on the SharePoint server: "
					+ "Get-SPFarm | Select Id");
		}
		throw new Exception("Could not discover realm from " + base
				+ " (HTTP " + status + "). "
				+ "Check the URL is correct and the server is reachable.");
	}

	/*
	 * Build and return a high-trust app-only access token (signed JWT).
	 * The returned string is used directly as a Bearer token.
	 */
	public static String getAccessToken(ServerData sd) throws Exception {
		RSAPrivateKey privateKey = parsePemPrivateKey(sd.sharepoint_cert_pem);

		String host = new URL(sd.sharepoint_url).getHost();
		String aud  = SP_PRINCIPAL + "/" + host + "@" + sd.sharepoint_realm;
		String iss  = sd.sharepoint_client_id + "@" + sd.sharepoint_realm;

		long now = System.currentTimeMillis() / 1000L;

		String appCtx = "{\"CacheKey\":\"" + UUID.randomUUID() + "\","
				+ "\"SecurityTokenServiceUri\":\"https://" + host
				+ "/_forms/default.aspx?wa=wsignin1.0\"}";

		JWTClaimsSet claims = new JWTClaimsSet.Builder()
				.audience(aud)
				.issuer(iss)
				.notBeforeTime(new Date(now * 1000L))
				.expirationTime(new Date((now + 43200L) * 1000L))
				.claim("nameid", iss)
				.claim("appctxsender", iss)
				.claim("appctx", appCtx)
				.build();

		JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
				.type(JOSEObjectType.JWT)
				.build();

		SignedJWT jwt = new SignedJWT(header, claims);
		jwt.sign(new RSASSASigner(privateKey));
		return jwt.serialize();
	}

	/*
	 * Set up authentication for an API call.
	 * For S2S: returns a Bearer token string.
	 * For NTLM: installs a java.net.Authenticator scoped to the SP host and returns null.
	 *           Java's HttpURLConnection handles the NTLM handshake automatically.
	 */
	private static String authenticate(ServerData sd) throws Exception {
		if ("ntlm".equalsIgnoreCase(sd.sharepoint_auth_type)) {
			final String spHost   = new java.net.URL(sd.sharepoint_url).getHost();
			final String username = sd.sharepoint_username != null ? sd.sharepoint_username : "";
			final String password = sd.sharepoint_password != null ? sd.sharepoint_password : "";
			final String domain   = sd.sharepoint_domain   != null ? sd.sharepoint_domain   : "";
			// Prefix domain to username if provided (NTLM format: domain\\user)
			final String fullUser = domain.isEmpty() ? username : domain + "\\" + username;
			java.net.Authenticator.setDefault(new java.net.Authenticator() {
				@Override
				protected java.net.PasswordAuthentication getPasswordAuthentication() {
					// Only respond to the configured SharePoint host
					if (getRequestingHost() != null && getRequestingHost().equalsIgnoreCase(spHost)) {
						return new java.net.PasswordAuthentication(fullUser, password.toCharArray());
					}
					return null;
				}
			});
			return null;  // NTLM — no Bearer token
		}
		return getAccessToken(sd);  // S2S — return JWT
	}

	/*
	 * Return the titles of all non-hidden SharePoint lists.
	 * Used to populate the list-title dropdown on the resources page.
	 */
	public static List<String> getAvailableLists(ServerData sd) throws Exception {
		String token = authenticate(sd);
		String url   = stripTrailingSlash(sd.sharepoint_url)
				+ "/_api/web/lists?$select=Title&$filter=Hidden%20eq%20false";

		List<String> titles = new ArrayList<>();

		try {
			String body = get(url, token);
			
			JsonArray items = JsonParser.parseString(body)
					.getAsJsonObject()
					.getAsJsonObject("d")
					.getAsJsonArray("results");
	
			for (JsonElement el : items) {
				titles.add(el.getAsJsonObject().get("Title").getAsString());
			}
		} catch (Exception e) {
			log.info("Error getting sharepoint lists from: " + url);
			log.log(Level.SEVERE, e.getMessage(), e);
			throw e;
		}
		return titles;
	}

	/*
	 * Return the non-hidden, non-read-only fields of a SharePoint list.
	 * Used to populate the column-name dropdown in the notifications UI.
	 */
	public static List<SharePointField> getListFields(ServerData sd, String listTitle) throws Exception {
		String token = authenticate(sd);
		String url   = stripTrailingSlash(sd.sharepoint_url)
				+ "/_api/web/lists/getbytitle('" + encodeTitle(listTitle) + "')/fields"
				+ "?$select=Title,InternalName"
				+ "&$filter=Hidden%20eq%20false%20and%20ReadOnlyField%20eq%20false";

		String body = get(url, token);
		List<SharePointField> fields = new ArrayList<>();

		JsonArray items = JsonParser.parseString(body)
				.getAsJsonObject()
				.getAsJsonObject("d")
				.getAsJsonArray("results");

		for (JsonElement el : items) {
			JsonObject item = el.getAsJsonObject();
			SharePointField f = new SharePointField();
			f.internalName = item.get("InternalName").getAsString();
			f.displayName  = item.get("Title").getAsString();
			fields.add(f);
		}
		return fields;
	}

	/*
	 * Insert a new row into a SharePoint list.
	 */
	public static void postListItem(ServerData sd, String listTitle,
			Map<String, Object> fields) throws Exception {
		String token      = authenticate(sd);
		String entityType = getListEntityType(sd, listTitle, token);
		String url        = stripTrailingSlash(sd.sharepoint_url)
				+ "/_api/web/lists/getbytitle('" + encodeTitle(listTitle) + "')/items";

		Map<String, Object> body = new LinkedHashMap<>(fields);
		body.put("__metadata", Map.of("type", entityType));

		postJson(url, token, new Gson().toJson(body), "POST", null);
		log.info("SharePoint: inserted item into list '" + listTitle + "'");
	}

	/*
	 * Update an existing row in a SharePoint list.
	 * Finds the row where matchColumn equals matchValue, then applies fields.
	 * Throws if no matching row is found.
	 */
	public static void updateListItem(ServerData sd, String listTitle,
			String matchColumn, String matchValue,
			Map<String, Object> fields) throws Exception {
		String token = authenticate(sd);

		// Find the item by filter
		String filterUrl = stripTrailingSlash(sd.sharepoint_url)
				+ "/_api/web/lists/getbytitle('" + encodeTitle(listTitle) + "')/items"
				+ "?$filter=" + URLEncoder.encode(matchColumn + " eq '" + matchValue + "'",
						StandardCharsets.UTF_8)
				+ "&$select=Id&$top=1";

		String response = get(filterUrl, token);
		JsonArray results = JsonParser.parseString(response)
				.getAsJsonObject()
				.getAsJsonObject("d")
				.getAsJsonArray("results");

		if (results.size() == 0) {
			throw new Exception("SharePoint update: no item in '" + listTitle
					+ "' where " + matchColumn + " = '" + matchValue + "'");
		}
		int itemId = results.get(0).getAsJsonObject().get("Id").getAsInt();

		String itemUrl = stripTrailingSlash(sd.sharepoint_url)
				+ "/_api/web/lists/getbytitle('" + encodeTitle(listTitle) + "')/items(" + itemId + ")";

		String entityType = getListEntityType(sd, listTitle, token);
		Map<String, Object> body = new LinkedHashMap<>(fields);
		body.put("__metadata", Map.of("type", entityType));

		// MERGE = partial update; IF-MATCH: * means update regardless of ETag
		postJson(itemUrl, token, new Gson().toJson(body), "MERGE", "IF-MATCH: *");
		log.info("SharePoint: updated item " + itemId + " in list '" + listTitle + "'");
	}

	/*
	 * Fetch all items from a SharePoint list up to maxRows.
	 * Returns a list of rows; each row is a map of column name to string value.
	 * Used by the background sync job to populate the local CSV cache.
	 */
	public static List<Map<String, String>> getListItems(ServerData sd,
			String listTitle, int maxRows) throws Exception {
		String token = authenticate(sd);
		String url   = stripTrailingSlash(sd.sharepoint_url)
				+ "/_api/web/lists/getbytitle('" + encodeTitle(listTitle) + "')/items"
				+ "?$top=" + maxRows;

		String response = get(url, token);
		JsonArray items = JsonParser.parseString(response)
				.getAsJsonObject()
				.getAsJsonObject("d")
				.getAsJsonArray("results");

		List<Map<String, String>> rows = new ArrayList<>();
		for (JsonElement el : items) {
			JsonObject item = el.getAsJsonObject();
			Map<String, String> row = new LinkedHashMap<>();
			for (Map.Entry<String, JsonElement> entry : item.entrySet()) {
				String key = entry.getKey();
				if ("__metadata".equals(key)) continue;
				JsonElement val = entry.getValue();
				// Skip nested objects/arrays (e.g. lookup fields, attachments)
				if (val.isJsonObject() || val.isJsonArray()) continue;
				row.put(key, val.isJsonNull() ? "" : val.getAsString());
			}
			rows.add(row);
		}
		return rows;
	}

	// -------------------------------------------------------------------------
	// Private helpers
	// -------------------------------------------------------------------------

	private static String getListEntityType(ServerData sd, String listTitle,
			String token) throws Exception {
		String url = stripTrailingSlash(sd.sharepoint_url)
				+ "/_api/web/lists/getbytitle('" + encodeTitle(listTitle) + "')"
				+ "?$select=ListItemEntityTypeFullName";
		String body = get(url, token);
		return JsonParser.parseString(body)
				.getAsJsonObject()
				.getAsJsonObject("d")
				.get("ListItemEntityTypeFullName")
				.getAsString();
	}

	/*
	 * Parse a PKCS8 PEM private key.
	 * The PEM must use -----BEGIN PRIVATE KEY----- headers (PKCS8 unencrypted).
	 */
	private static RSAPrivateKey parsePemPrivateKey(String pem) throws Exception {
		String stripped = pem
				.replace("-----BEGIN PRIVATE KEY-----", "")
				.replace("-----END PRIVATE KEY-----", "")
				.replaceAll("\\s+", "");
		byte[] keyBytes = Base64.getDecoder().decode(stripped);
		PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
		return (RSAPrivateKey) KeyFactory.getInstance("RSA").generatePrivate(spec);
	}

	private static String get(String url, String token) throws Exception {
		HttpURLConnection conn = openConnection(url, "GET", token, false);
		int status = conn.getResponseCode();
		String body = readResponse(conn);
		if (status < 200 || status >= 300) {
			throw new Exception("SharePoint GET failed, HTTP " + status + ": " + body);
		}
		return body;
	}

	private static void postJson(String url, String token, String jsonBody,
			String httpMethod, String extraHeader) throws Exception {
		HttpURLConnection conn = openConnection(url, "POST", token, true);
		// httpMethod may be "POST" or "MERGE" (tunnelled via X-HTTP-Method)
		if (!"POST".equals(httpMethod)) {
			conn.setRequestProperty("X-HTTP-Method", httpMethod);
		}
		if (extraHeader != null) {
			String[] parts = extraHeader.split(": ", 2);
			conn.setRequestProperty(parts[0], parts[1]);
		}
		conn.setRequestProperty("Content-Type", "application/json;odata=verbose");

		byte[] input = jsonBody.getBytes(StandardCharsets.UTF_8);
		conn.setRequestProperty("Content-Length", String.valueOf(input.length));
		try (OutputStream os = conn.getOutputStream()) {
			os.write(input);
		}

		int status = conn.getResponseCode();
		// 201 Created (insert) or 204 No Content (update) are both success
		if (status < 200 || status >= 300) {
			String body = readResponse(conn);
			throw new Exception("SharePoint " + httpMethod + " failed, HTTP " + status + ": " + body);
		}
	}

	private static HttpURLConnection openConnection(String url, String method,
			String token, boolean doOutput) throws Exception {
		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		conn.setRequestMethod(method);
		conn.setRequestProperty("Accept", "application/json;odata=verbose");
		if (token != null) {
			conn.setRequestProperty("Authorization", "Bearer " + token);
		}
		conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
		conn.setReadTimeout(READ_TIMEOUT_MS);
		conn.setDoOutput(doOutput);
		return conn;
	}

	private static String readResponse(HttpURLConnection conn) throws IOException {
		InputStream is;
		try {
			is = conn.getInputStream();
		} catch (IOException e) {
			is = conn.getErrorStream();
			if (is == null) return "";
		}
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(is, StandardCharsets.UTF_8))) {
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line).append('\n');
			}
			return sb.toString().trim();
		}
	}

	private static String stripTrailingSlash(String url) {
		return url == null ? "" : url.replaceAll("/+$", "");
	}

	private static String encodeTitle(String title) {
		if (title == null) return "";
		// Escape single quotes for OData string literals, then URL-encode for the path segment
		String escaped = title.replace("'", "''");
		try {
			return java.net.URLEncoder.encode(escaped, "UTF-8").replace("+", "%20");
		} catch (java.io.UnsupportedEncodingException e) {
			return escaped;
		}
	}
}
