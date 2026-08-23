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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.model.Dhis2ConnectionTest;
import org.smap.sdal.model.Dhis2Server;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/*
 * Talks to a DHIS2 instance
 *
 * Only the calls needed to establish and verify a connection are here.  Reference data sync and
 * data export build on the request helpers rather than repeating them
 */
public class Dhis2Manager {

	private static Logger log = Logger.getLogger(Dhis2Manager.class.getName());

	private static final int CONNECT_TIMEOUT = 15000;
	private static final int READ_TIMEOUT = 120000;		// Metadata responses can be large

	// DHIS2 authority required to write aggregate data values.  ALL covers everything
	private static final String AUTH_ALL = "ALL";
	private static final String AUTH_DATAVALUE_ADD = "F_DATAVALUE_ADD";

	/*
	 * Test a connection and report what it will and will not be able to do
	 *
	 * Deliberately does not throw.  A failed test is an answer, not an error, and the caller wants
	 * to show the reason rather than a stack trace
	 */
	public Dhis2ConnectionTest test(Dhis2Server server) {

		Dhis2ConnectionTest result = new Dhis2ConnectionTest();

		/*
		 * 1. Is it there, and does it accept the token
		 */
		try {
			JsonObject info = getJsonObject(server, "/system/info");
			result.reachable = true;
			result.authenticated = true;
			result.version = asString(info, "version");
			result.revision = asString(info, "revision");
			result.system_name = asString(info, "systemName");

			JsonElement dbInfo = info.get("databaseInfo");
			if(dbInfo != null && dbInfo.isJsonObject()) {
				result.database_name = asString(dbInfo.getAsJsonObject(), "name");
			}
		} catch (Dhis2AuthException e) {
			result.reachable = true;
			result.authenticated = false;
			result.error = e.getMessage();
			// Logged because a failed test is the thing someone will be trying to diagnose
			log.info("DHIS2 test rejected for " + server.base_url + ": " + e.getMessage());
			return result;
		} catch (Exception e) {
			result.error = e.getMessage();
			log.info("DHIS2 test failed for " + server.base_url + ": " + e.getMessage());
			return result;
		}

		/*
		 * 2. What can the user this token belongs to actually do
		 *
		 * A valid token is not enough.  Data capture is restricted to the org units assigned to the
		 * user, so a connection that passes step 1 can still reject every value we send
		 */
		try {
			JsonObject me = getJsonObject(server,
					"/me?fields=username,authorities,organisationUnits[id],dataViewOrganisationUnits[id]");

			result.username = asString(me, "username");

			JsonArray authorities = me.getAsJsonArray("authorities");
			if(authorities != null) {
				for(JsonElement a : authorities) {
					String auth = a.getAsString();
					if(AUTH_ALL.equals(auth)) {
						result.superuser = true;
					} else if(AUTH_DATAVALUE_ADD.equals(auth)) {
						result.can_add_data_values = true;
					}
				}
			}
			if(result.superuser) {
				result.can_add_data_values = true;
			}

			result.capture_org_units = size(me.getAsJsonArray("organisationUnits"));
			result.view_org_units = size(me.getAsJsonArray("dataViewOrganisationUnits"));

			if(result.capture_org_units == 0) {
				result.warnings.add("This DHIS2 user has no data capture organisation units. "
						+ "Data sent from Smap will be rejected until org units are assigned to the user.");
			}
			if(!result.can_add_data_values) {
				result.warnings.add("This DHIS2 user does not report the "
						+ AUTH_DATAVALUE_ADD + " authority. Exporting data values may be rejected.");
			}

		} catch (Exception e) {
			// The instance is reachable and the token works, so this is a warning not a failure
			log.log(Level.WARNING, "DHIS2 test: could not read /me: " + e.getMessage(), e);
			result.warnings.add("Could not read the user details from DHIS2: " + e.getMessage());
		}

		return result;
	}

	/*
	 * A summary line worth storing against the connection and showing in a list
	 */
	public String summarise(Dhis2ConnectionTest t) {
		if(t == null) {
			return "Not tested";
		}
		if(!t.reachable) {
			return "Not reachable: " + t.error;
		}
		if(!t.authenticated) {
			return "Token rejected: " + t.error;
		}
		StringBuilder sb = new StringBuilder("DHIS2 ");
		sb.append(t.version == null ? "?" : t.version);
		if(t.database_name != null) {
			sb.append(", database ").append(t.database_name);
		}
		if(t.username != null) {
			sb.append(", user ").append(t.username);
		}
		if(!t.warnings.isEmpty()) {
			sb.append(", ").append(t.warnings.size()).append(" warning")
				.append(t.warnings.size() == 1 ? "" : "s");
		}
		return sb.toString();
	}

	/*
	 * GET a path below /api and parse the response as a JSON object
	 * The path starts with a slash, for example "/system/info"
	 */
	public JsonObject getJsonObject(Dhis2Server server, String path) throws Exception {
		String body = get(server, path);
		JsonElement parsed = JsonParser.parseString(body);
		if(!parsed.isJsonObject()) {
			throw new Exception("Unexpected response from DHIS2 for " + path);
		}
		return parsed.getAsJsonObject();
	}

	/*
	 * GET a path below /api and return the body
	 */
	public String get(Dhis2Server server, String path) throws Exception {

		String url = server.getApiUrl() + path;
		HttpURLConnection conn = null;

		try {
			conn = open(server, url, "GET");
			int status = conn.getResponseCode();

			if(status < 200 || status > 299) {
				/*
				 * Read the error stream once, a second read returns nothing.  DHIS2 usually explains
				 * a rejection in the body, and a token refused for an IP restriction looks exactly
				 * like a token that is simply wrong unless that explanation is passed on
				 */
				String detail = readError(conn);
				String suffix = (detail == null || detail.isEmpty()) ? "" : ": " + detail;

				if(status == HttpURLConnection.HTTP_UNAUTHORIZED || status == HttpURLConnection.HTTP_FORBIDDEN) {
					throw new Dhis2AuthException("DHIS2 rejected the token (" + status + ")" + suffix);
				}
				throw new Exception("DHIS2 returned " + status + " for " + path + suffix);
			}
			return read(conn.getInputStream());

		} finally {
			if(conn != null) {
				try { conn.disconnect(); } catch(Exception e) {}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private HttpURLConnection open(Dhis2Server server, String url, String method) throws IOException {

		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		conn.setRequestMethod(method);
		conn.setConnectTimeout(CONNECT_TIMEOUT);
		conn.setReadTimeout(READ_TIMEOUT);
		conn.setRequestProperty("Accept", "application/json");

		if(server.api_token != null && server.api_token.trim().length() > 0) {
			// Personal access token.  Preferred over basic auth because the client can scope,
			// expire and revoke it without touching a user account
			conn.setRequestProperty("Authorization", "ApiToken " + server.api_token.trim());
		}

		return conn;
	}

	private String read(InputStream is) throws IOException {
		if(is == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
			String line;
			while((line = br.readLine()) != null) {
				sb.append(line);
			}
		}
		return sb.toString();
	}

	private String readError(HttpURLConnection conn) {
		try {
			String body = read(conn.getErrorStream());
			return body != null && body.length() > 500 ? body.substring(0, 500) : body;
		} catch (Exception e) {
			return null;
		}
	}

	private String asString(JsonObject o, String name) {
		if(o == null) {
			return null;
		}
		JsonElement e = o.get(name);
		return (e == null || e.isJsonNull() || !e.isJsonPrimitive()) ? null : e.getAsString();
	}

	private int size(JsonArray a) {
		return a == null ? 0 : a.size();
	}

	/*
	 * Distinguishes a rejected token from an unreachable server, the two get different advice
	 */
	public static class Dhis2AuthException extends Exception {
		private static final long serialVersionUID = 1L;
		public Dhis2AuthException(String msg) {
			super(msg);
		}
	}
}
