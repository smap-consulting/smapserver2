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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
	 * The organisation unit hierarchy, flattened into rows ready for a CSV lookup
	 *
	 * DHIS2 gives each org unit a "path", a concatenation of ancestor uids.  A cascading select on
	 * a device cannot walk parents, so the hierarchy is flattened here into a pair of columns per
	 * level.  A form then filters on an ordinary column, level2_code for a district say, with no
	 * recursion and nothing for the device to work out.
	 *
	 * Organisation unit group sets are a second, non hierarchical axis: facility type, ownership,
	 * urban or rural.  Each becomes its own column so it can be used as a filter alongside the
	 * levels rather than instead of them.
	 *
	 * ouFilter limits the sync to a subtree.  An organisation working in one province has no use
	 * for a national hierarchy, and reducing the payload at source beats compressing it later
	 */
	public List<Map<String, String>> getOrgUnits(Dhis2Server server, String ouFilter) throws Exception {

		// Level names, so columns can carry the client's own words rather than level1, level2
		Map<Integer, String> levelNames = getOrgUnitLevelNames(server);

		// Which group set does each group belong to
		Map<String, String> groupToSet = new LinkedHashMap<>();
		LinkedHashSet<String> groupSetNames = new LinkedHashSet<>();
		loadGroupSets(server, groupToSet, groupSetNames);

		StringBuilder path = new StringBuilder("/organisationUnits.json?paging=false")
				.append("&fields=id,code,name,level,path,organisationUnitGroups[id,code,name]");
		if(ouFilter != null && ouFilter.trim().length() > 0) {
			// like rather than eq: path holds the whole ancestry, so this selects the subtree
			path.append("&filter=path:like:").append(encode(ouFilter.trim()));
		}

		JsonObject response = getJsonObject(server, path.toString());
		JsonArray units = response.getAsJsonArray("organisationUnits");
		if(units == null) {
			return new ArrayList<>();
		}

		/*
		 * Index every unit by uid first, so an ancestor can be resolved without another request.
		 * DHIS2 does not return the hierarchy in any useful order
		 */
		Map<String, JsonObject> byUid = new LinkedHashMap<>();
		int maxLevel = 0;
		for(JsonElement e : units) {
			JsonObject ou = e.getAsJsonObject();
			String uid = asString(ou, "id");
			if(uid != null) {
				byUid.put(uid, ou);
			}
			int level = asInt(ou, "level");
			if(level > maxLevel) {
				maxLevel = level;
			}
		}

		List<Map<String, String>> rows = new ArrayList<>();

		for(JsonElement e : units) {
			JsonObject ou = e.getAsJsonObject();

			// LinkedHashMap so the column order is stable between syncs
			Map<String, String> row = new LinkedHashMap<>();
			row.put("uid", nz(asString(ou, "id")));
			row.put("code", nz(asString(ou, "code")));
			row.put("name", nz(asString(ou, "name")));
			int level = asInt(ou, "level");
			row.put("level", level > 0 ? String.valueOf(level) : "");

			/*
			 * Ancestors, read from the path.  The last element is the unit itself, so a facility
			 * fills every level column down to its own
			 */
			String[] ancestors = splitPath(asString(ou, "path"));
			for(int l = 1; l <= maxLevel; l++) {
				String levelCode = "";
				String levelName = "";
				if(l <= ancestors.length) {
					JsonObject ancestor = byUid.get(ancestors[l - 1]);
					if(ancestor != null) {
						levelCode = nz(asString(ancestor, "code"));
						levelName = nz(asString(ancestor, "name"));
					}
				}
				row.put(levelColumn(levelNames, l) + "_code", levelCode);
				row.put(levelColumn(levelNames, l) + "_name", levelName);
			}

			// One column per group set, holding the group this unit belongs to within that set
			Map<String, String> setValues = new LinkedHashMap<>();
			JsonArray groups = ou.getAsJsonArray("organisationUnitGroups");
			if(groups != null) {
				for(JsonElement g : groups) {
					JsonObject group = g.getAsJsonObject();
					String setName = groupToSet.get(asString(group, "id"));
					if(setName == null) {
						continue;		// A group that belongs to no set is not a usable filter
					}
					String value = asString(group, "code");
					if(value == null) {
						value = nz(asString(group, "name"));
					}
					if(setValues.containsKey(setName)) {
						/*
						 * Group sets are meant to be mutually exclusive but DHIS2 does not always
						 * enforce it.  Keep the first and say so rather than choosing silently,
						 * it usually means the client's metadata needs attention
						 */
						log.warning("DHIS2 org unit " + asString(ou, "id")
								+ " is in more than one group of set " + setName
								+ ", keeping " + setValues.get(setName));
					} else {
						setValues.put(setName, value);
					}
				}
			}
			for(String setName : groupSetNames) {
				row.put("gs_" + cleanColumn(setName), nz(setValues.get(setName)));
			}

			rows.add(row);
		}

		return rows;
	}

	/*
	 * The option sets available on the instance, for choosing one to synchronise
	 * Returns uid, code and name only, the options themselves are fetched when it is synced
	 */
	public List<Map<String, String>> listOptionSets(Dhis2Server server) throws Exception {

		List<Map<String, String>> sets = new ArrayList<>();

		JsonObject response = getJsonObject(server,
				"/optionSets.json?paging=false&fields=id,code,name&order=name:asc");
		JsonArray items = response.getAsJsonArray("optionSets");
		if(items == null) {
			return sets;
		}

		for(JsonElement e : items) {
			JsonObject os = e.getAsJsonObject();
			Map<String, String> set = new LinkedHashMap<>();
			set.put("uid", nz(asString(os, "id")));
			set.put("code", nz(asString(os, "code")));
			set.put("name", nz(asString(os, "name")));
			sets.add(set);
		}

		return sets;
	}

	/*
	 * The options in one option set, as rows ready for a CSV lookup
	 *
	 * The value column is the DHIS2 code, because that is what DHIS2 expects back and it does not
	 * change when an option is relabelled.  The label column is the name
	 *
	 * The order column is called sortby because Smap orders choices by a column of that name
	 * without being asked.  It is zero padded so that a plain text sort puts 2 before 10
	 */
	public List<Map<String, String>> getOptionSetOptions(Dhis2Server server, String uid) throws Exception {

		if(uid == null || uid.trim().length() == 0) {
			throw new Exception("No DHIS2 option set has been chosen for this resource");
		}

		JsonObject response = getJsonObject(server, "/optionSets/" + encode(uid.trim())
				+ ".json?fields=id,code,name,options[id,code,name,sortOrder]");

		List<Map<String, String>> rows = new ArrayList<>();
		JsonArray options = response.getAsJsonArray("options");
		if(options == null) {
			return rows;
		}

		int fallbackOrder = 0;
		for(JsonElement e : options) {
			JsonObject o = e.getAsJsonObject();

			Map<String, String> row = new LinkedHashMap<>();
			row.put("code", nz(asString(o, "code")));
			row.put("name", nz(asString(o, "name")));
			row.put("uid", nz(asString(o, "id")));

			int order = asInt(o, "sortOrder");
			if(order <= 0) {
				order = ++fallbackOrder;		// Keep the order DHIS2 returned them in
			}
			row.put("sortby", String.format("%05d", order));

			rows.add(row);
		}

		return rows;
	}

	/*
	 * Level number to level name, so a column can be called district rather than level3
	 */
	private Map<Integer, String> getOrgUnitLevelNames(Dhis2Server server) {

		Map<Integer, String> names = new LinkedHashMap<>();
		try {
			JsonObject response = getJsonObject(server,
					"/organisationUnitLevels.json?paging=false&fields=level,name");
			JsonArray levels = response.getAsJsonArray("organisationUnitLevels");
			if(levels != null) {
				for(JsonElement e : levels) {
					JsonObject l = e.getAsJsonObject();
					int level = asInt(l, "level");
					if(level > 0) {
						names.put(level, asString(l, "name"));
					}
				}
			}
		} catch (Exception e) {
			// Not fatal.  The columns fall back to level1, level2 and so on
			log.log(Level.WARNING, "DHIS2: could not read org unit levels: " + e.getMessage(), e);
		}
		return names;
	}

	/*
	 * Group uid to the name of the group set it belongs to
	 */
	private void loadGroupSets(Dhis2Server server, Map<String, String> groupToSet,
			LinkedHashSet<String> groupSetNames) {

		try {
			JsonObject response = getJsonObject(server,
					"/organisationUnitGroupSets.json?paging=false"
					+ "&fields=id,code,name,organisationUnitGroups[id]");
			JsonArray sets = response.getAsJsonArray("organisationUnitGroupSets");
			if(sets == null) {
				return;
			}
			for(JsonElement e : sets) {
				JsonObject set = e.getAsJsonObject();
				String setName = asString(set, "code");
				if(setName == null) {
					setName = asString(set, "name");
				}
				if(setName == null) {
					continue;
				}
				groupSetNames.add(setName);
				JsonArray groups = set.getAsJsonArray("organisationUnitGroups");
				if(groups != null) {
					for(JsonElement g : groups) {
						String groupUid = asString(g.getAsJsonObject(), "id");
						if(groupUid != null) {
							groupToSet.put(groupUid, setName);
						}
					}
				}
			}
		} catch (Exception e) {
			// Not fatal.  The hierarchy is still usable without the group set columns
			log.log(Level.WARNING, "DHIS2: could not read org unit group sets: " + e.getMessage(), e);
		}
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

	private int asInt(JsonObject o, String name) {
		if(o == null) {
			return 0;
		}
		JsonElement e = o.get(name);
		if(e == null || e.isJsonNull() || !e.isJsonPrimitive()) {
			return 0;
		}
		try {
			return e.getAsInt();
		} catch (Exception ex) {
			return 0;
		}
	}

	private String nz(String s) {
		return s == null ? "" : s;
	}

	/*
	 * A DHIS2 path is "/uid/uid/uid", root first, ending with the unit itself
	 */
	private String[] splitPath(String path) {
		if(path == null || path.trim().length() == 0) {
			return new String[0];
		}
		String p = path.trim();
		if(p.startsWith("/")) {
			p = p.substring(1);
		}
		if(p.length() == 0) {
			return new String[0];
		}
		return p.split("/");
	}

	/*
	 * The column prefix for a hierarchy level, using the client's own level name where they have
	 * set one.  Falls back to level1, level2 and so on
	 */
	private String levelColumn(Map<Integer, String> levelNames, int level) {
		String name = levelNames.get(level);
		if(name == null || name.trim().length() == 0) {
			return "level" + level;
		}
		return cleanColumn(name);
	}

	/*
	 * Make a DHIS2 name safe to use as a CSV column and in a choice filter expression
	 */
	private String cleanColumn(String name) {
		String c = name.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_");
		c = c.replaceAll("^_+", "").replaceAll("_+$", "");
		if(c.length() == 0) {
			c = "col";
		}
		if(Character.isDigit(c.charAt(0))) {
			c = "c" + c;		// A column cannot start with a digit
		}
		return c;
	}

	private String encode(String value) {
		try {
			return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
		} catch (Exception e) {
			return value;
		}
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
