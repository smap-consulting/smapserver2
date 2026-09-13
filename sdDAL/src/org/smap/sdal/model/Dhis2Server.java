package org.smap.sdal.model;

/*
 * A DHIS2 instance that an organisation can exchange data with
 * Held per organisation, not per server, so one tenant cannot write into another tenant's DHIS2
 */
public class Dhis2Server {
	public int id;
	public int o_id;
	public String label;			// Shown when choosing a connection
	public String base_url;			// Root of the instance, no trailing /api
	public String api_token;		// Only ever set on the way in.  Never returned to the client
	public String api_version;		// Optional version pin, eg "42"
	public String last_tested;
	public String last_test_result;
	public boolean enabled;

	public boolean token_set;		// Tells the UI a token is held without disclosing it

	/*
	 * The API base, honouring the version pin if one is set
	 * DHIS2 accepts both /api/... and /api/<version>/...
	 */
	public String getApiUrl() {
		StringBuilder url = new StringBuilder(base_url == null ? "" : base_url.trim());
		while(url.length() > 0 && url.charAt(url.length() - 1) == '/') {
			url.deleteCharAt(url.length() - 1);
		}
		url.append("/api");
		if(api_version != null && api_version.trim().length() > 0) {
			url.append("/").append(api_version.trim());
		}
		return url.toString();
	}
}
