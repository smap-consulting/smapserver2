package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * An OAuth client.
 *
 * Either read from a Client ID Metadata Document at authorize time, in which case client_id is the
 * https URL the document came from, or created by dynamic registration, in which case it is an
 * opaque value this server minted.  There is no organisation on a client: registration happens
 * before anyone has consented, so there is no user yet and therefore no organisation.
 */
public class OAuthClient {

	public int id;
	public String client_id;
	public String source;					// "cimd" || "dcr"
	public String client_name;
	public ArrayList<String> redirect_uris = new ArrayList<>();
	public ArrayList<String> grant_types = new ArrayList<>();
	public String token_endpoint_auth_method;
	public String application_type;			// "native" allows a loopback redirect
	public String scope;
	public String software_id;
	public String status;					// "active" || "pending" || "disabled"
	public String registration_ip;

	/*
	 * Returned once, by registration, and never stored in the clear
	 */
	public transient String client_secret;

	/*
	 * Only ever read back from the database, to check a confidential client's secret
	 */
	public transient String secretHash;

	/*
	 * How old the cached metadata document is.  Long.MAX_VALUE when it has never been read.
	 */
	public transient long metadataAgeMs = Long.MAX_VALUE;

	public boolean fetchedWithin(long ms) {
		return metadataAgeMs < ms;
	}

	public boolean isPublic() {
		return token_endpoint_auth_method == null || "none".equals(token_endpoint_auth_method);
	}

	public boolean isNative() {
		return "native".equals(application_type);
	}

	/*
	 * Whether the client registered itself, which the consent screen says out loud.  A self
	 * registered client chose its own name, so the name is not evidence of anything.
	 */
	public boolean isSelfRegistered() {
		return !"preregistered".equals(source);
	}
}
