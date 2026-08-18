package org.smap.sdal.model;

import java.sql.Timestamp;

/*
 * One API or device token.
 *
 * value is set only in the response to the call that creates the token.  The server keeps
 * a sha256 of it and nothing else, so it can never be shown again.
 */
public class ApiToken {

	public int id;
	public String scope;			// api || app
	public String name;
	public String prefix;			// Leading characters of the value, enough to tell two tokens apart
	public Timestamp created;
	public String created_by;
	public Timestamp expires;		// Null means no expiry
	public Timestamp last_used;
	public Timestamp revoked;
	public String revoked_by;

	public String value;			// Only ever set once, when the token is created
}
