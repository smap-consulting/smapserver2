package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * The result of testing a DHIS2 connection
 *
 * More than a reachability check.  A token can be perfectly valid and the push still fail because
 * the DHIS2 user has no data capture org units, or lacks the authority to add data values.  That
 * is the first thing that goes wrong at a new client, so it is reported here rather than left to
 * be discovered during an export
 */
public class Dhis2ConnectionTest {

	public boolean reachable;			// The instance answered
	public boolean authenticated;		// The token was accepted

	public String version;				// eg "2.42.1"
	public String revision;
	public String system_name;
	public String database_name;

	public String username;				// The DHIS2 user the token belongs to
	public boolean superuser;			// Holds the ALL authority
	public boolean can_add_data_values;	// Holds ALL or F_DATAVALUE_ADD
	public int capture_org_units;		// Org units the user may capture data for
	public int view_org_units;			// Org units the user may read data for

	public String error;				// Set when reachable or authenticated is false

	/*
	 * Anything the user should act on before trying to export
	 * Warnings rather than failures: the authority names are checked against what this version of
	 * DHIS2 reports, and a name we do not recognise should not stop someone using the connection
	 */
	public ArrayList<String> warnings = new ArrayList<>();
}
