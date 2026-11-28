package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Response to a set of changes submitted to the server
 */
public class ChangeResponse {
	public int success;		// Number of sucess updates
	public int failed;		// Number of failed updates
	public int version;		// The new version of the survey
	public ArrayList<ChangeSet> changeSet;	
	public ArrayList<String> warnings = new ArrayList<> ();	// Server expressions in the bundle that the changes broke
	public ArrayList<String> refused = new ArrayList<> ();	// Problems in the form that stopped the changes being saved
}
