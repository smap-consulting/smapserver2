package org.smap.sdal.model;

/*
 * Used when managing hierarchies of task - assignments
 * This should replace Task Feature which combines task and assignment
 */
import java.sql.Timestamp;
import java.util.ArrayList;

public class TaskServerDefn {
	public int id;
	public String name;
	public int form_id;
	public String form_name;
	public Timestamp from;
	public Timestamp to;
	public String location_trigger;
	public boolean repeat;
	public String address;			// Text address
	public String guidance;			// Key value pairs representing an unstructured address
	public Double lat;				// Latitude
	public Double lon;				// Longitude
	public String initial_data;
	public String update_id;
	public String instance_id;
	
	public ArrayList<AssignmentServerDefn> assignments = new ArrayList<AssignmentServerDefn> ();
}
