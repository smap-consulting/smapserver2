package org.smap.sdal.model;

import java.sql.Timestamp;
import java.util.ArrayList;

public class TaskProperties {
	public int id;
	public int a_id;					// The assignment ID which is the true identifier of the task (ie the task is the task defn + assignment)
	public String name;
	public String pid;				// Project id
	public String url;
	public int form_id;
	public String form_ident;
	public String form_name;
	public boolean blocked;
	public String form_version;
	public int assignee;
	public String assignee_type;				// user || role
	public String assignee_ident;
	public boolean generate_user;			// Create a temporary user ident
	public String assignee_name;
	public String initial_data;
	public String update_id;					// Unique identifier of record to be updated
	public String instance_id;				// Unique identifier of source record
	public Timestamp from;
	public Timestamp to;
	public String location_trigger;
	public boolean repeat;
	public int repeat_count;
	public String address;			// Text address
	public String guidance;			// Key value pairs representing an unstructured address
	public String email;
	public String location;			// WKT version of geometry, duplicates data in geometry, used when updating location
	
	public String status;
}
