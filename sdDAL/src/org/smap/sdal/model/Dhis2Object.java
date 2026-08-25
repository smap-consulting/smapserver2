package org.smap.sdal.model;

/*
 * A DHIS2 data set or program, cached for the mapping screens
 *
 * Only the fields a picker needs are held as columns.  The detail is kept as the JSON DHIS2
 * returned, because nothing queries inside it and modelling their structure again here would
 * only be something else to keep in step
 */
public class Dhis2Object {

	public static final String TYPE_DATASET = "dataset";
	public static final String TYPE_PROGRAM = "program";

	public String object_type;
	public String uid;
	public String code;
	public String name;
	public String last_fetched;		// Null if only the summary has been listed
}
