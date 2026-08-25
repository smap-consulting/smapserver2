package org.smap.sdal.model;

/*
 * One value in an export: where it comes from in Smap and where it goes in DHIS2
 *
 * Bound on question name rather than question id.  Replacing a survey from an XLSForm creates
 * new questions with new ids, and the old ids belong to what is then a deleted survey, so a
 * mapping keyed on id would break on every replacement.  Names are what roles, relevance and
 * cross survey matching already rely on
 */
public class Dhis2ExportItem {

	public static final String AGG_ONE = "one";		// One submission is one value, no grouping
	public static final String AGG_COUNT = "count";	// Number of submissions in the period
	public static final String AGG_SUM = "sum";		// Total of a numeric question

	public int id;
	public int e_id;
	public String question_name;			// Null when counting records rather than a question
	public String aggregation;
	public String data_element;				// DHIS2 data element code
	public String category_option_combo;	// Optional, needed where the element is disaggregated
	public int seq;
}
