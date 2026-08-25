package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * An export of a survey bundle's data into a DHIS2 data set
 *
 * Held at bundle level rather than per survey, because surveys in a bundle share data tables so
 * a question name means the same thing across them.  Across bundles it does not, which is why
 * this is not global
 */
public class Dhis2Export {

	public int id;
	public int o_id;
	public String group_survey_ident;		// The bundle
	public String dataset_uid;
	public String dataset_name;
	public String period_type;				// Monthly | Weekly | Quarterly | Yearly | Daily
	public String period_question;			// Question name, null uses the upload time
	public String orgunit_question;			// Question name holding the DHIS2 org unit code
	public boolean enabled;
	public String last_export;
	public String last_export_result;

	public ArrayList<Dhis2ExportItem> items = new ArrayList<>();
}
