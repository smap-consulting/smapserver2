package org.smap.sdal.model;

/*
 * FormLink Class
 * Used for drill down from parent to child forms
 */
public class FormLink {
	public String name;
	public String parentName;		// The name of the form that contains the data used by a reference form	
	public String type;				// sub_form or survey
	public String surveyId;			// Keep as string
	public String keyQuestion;
	public FormLink(String n, String pn, String t, String s, String k) {
		name = n;
		parentName = pn;
		type = t;
		surveyId = s;
		keyQuestion = k;
	}
}
