package org.smap.sdal.model;

/*
 * FormLink Class
 * Used for drill down from parent to child forms
 */
public class FormLink {
	public String name;
	public String parentName;		// The name of the form that contains the data used by a reference form	

	public FormLink(String n, String pn) {
		name = n;
		parentName = pn;
	}
}
