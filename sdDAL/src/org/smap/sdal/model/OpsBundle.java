package org.smap.sdal.model;

/*
 * Open-case breakdown for one case bundle (group survey), offered as an
 * alternative to the per-unit (role) rollup on the Operations Overview.
 */
public class OpsBundle {
	public String bundleIdent;	// group_survey_ident
	public String name;			// bundle display name
	public int openCases;
	public int stale;
	public int unassigned;
	public String rag;			// green || amber || red || none

	public OpsBundle(String name) {
		this.name = name;
	}
}
