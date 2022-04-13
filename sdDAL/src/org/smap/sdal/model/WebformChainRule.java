package org.smap.sdal.model;

public class WebformChainRule {
	public int id;
	public String sIdent;				// Survey to which the rule applies
	public String type;
	public String newSurveyIdent;			// New survey to be instantiated
	public boolean instance;		// If true the new surveys is an instance
	public String rule;
	
}
