package org.smap.sdal.model;

public class WebformChainRule {
	public int id;
	public String sIdent;				// Survey to which the rule applies
	public String type;
	public String newSurveyIdent;		// New survey to be instantiated
	public int newSurveyId;				// Used by the client which deals in survey id's rather than idents
	public String newSurveyName;		// Used by the client to show to the user
	public boolean instance;			// If true the new surveys is an instance
	public String rule;
	
}
