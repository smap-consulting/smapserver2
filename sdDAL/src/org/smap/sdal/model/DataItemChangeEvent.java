package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Contains details of a change item
 */
public class DataItemChangeEvent {
	
	public String event;
	public String userName;
	public String SurveyName;
	public int surveyVersion;
	public ArrayList<DataItemChange> changes;

	
}
