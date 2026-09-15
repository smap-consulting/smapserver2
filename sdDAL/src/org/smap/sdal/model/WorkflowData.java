package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Container returned by GET /surveyKPI/workflow/items.
 */
public class WorkflowData {
	public ArrayList<WorkflowItem> items = new ArrayList<>();
	public ArrayList<WorkflowLink> links = new ArrayList<>();
	/*
	 * Every stage that has a person attached, as a flat list.  The same graph read the other way:
	 * the diagram says what leads to what, this says who does it and whether they can.
	 */
	public ArrayList<WorkflowPerson> people = new ArrayList<>();
}
