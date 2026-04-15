package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Container returned by GET /surveyKPI/workflow/items.
 */
public class WorkflowData {
	public ArrayList<WorkflowItem> items = new ArrayList<>();
	public ArrayList<WorkflowLink> links = new ArrayList<>();
}
