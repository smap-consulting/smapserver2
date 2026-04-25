package org.smap.sdal.model;

/*
 * A directed edge between two WorkflowItems: from a trigger node to an action node.
 */
public class WorkflowLink {
	public String from;   // id of the trigger WorkflowItem
	public String to;     // id of the action WorkflowItem
}
