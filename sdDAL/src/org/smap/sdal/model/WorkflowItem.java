package org.smap.sdal.model;

/*
 * A single node in a workflow diagram - either a trigger or an action.
 * Multiple records in the forward / task_group tables may map to the same
 * WorkflowItem (e.g. two notifications both triggered by the same survey share
 * one submission trigger node).  Deduplication is done by the WorkflowManager
 * using the id field as a stable key.
 */
public class WorkflowItem {
	public String id;        // stable deduplication key, e.g. "submission:s:42" or "task:tg:7"
	public String type;      // submission | periodic | reminder | task | case | email | sms | server_calc | forward
	public String role;      // "trigger" or "action"
	public String name;      // display name shown on the node card
	public boolean enabled;
}
