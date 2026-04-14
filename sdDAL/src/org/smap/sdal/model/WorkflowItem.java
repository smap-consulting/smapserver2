package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.List;

/*
 * A single node in a workflow diagram.
 *
 * type  — what kind of workitem:
 *           form | task | case | decision | periodic | reminder | email | sms
 *
 * role  — determines the visual shape of the node:
 *           form         (rectangle — a survey/form)
 *           decision     (diamond)
 *           trigger      (rounded — periodic/reminder trigger)
 *           notification (envelope/bell — email/sms)
 *
 * name  — derived from the Name Source defined per type:
 *           form / task / case  → survey display name
 *           periodic / reminder / email / sms → notification name
 *
 * Nodes are deduplicated by id across all data sources so that, e.g., two
 * notifications both triggered by the same survey share one "form" node.
 */
public class WorkflowItem {
	public String id;
	public String type;
	public String role;
	public String name;
	public boolean enabled;
	public int x;
	public int y;
	public String project;   // project name the notification belongs to
	public String bundle;    // bundle display name, if this node is part of a bundle
	public String assignee;  // for task/case types: username, role name, emails, "Submitter", "From Data"
	public List<Integer> fwdIds = new ArrayList<>();  // forward record IDs backing this node
	public List<Integer> tgIds  = new ArrayList<>();  // task_group record IDs backing this node
}
