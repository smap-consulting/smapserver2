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
	public String label;  // notification / task-group name (user-entered step label)
	public boolean enabled;
	public int x;
	public int y;
	public String project;   // project name the notification belongs to
	public String bundle;    // bundle display name, if this node is part of a bundle
	public String assignee;  // for task/case types: username, role name, emails, "Submitter", "From Data"
	public List<Integer> fwdIds   = new ArrayList<>();  // forward record IDs backing this node
	public List<Integer> tgIds    = new ArrayList<>();  // task_group record IDs backing this node
	public List<Integer> startIds = new ArrayList<>();  // workflow_start record IDs backing this node
	public int caseSurveyId;    // for case nodes: the integer ID of the case management survey
	public int targetSurveyId;  // for task nodes: the integer ID of the target survey
	/*
	 * Whether x and y are where this user put this node, as opposed to where the default layout
	 * computed it.  Sent so the page can save back only the positions somebody actually chose: a
	 * computed default written into the saved layout becomes indistinguishable from a choice, and
	 * from then on the node no longer follows the layout it is supposed to.
	 */
	public boolean pinned;
	/*
	 * The bundle this node belongs to for the purpose of grouping and colouring, worked out by
	 * following the links rather than read off the node.
	 *
	 * Distinct from bundle, which is only set where the node has a survey of its own to take it
	 * from - forms.  A case, a decision or an email has none, so bundle is empty on most of the
	 * graph and cannot be what the page groups or colours by.  Kept separate rather than filled
	 * into bundle because this one is inferred: a step reachable from two bundles is put in the
	 * first that reaches it, which is a reasonable place to draw it and not a fact about the step.
	 */
	public String band;
}
