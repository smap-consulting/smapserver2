package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.List;

/*
 * One stage of a process that has a person attached to it.
 *
 * The workflow diagram answers what leads to what.  This answers who, which is a different question
 * and the one that decides whether a process actually runs: a step assigned to a role nobody holds,
 * or to somebody who cannot open the form they are sent to, looks exactly like a working step on the
 * diagram and silently does nothing.
 */
public class WorkflowPerson {
	public String nodeId;			// the workflow node this row describes
	public String process;			// the bundle, as the workflow page bands them
	public String stage;			// what the step is called
	public String type;				// form | case | task | reference | email
	public String when;				// the condition that reaches this step, or empty for always
	public String form;				// the survey the person works on
	public String project;			// the project holding that survey, which is what gates access
	public String assignedTo;		// the role name, the username, or how the assignee is chosen
	public String assigneeType;		// role | user | submitter | data | emails | project | none
	public List<String> people = new ArrayList<>();	// who that resolves to now
	public boolean ok = true;		// whether this step can actually reach somebody who can do it
	public String problem;			// what is wrong with it when it cannot
}
