package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.List;

/*
 * A single case record for the org-scoped ops case viewer (Phase 5).
 */
public class OpsCase {
	public String groupSurveyIdent;
	public String caseSurveyIdent;		// _case_survey (oversight survey), used for assign
	public String instanceid;
	public String thread;
	public int prikey;
	public String title;
	public String bundle;				// bundle display name
	public String assignee;				// _assigned (owner ident), may be null
	public boolean closed;				// _case_closed is set
	public List<OpsField> fields = new ArrayList<>();

	public OpsCase(String groupSurveyIdent, String instanceid) {
		this.groupSurveyIdent = groupSurveyIdent;
		this.instanceid = instanceid;
	}
}
