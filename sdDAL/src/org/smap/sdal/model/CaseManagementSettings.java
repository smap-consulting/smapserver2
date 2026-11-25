package org.smap.sdal.model;

public class CaseManagementSettings {
	public String name;					// Name of the Bundle / Topic that is hosting the case
	public String description;			// Description of the Bundle / Topic
	public String finalStatus;			// The one closed status of earlier versions, the first of closedStatuses
	public java.util.List<String> closedStatuses;	// Status values that mean the case is closed
	public String statusQuestion;		// Column name of the status question
	public String criticalityQuestion;	// Column name of the status question
	/*
	 * Whether a case can be assigned to more than one person at a time.  Off by default, which is how
	 * cases have always worked.  Null in a save that does not mention it, which keeps the stored value,
	 * so a page or tool that does not know about it cannot turn it off.
	 */
	public Boolean multipleAssignees;

	/*
	 * The status values that close a case.  Settings saved before there could be several have only finalStatus.
	 */
	public java.util.List<String> getClosedStatuses() {
		java.util.ArrayList<String> out = new java.util.ArrayList<> ();
		if(closedStatuses != null) {
			for(String v : closedStatuses) {
				if(v != null && v.trim().length() > 0 && !out.contains(v.trim())) {
					out.add(v.trim());
				}
			}
		}
		if(out.isEmpty() && finalStatus != null && finalStatus.trim().length() > 0) {
			out.add(finalStatus.trim());
		}
		return out;
	}

	/*
	 * Only the closedStatuses given, tidied, without falling back to finalStatus
	 */
	public java.util.List<String> getClosedStatusesListed() {
		java.util.ArrayList<String> out = new java.util.ArrayList<> ();
		if(closedStatuses != null) {
			for(String v : closedStatuses) {
				if(v != null && v.trim().length() > 0 && !out.contains(v.trim())) {
					out.add(v.trim());
				}
			}
		}
		return out;
	}

	/*
	 * Whether a status value closes the case
	 */
	public boolean isClosed(String status) {
		return status != null && getClosedStatuses().contains(status.trim());
	}

	/*
	 * Whether cases close at all: there is a status question and a value that closes
	 */
	public boolean hasClosedStatus() {
		return statusQuestion != null && statusQuestion.trim().length() > 0 && !getClosedStatuses().isEmpty();
	}

	public boolean allowsMultipleAssignees() {
		return multipleAssignees != null && multipleAssignees;
	}
}
