package org.smap.sdal.model;

public class CaseManagementSettings {
	public String name;					// Name of the Bundle / Topic that is hosting the case
	public String description;			// Description of the Bundle / Topic
	public String finalStatus;
	public String statusQuestion;		// Column name of the status question
	public String criticalityQuestion;	// Column name of the status question
	/*
	 * Whether a case can be assigned to more than one person at a time.  Off by default, which is how
	 * cases have always worked.  Null in a save that does not mention it, which keeps the stored value,
	 * so a page or tool that does not know about it cannot turn it off.
	 */
	public Boolean multipleAssignees;

	public boolean allowsMultipleAssignees() {
		return multipleAssignees != null && multipleAssignees;
	}
}
