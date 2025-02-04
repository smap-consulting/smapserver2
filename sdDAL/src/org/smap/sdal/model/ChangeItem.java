package org.smap.sdal.model;

import java.sql.Timestamp;

/*
 * Contains details of a change item
 */
public class ChangeItem {
	
	// Reference data about the survey or question or option to be updated or properties of a new element
	public PropertyChange property;	// Details of a change to a question or option property
	public Question question;		// Details of a new question
	public Option option;			// Details of a new option
	public String name;				// Name used for option lists
	
	// Properties for logging
	public int cId;					// The database key for this change (only used when reading a change item from the log)
	public int version;				// The survey version that this applies to
	public String userName;			// The name of the user who made this change
	public String qName;			// The name of the question being updated
	public String qType;			// The question type (used when updating options for a question)
	public String fileName;			// External file name used to load changes to choices
	public String fileUrl;			// URL to retrieve the uploaded file
	public String changeType;		// Copied from the changeset and added to the log for each change item
	public String source;			// Copied from the changeset and added to the log for each change item
	public Timestamp updatedTime;	// The time that this update was made (Only used when reading a change item from the log)
	public boolean  apply_results;	// Set true once the change has been attempted to be applied to the results database
	public boolean success;			// Set true if the change item has been successfully applied to the results database
	public String msg;				// Error messages
	public String action;			// Add / delete / change
	public String type;				// question or option
	public int origSId;				// The survey id when this CI was created, this suvey id will change if a survey is replaced

	public ChangeItem() {
		
	}
	
	public ChangeItem(String type, String propType, int qId, String oldVal, 
			String newVal,
			String languageName) {
		property = new PropertyChange();
		property.type = type;
		property.propType = propType;	// as opposed to media
		property.qId = qId;
		property.oldVal = oldVal;
		property.newVal = newVal;
		property.languageName = languageName;
	}
}
