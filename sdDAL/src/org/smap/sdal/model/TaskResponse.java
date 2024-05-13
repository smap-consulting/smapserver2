package org.smap.sdal.model;

import java.util.List;
import java.util.Set;

import com.google.gson.annotations.SerializedName;

public class TaskResponse {

	public String message;
	public String status;
	public int version;				// Manage progressive enhancement of this service by incrementing version
	public String deviceId;
	public long time_difference = 0;
	@SerializedName("data")
	public List<TaskResponseAssignment> taskAssignments;
	public List<FormLocator> forms;
	public FieldTaskSettings settings;
	public List<TaskCompletionInfo> taskCompletionInfo;
	public List<PointEntry> userTrail;
	public List<Project> projects;
	public String current_org;
	public Set<String> orgs;
	public List<ReferenceSurvey> refSurveys;
	
}
