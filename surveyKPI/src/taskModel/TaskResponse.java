package taskModel;

import java.util.HashSet;
import java.util.List;

import org.smap.sdal.model.Project;
import org.smap.sdal.model.TaskAssignment;

import com.google.gson.annotations.SerializedName;

public class TaskResponse {

	public String message;
	public String status;
	public int version;				// Manage progressive enhancement of this service by incrementing version
	public String deviceId;
	public long time_difference = 0;
	@SerializedName("data")
	public List<TaskAssignment> taskAssignments;
	public List<FormLocator> forms;
	public FieldTaskSettings settings;
	public List<TaskCompletionInfo> taskCompletionInfo;
	public List<PointEntry> userTrail;
	public List<Project> projects;
	public String current_org;
	public HashSet<String> orgs;
	public List<ReferenceSurvey> refSurveys;
	
}
