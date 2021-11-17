package org.smap.sdal.model;

public class DashboardDetails {
	
	public String region;
	public String awsAccountId;
	public String dashboardId;
	public String roleSessionName;
	
	public String getRoleArn() {
		return "arn:aws:iam::" + awsAccountId + ":role/dashboard_role";
	}
}
