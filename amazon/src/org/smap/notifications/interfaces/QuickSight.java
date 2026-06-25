package org.smap.notifications.interfaces;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.quicksight.AmazonQuickSight;
import com.amazonaws.services.quicksight.AmazonQuickSightClientBuilder;
import com.amazonaws.services.quicksight.model.CreateGroupMembershipRequest;
import com.amazonaws.services.quicksight.model.DescribeUserRequest;
import com.amazonaws.services.quicksight.model.DescribeUserResult;
import com.amazonaws.services.quicksight.model.GetDashboardEmbedUrlRequest;
import com.amazonaws.services.quicksight.model.GetDashboardEmbedUrlResult;
import com.amazonaws.services.quicksight.model.IdentityType;
import com.amazonaws.services.quicksight.model.RegisterUserRequest;
import com.amazonaws.services.quicksight.model.RegisterUserResult;
import com.amazonaws.services.quicksight.model.ResourceNotFoundException;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS Quicksight service
 */
public class QuickSight extends AWSService {

	AmazonQuickSight quicksightClient = null;
	//final String dashboardId = "3c0205d9-c84c-49bd-8112-20e81c16f619";
	//final String awsAccountId = "439804189189";
	String dashboardId = null;
	String awsAccountId = null;

	public QuickSight(String r, BasicSessionCredentials credentials, String basePath,
			String dashboardId,
			String awsAccountId) {	

		super(r, basePath);
		
		this.dashboardId = dashboardId;
		this.awsAccountId = awsAccountId;
		
		// create a new transcribe client
		ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setConnectionTimeout(60000);
        clientConfig.setMaxConnections(100);
        clientConfig.setSocketTimeout(60000);
        
        final AWSCredentialsProvider credsProvider = new AWSCredentialsProvider() {
        	@Override
        	public AWSCredentials getCredentials() {
        		// provide actual IAM access key and secret key here
        		return credentials;
        	}
        	
        	@Override
        	public void refresh() {}
        };
        	
        
		quicksightClient = AmazonQuickSightClientBuilder.standard()
				.withCredentials(credsProvider)
				.withRegion(region)
				.withClientConfiguration(clientConfig)
				.build();
	}
	
	public String registerUser(String userId) {
		
		String userArn = null;
		String memberName = "dashboard_role/" + userId;
		
		try {
			DescribeUserResult userResult = quicksightClient.describeUser(new DescribeUserRequest()
		            .withAwsAccountId(awsAccountId)
		            .withUserName(memberName)
		            .withNamespace("default"));
			
			userArn = userResult.getUser().getArn();
			

		} catch (ResourceNotFoundException e) {
			
			userArn = "arn:aws:quicksight:" + region + ":" + awsAccountId + ":user/default/dashboard_role/" + userId;
			
			RegisterUserResult registerUserResult = quicksightClient.registerUser(new RegisterUserRequest()
		            .withAwsAccountId(awsAccountId)
		            .withIdentityType(IdentityType.IAM)
		            .withNamespace("default")
		            .withIamArn("arn:aws:iam::439804189189:role/dashboard_role")
		            .withUserRole("READER")
		            .withSessionName(userId)
		            .withEmail("john.doe@example.com"));
			userArn = registerUserResult.getUser().getArn();
			
			// Add the user to the dashboard group
			quicksightClient.createGroupMembership(new CreateGroupMembershipRequest()
		            .withAwsAccountId(awsAccountId)
		            .withNamespace("default")
		            .withMemberName(memberName)
		            .withGroupName("cuso"));
		}
		
		return userArn;
	}
	
	public String getDashboardUrl(String userArn) {

		final GetDashboardEmbedUrlResult dashboardEmbedUrlResult =
				quicksightClient.getDashboardEmbedUrl(new GetDashboardEmbedUrlRequest()
		            .withDashboardId(dashboardId)
		            .withAwsAccountId(awsAccountId)
		            .withUserArn(userArn)
		            .withIdentityType(IdentityType.QUICKSIGHT)
		            .withResetDisabled(true)
		            .withSessionLifetimeInMinutes(100l)
		            .withUndoRedoDisabled(false));
		
		return dashboardEmbedUrlResult.getEmbedUrl();
	}

}
