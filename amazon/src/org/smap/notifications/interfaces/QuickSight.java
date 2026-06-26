package org.smap.notifications.interfaces;

import java.time.Duration;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.quicksight.QuickSightClient;
import software.amazon.awssdk.services.quicksight.model.CreateGroupMembershipRequest;
import software.amazon.awssdk.services.quicksight.model.DescribeUserRequest;
import software.amazon.awssdk.services.quicksight.model.DescribeUserResponse;
import software.amazon.awssdk.services.quicksight.model.EmbeddingIdentityType;
import software.amazon.awssdk.services.quicksight.model.GetDashboardEmbedUrlRequest;
import software.amazon.awssdk.services.quicksight.model.GetDashboardEmbedUrlResponse;
import software.amazon.awssdk.services.quicksight.model.IdentityType;
import software.amazon.awssdk.services.quicksight.model.RegisterUserRequest;
import software.amazon.awssdk.services.quicksight.model.RegisterUserResponse;
import software.amazon.awssdk.services.quicksight.model.ResourceNotFoundException;

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

	QuickSightClient quicksightClient = null;
	//final String dashboardId = "3c0205d9-c84c-49bd-8112-20e81c16f619";
	//final String awsAccountId = "439804189189";
	String dashboardId = null;
	String awsAccountId = null;

	public QuickSight(String r, AwsSessionCredentials credentials, String basePath,
			String dashboardId,
			String awsAccountId) {

		super(r, basePath);

		this.dashboardId = dashboardId;
		this.awsAccountId = awsAccountId;

		quicksightClient = QuickSightClient.builder()
				.credentialsProvider(StaticCredentialsProvider.create(credentials))
				.region(Region.of(region))
				.httpClientBuilder(ApacheHttpClient.builder()
						.connectionTimeout(Duration.ofMillis(60000))
						.socketTimeout(Duration.ofMillis(60000))
						.maxConnections(100))
				.build();
	}
	
	public String registerUser(String userId) {
		
		String userArn = null;
		String memberName = "dashboard_role/" + userId;
		
		try {
			DescribeUserResponse userResult = quicksightClient.describeUser(DescribeUserRequest.builder()
		            .awsAccountId(awsAccountId)
		            .userName(memberName)
		            .namespace("default")
		            .build());

			userArn = userResult.user().arn();


		} catch (ResourceNotFoundException e) {

			userArn = "arn:aws:quicksight:" + region + ":" + awsAccountId + ":user/default/dashboard_role/" + userId;

			RegisterUserResponse registerUserResult = quicksightClient.registerUser(RegisterUserRequest.builder()
		            .awsAccountId(awsAccountId)
		            .identityType(IdentityType.IAM)
		            .namespace("default")
		            .iamArn("arn:aws:iam::439804189189:role/dashboard_role")
		            .userRole("READER")
		            .sessionName(userId)
		            .email("john.doe@example.com")
		            .build());
			userArn = registerUserResult.user().arn();

			// Add the user to the dashboard group
			quicksightClient.createGroupMembership(CreateGroupMembershipRequest.builder()
		            .awsAccountId(awsAccountId)
		            .namespace("default")
		            .memberName(memberName)
		            .groupName("cuso")
		            .build());
		}
		
		return userArn;
	}
	
	public String getDashboardUrl(String userArn) {

		final GetDashboardEmbedUrlResponse dashboardEmbedUrlResult =
				quicksightClient.getDashboardEmbedUrl(GetDashboardEmbedUrlRequest.builder()
		            .dashboardId(dashboardId)
		            .awsAccountId(awsAccountId)
		            .userArn(userArn)
		            .identityType(EmbeddingIdentityType.QUICKSIGHT)
		            .resetDisabled(true)
		            .sessionLifetimeInMinutes(100l)
		            .undoRedoDisabled(false)
		            .build());

		return dashboardEmbedUrlResult.embedUrl();
	}

}
