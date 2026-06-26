package org.smap.notifications.interfaces;

import java.time.Duration;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS STS service
 */
public class STS extends AWSService {

	StsClient stsClient = null;

	public STS(String r, String basePath) {

		super(r, basePath);

		// create a new STS client
		stsClient = StsClient.builder()
				.credentialsProvider(DefaultCredentialsProvider.create())
				.region(Region.of(region))
				.httpClientBuilder(ApacheHttpClient.builder()
						.connectionTimeout(Duration.ofMillis(60000))
						.socketTimeout(Duration.ofMillis(60000))
						.maxConnections(100))
                .build();
	}

	public AwsSessionCredentials getSessionCredentials(String roleARN, String roleSessionName) {

		//final String roleARN = "arn:aws:iam::439804189189:role/dashboard_role";
		//final String roleSessionName = "cuso";


		AssumeRoleRequest roleRequest = AssumeRoleRequest.builder()
                .roleArn(roleARN)
                .roleSessionName(roleSessionName)
                .build();

		AssumeRoleResponse roleResponse = stsClient.assumeRole(roleRequest);
		Credentials sessionCredentials = roleResponse.credentials();

		 // Create a session credentials object that contains the credentials you just retrieved.
		return AwsSessionCredentials.create(
                sessionCredentials.accessKeyId(),
                sessionCredentials.secretAccessKey(),
                sessionCredentials.sessionToken());
	}

}
