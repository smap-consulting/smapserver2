package org.smap.notifications.interfaces;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

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

	AWSSecurityTokenService stsClient = null;

	public STS(String r, String basePath) {
		
		super(r, basePath);
		
		// create a new transcribe client
		ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setConnectionTimeout(60000);
        clientConfig.setMaxConnections(100);
        clientConfig.setSocketTimeout(60000);
        
        
        stsClient = AWSSecurityTokenServiceClientBuilder.standard()
        		.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
				.withRegion(region)
				.withClientConfiguration(clientConfig)
                .build();
	}
	
	public BasicSessionCredentials getSessionCredentials(String roleARN, String roleSessionName) {
		
		//final String roleARN = "arn:aws:iam::439804189189:role/dashboard_role";
		//final String roleSessionName = "cuso";
		
		
		AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                .withRoleArn(roleARN)
                .withRoleSessionName(roleSessionName);
				
		AssumeRoleResult roleResponse = stsClient.assumeRole(roleRequest);
		Credentials sessionCredentials = roleResponse.getCredentials();
		
		 // Create a BasicSessionCredentials object that contains the credentials you just retrieved.
        BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(),
                sessionCredentials.getSecretAccessKey(),
                sessionCredentials.getSessionToken());
        
		return awsCredentials;
	}

}
