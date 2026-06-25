package org.smap.notifications.interfaces;

import java.io.File;
import java.util.logging.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Pty Ltd
 * 
 ******************************************************************************/

/*
 * Base class for calling AWS services
 */
public abstract class AWSService {

	static Logger log = Logger.getLogger(AWSService.class.getName());

	String defaultBucketName;	// Used if file is not already in an S3 bucket
	String region;
	String basePath;
	AmazonS3 s3 = null;
	

	public AWSService(String r, String basePath) {
		
		if(r != null) {
			this.region = r;
		} else {
			this.region = "us-east-1";
		}
		this.basePath = basePath;
		
		defaultBucketName = "smap-ai-" + region;
		// create a new S3 client
		//log.info("Getting s3 client for regions: " + region);
		s3 = AmazonS3Client.builder()
				.withRegion(region)
				.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
				.build();
        
	}
	
	public String  setBucket(String mediaBucket, String serverFilePath, String filePath) {
		String bucketName = null;
		if(mediaBucket == null) {
			bucketName = defaultBucketName;
			File file = new File(serverFilePath);				
			if(file.exists()) {
				s3.putObject(new PutObjectRequest(bucketName, filePath, file));
			} else {
				return("Error: Media File not found: " + file.getAbsolutePath());
			}
		} else {
			bucketName = mediaBucket;
		}
		return bucketName;
	}
}
