package org.smap.notifications.interfaces;

import java.io.File;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

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

	// S3 clients are thread-safe and expensive to create, so share one per region
	private static final java.util.concurrent.ConcurrentHashMap<String, S3Client> s3Clients =
			new java.util.concurrent.ConcurrentHashMap<>();

	String defaultBucketName;	// Used if file is not already in an S3 bucket
	String region;
	String basePath;
	S3Client s3 = null;


	public AWSService(String r, String basePath) {

		if(r != null) {
			this.region = r;
		} else {
			this.region = "us-east-1";
		}
		this.basePath = basePath;

		defaultBucketName = "smap-ai-" + region;
		// Get a shared S3 client for this region
		s3 = getS3Client(region);

	}

	/*
	 * Get a reusable S3 client for the given region
	 */
	static S3Client getS3Client(String region) {
		return s3Clients.computeIfAbsent(region, r -> S3Client.builder()
				.region(Region.of(r))
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build());
	}

	public String  setBucket(String mediaBucket, String serverFilePath, String filePath) {
		String bucketName = null;
		if(mediaBucket == null) {
			bucketName = defaultBucketName;
			File file = new File(serverFilePath);
			if(file.exists()) {
				s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(filePath).build(),
						RequestBody.fromFile(file));
			} else {
				return("Error: Media File not found: " + file.getAbsolutePath());
			}
		} else {
			bucketName = mediaBucket;
		}
		return bucketName;
	}
}
