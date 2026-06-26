package org.smap.notifications.interfaces;

import java.util.concurrent.ConcurrentHashMap;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.Label;
import software.amazon.awssdk.services.rekognition.model.S3Object;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS Rekognition service
 */
public class ImageProcessing extends AWSService {

	// Rekognition clients are thread-safe and expensive to create, so share one per region
	private static final ConcurrentHashMap<String, RekognitionClient> rekognitionClients = new ConcurrentHashMap<>();

	public ImageProcessing(String region, String basePath) {
		super(region, basePath);
	}

	/*
	 * Get labels
	 */
	public String getLabels(
			String basePath,
			String fileIdentifier, 	// How the file is identified in the bucket
			String format, 
			String mediaBucket) {
		
		String serverFilePath = basePath + fileIdentifier;
		StringBuffer labels = new StringBuffer("");
		
		// Put local files into remote default bucket			
		String bucketName = setBucket(mediaBucket, serverFilePath, fileIdentifier);	
			
		S3Object s3Object = S3Object.builder()
				.bucket(bucketName)
				.name(fileIdentifier)
				.build();
		Image image = Image.builder().s3Object(s3Object).build();
		DetectLabelsRequest request = DetectLabelsRequest.builder()
				.image(image)
				.maxLabels(10)
				.minConfidence(80F)
				.build();

		RekognitionClient rekognitionClient = rekognitionClients.computeIfAbsent(region, r -> RekognitionClient.builder()
			.region(Region.of(r))
			.credentialsProvider(DefaultCredentialsProvider.create())
			.build());

		log.info("get labels for region: " + region);
		DetectLabelsResponse result = rekognitionClient.detectLabels(request);

		if(format.equals("params")) {
			labels.append(result.toString());
		} else {
			for(Label l : result.labels()) {
				if(labels.length() > 0) {
					labels.append(", ");
				}
				labels.append(l.name());
			}
		}
		
		return labels.toString();
		
	}

}
