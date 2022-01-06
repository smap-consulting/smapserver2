package org.smap.notifications.interfaces;

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
		
		StringBuffer labels = new StringBuffer("");
		
		return labels.toString();
		
	}

}
