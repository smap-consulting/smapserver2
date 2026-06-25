package org.smap.notifications.interfaces;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.Request;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS S3 service
 */
public class S3 extends AWSService {

	//AmazonTranscribe transcribeClient = null;
	AmazonS3URI s3uri = null;
	
	public S3(String r, String uri, String basePath) {	
		super(r, basePath);
		s3uri = new AmazonS3URI(uri);
	}
	
	/*
	 * Get an s3 object as a string
	 */
	public String get() throws Exception {
		 
		StringBuilder sb = new StringBuilder();
		if(s3uri != null) {
			BufferedReader bufferedReader = null;
			try {
				
				bufferedReader = new BufferedReader(new InputStreamReader(s3.getObject(new GetObjectRequest(s3uri.getBucket(), s3uri.getKey())).getObjectContent()));
				
				String line;
				while ( (line = bufferedReader.readLine()) != null ) {
					sb.append(line);
				}
	
			} finally {
				if(bufferedReader != null) try{bufferedReader.close();} catch(Exception e) {}
			}
		} else {
			throw new Exception("S3 object not found");
		}
		return sb.toString();
	}
	
	public void rm() throws IOException {
		 
		s3.deleteObject(s3uri.getBucket(), s3uri.getKey());
		s3uri = null;
	}
	

}
