package org.smap.notifications.interfaces;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

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

	S3Uri s3uri = null;

	public S3(String r, String uri, String basePath) {
		super(r, basePath);
		s3uri = s3.utilities().parseUri(URI.create(uri));
	}

	/*
	 * Get an s3 object as a string
	 */
	public String get() throws Exception {

		StringBuilder sb = new StringBuilder();
		if(s3uri != null) {
			BufferedReader bufferedReader = null;
			try {

				GetObjectRequest req = GetObjectRequest.builder()
						.bucket(s3uri.bucket().orElse(null))
						.key(s3uri.key().orElse(null))
						.build();
				bufferedReader = new BufferedReader(new InputStreamReader(s3.getObject(req)));

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

		s3.deleteObject(DeleteObjectRequest.builder()
				.bucket(s3uri.bucket().orElse(null))
				.key(s3uri.key().orElse(null))
				.build());
		s3uri = null;
	}
	

}
