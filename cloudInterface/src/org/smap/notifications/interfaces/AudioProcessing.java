package org.smap.notifications.interfaces;

import java.util.ResourceBundle;


/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS transcribe service
 */
public class AudioProcessing extends AWSService {

	public AudioProcessing(String r, String b) {
		
		super(r, b);
		
	
	}

	/*
	 * Submit an audio job
	 */
	public String submitJob(ResourceBundle localisation, 
			String basePath, 
			String fileIdentifier, 	// How the file is identified in the bucket
			String fromLang, 
			String job,
			String mediaBucket,
			boolean medical,
			String medType) {
		
		StringBuffer response = new StringBuffer("");
		

		
		return response.toString();
		
	}
	
	/*
	 * Get the transcript
	 */
	public String getTranscriptUri(String job) {
		String uri = null;
	
		return uri;
	}
	
	/*
	 * Get the medical transcript
	 */
	public String getMedicalTranscriptUri(String job) {
		String uri = null;
		
	
		return uri;
	}

}
