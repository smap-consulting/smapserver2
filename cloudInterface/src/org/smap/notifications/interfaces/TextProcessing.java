package org.smap.notifications.interfaces;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS Translate service
 */
public class TextProcessing extends AWSService {
	
	public TextProcessing(String region, String b) {
		super(region, b);	
	}

	/*
	 * Get labels
	 */
	public String getTranslatian(
			String source,
			String sourceLanguage,
			String targetLanguage) throws Exception {
			
		String out = null;
		return out;
		
	}

}
