package org.smap.notifications.interfaces;

import java.util.ArrayList;


import model.Sentiment;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS Comprehend service
 */
public class SentimentProcessing extends AWSService {
	
	public SentimentProcessing(String region, String basePath) {
		super(region, basePath);	
	}

	/*
	 * Get the Sentiment
	 */
	public Sentiment getSentiment(
			String source,
			String language) throws Exception {
		
		Sentiment sentiment = new Sentiment();
		
		return sentiment;
		
	}
	
}
