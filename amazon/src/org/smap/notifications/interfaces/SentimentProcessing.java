package org.smap.notifications.interfaces;

import java.util.ArrayList;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClient;
import com.amazonaws.services.comprehend.model.DetectSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectSentimentResult;
import com.amazonaws.services.comprehend.model.SentimentScore;
import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.TranslateTextRequest;
import com.amazonaws.services.translate.model.TranslateTextResult;

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
		
		AmazonComprehend comprehend = AmazonComprehendClient.builder()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(region)
                .build();
		
		DetectSentimentRequest request = new DetectSentimentRequest()
				.withText(source)
				.withLanguageCode(language);
		DetectSentimentResult result  = comprehend.detectSentiment(request);
			
		sentiment.sentiment = result.getSentiment();
		sentiment.score = getScore(result.getSentimentScore(), sentiment.sentiment);
        
		log.info("Sentiment: " + sentiment);
		return sentiment;
		
	}
	
	private Float getScore(SentimentScore scores, String sentiment) {
		Float score = (float) 0.0;
		
		if(sentiment != null) {
			if(sentiment.equals("POSITIVE")) {
				score = scores.getPositive();
			} else if(sentiment.equals("NEGATIVE")) {
				score = scores.getNegative();
			} else if(sentiment.equals("NEUTRAL")) {
				score = scores.getNeutral();
			} else if(sentiment.equals("MIXED")) {
				score = scores.getMixed();
			}
			
		}
		return score;
	}
}
