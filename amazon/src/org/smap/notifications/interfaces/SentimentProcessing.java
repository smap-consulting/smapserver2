package org.smap.notifications.interfaces;

import java.util.concurrent.ConcurrentHashMap;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DetectSentimentRequest;
import software.amazon.awssdk.services.comprehend.model.DetectSentimentResponse;
import software.amazon.awssdk.services.comprehend.model.SentimentScore;

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

	// Comprehend clients are thread-safe and expensive to create, so share one per region
	private static final ConcurrentHashMap<String, ComprehendClient> comprehendClients = new ConcurrentHashMap<>();

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

		ComprehendClient comprehend = comprehendClients.computeIfAbsent(region, r -> ComprehendClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(r))
                .build());

		DetectSentimentRequest request = DetectSentimentRequest.builder()
				.text(source)
				.languageCode(language)
				.build();
		DetectSentimentResponse result  = comprehend.detectSentiment(request);

		sentiment.sentiment = result.sentimentAsString();
		sentiment.score = getScore(result.sentimentScore(), sentiment.sentiment);
        
		log.info("Sentiment: " + sentiment);
		return sentiment;
		
	}
	
	private Float getScore(SentimentScore scores, String sentiment) {
		Float score = (float) 0.0;
		
		if(sentiment != null) {
			if(sentiment.equals("POSITIVE")) {
				score = scores.positive();
			} else if(sentiment.equals("NEGATIVE")) {
				score = scores.negative();
			} else if(sentiment.equals("NEUTRAL")) {
				score = scores.neutral();
			} else if(sentiment.equals("MIXED")) {
				score = scores.mixed();
			}

		}
		return score;
	}
}
