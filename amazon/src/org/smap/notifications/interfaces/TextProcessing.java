package org.smap.notifications.interfaces;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.translate.TranslateClient;
import software.amazon.awssdk.services.translate.model.TranslateTextRequest;
import software.amazon.awssdk.services.translate.model.TranslateTextResponse;

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

	private final int MAX_LENGTH = 5000 - 2;		// Maximum number of bytes accepted by AWS, allow two bytes to add a new line back in

	// Translate clients are thread-safe and expensive to create, so share one per region
	private static final ConcurrentHashMap<String, TranslateClient> translateClients = new ConcurrentHashMap<>();

	public TextProcessing(String region, String basePath) {
		super(region, basePath);
	}

	/*
	 * Get labels
	 */
	public String getTranslatian(
			String source,
			String sourceLanguage,
			String targetLanguage) throws Exception {
			
		TranslateClient translate = translateClients.computeIfAbsent(region, r -> TranslateClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(r))
                .build());

		String in = encodePlaceHolders(source);	// Add do not translates
		
		ArrayList<String> frags = getTextFragments(in);
		StringBuilder outBuf = new StringBuilder("");
		int backoff = 1;
		for (int i = 0; i < frags.size(); i++) {
			String frag = frags.get(i);
			try {
				TranslateTextRequest request = TranslateTextRequest.builder()
		                .text(frag)
		                .sourceLanguageCode(sourceLanguage)
		                .targetLanguageCode(targetLanguage)
		                .build();
		        TranslateTextResponse result  = translate.translateText(request);

		        outBuf.append(result.translatedText());
		        backoff = 1;
			} catch (Exception e) {
				String msg = e.getMessage();
				if(msg != null && msg.contains("Rate exceeded")) {
					i--;
					log.info("------------------------ Rate exceeded ----------- Sleeping for " + backoff + " seconds");
					Thread.sleep(backoff * 1000);
					backoff = 2 * backoff;
				} else {
					throw new Exception(e);
				}
			}
		}
        
		String out = decodePlaceHolders(outBuf.toString());
		log.info("Translate to: " + out);
		return out;
		
	}
	
	// Mark placeholders as do not translate
	private String encodePlaceHolders(String in) {
		return in.replace("${", "#${#");  
	}
	
	// Decode placeholders as do not translate
	private String decodePlaceHolders(String in) {
		return in.replace("{ #", "{#")
				.replace("$ {", "${")
				.replace("# $", "#$")
				.replace("#${#", "${");
	}
	
	/*
	 * Split the input string into fragments less than the maximum
	 */
	private ArrayList<String> getTextFragments(String in) throws Exception {
		ArrayList<String> out = new ArrayList<String> ();
		
		if(in.getBytes().length < MAX_LENGTH) {
			out.add(in);
		} else {
			// Lets hack out a split based on new lines
			String[] a = in.split("\\R");
			int count = 0;
			StringBuilder frag = new StringBuilder("");
			for(int i = 0; i < a.length; i++) {
				count += a[i].getBytes().length;
				if(count < MAX_LENGTH) {
					frag.append(a[i]);
				} else {
					if(frag.length() == 0) {
						throw new Exception("Cannot split text into fragments shorter than " + MAX_LENGTH + " using new lines");
					} else {
						frag.append("\n\r");
						out.add(frag.toString());
						count = 0;
						frag = new StringBuilder("");
						i--;
					}
				}
			}
			if(frag.length() > 0) {
				frag.append("\n\r");
				out.add(frag.toString());
			}
		}
		
		int count = 1;
		for(String f : out) {
			System.out.println(count++ + " ------- "  + f);
		}
		return out;
	}

}
