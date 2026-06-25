package org.smap.notifications.interfaces;

import java.util.ArrayList;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.TranslateTextRequest;
import com.amazonaws.services.translate.model.TranslateTextResult;

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
			
		AmazonTranslate translate = AmazonTranslateClient.builder()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(region)
                .build();
			
		String in = encodePlaceHolders(source);	// Add do not translates
		
		ArrayList<String> frags = getTextFragments(in);
		StringBuilder outBuf = new StringBuilder("");
		int backoff = 1;
		for (int i = 0; i < frags.size(); i++) {
			String frag = frags.get(i);
			try {
				TranslateTextRequest request = new TranslateTextRequest()
		                .withText(frag)
		                .withSourceLanguageCode(sourceLanguage)
		                .withTargetLanguageCode(targetLanguage);
		        TranslateTextResult result  = translate.translateText(request);
			
		        outBuf.append(result.getTranslatedText());
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
