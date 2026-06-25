package org.smap.notifications.interfaces;

import java.io.File;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.transcribe.AmazonTranscribe;
import com.amazonaws.services.transcribe.AmazonTranscribeClientBuilder;
import com.amazonaws.services.transcribe.model.GetMedicalTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.GetMedicalTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.Media;
import com.amazonaws.services.transcribe.model.MedicalTranscript;
import com.amazonaws.services.transcribe.model.MedicalTranscriptionJob;
import com.amazonaws.services.transcribe.model.Specialty;
import com.amazonaws.services.transcribe.model.StartMedicalTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.StartMedicalTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.StartTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.StartTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.Transcript;
import com.amazonaws.services.transcribe.model.TranscriptionJob;
import com.amazonaws.services.transcribe.model.Type;

import tools.Utilities;

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

	AmazonTranscribe transcribeClient = null;

	public AudioProcessing(String r, String basePath) {
		
		super(r, basePath);
		
		// create a new transcribe client
		ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setConnectionTimeout(60000);
        clientConfig.setMaxConnections(100);
        clientConfig.setSocketTimeout(60000);
        
		transcribeClient = AmazonTranscribeClientBuilder.standard()
				.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
				.withRegion(region)
				.withClientConfiguration(clientConfig)
				.build();
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
		boolean awsSupported = false;
		boolean convert = false;
		String ext = "";
		String serverFilePath = null;

		// Check to see if the file type is supported
		int idx = fileIdentifier.lastIndexOf('.');
		if(idx > 0) {
			ext = fileIdentifier.substring(idx+1).toLowerCase();
		}
		if(ext.equals("mp3") || ext.equals("mp4") || ext.equals("wav")) {
			awsSupported = true;
			log.info("Extension " + ext + " is nativly supported by AWS");
		} else if(ext.equals("amr")) {
			log.info("Extension " + ext + " will be converted to mp3");
			convert = true;
		}
		
		if(awsSupported || convert) {
			
			// Conversion
			if(convert) {
				String tempFilePath = null;
				String convertedTempFilePath = null;
				if(mediaBucket != null) {
					// Get the file onto local disk to convert it
					String fBase = "/smap/temp/" + UUID.randomUUID().toString() + ".";
					tempFilePath = fBase + ext;
					convertedTempFilePath = fBase + "mp3";
					
					File tempFile = new File(tempFilePath);
					log.info("Getting media file from s3 bucket: " + mediaBucket + " " + fileIdentifier + " to : " + tempFilePath);
					s3.getObject(new GetObjectRequest(mediaBucket, fileIdentifier, null), tempFile);
				} else {
					tempFilePath = basePath + fileIdentifier;
					convertedTempFilePath = "/smap/temp/" + UUID.randomUUID().toString() + ".mp3";					
				}
				
				// Convert the file and update file path to the converted file
				log.info("Converting: " + tempFilePath + " to " + convertedTempFilePath);
				if(!Utilities.convertMedia(tempFilePath, convertedTempFilePath)) {
					String msg = localisation.getString("aws_t_conv_erro");
					msg = msg.replace("%s1", tempFilePath);
					return(msg);
				}
				serverFilePath = convertedTempFilePath;
				mediaBucket = null;	// Need to upload
			} else if(mediaBucket == null) {
				serverFilePath = basePath + fileIdentifier;
			}
			
			// Put local files into remote default bucket			
			String bucketName = setBucket(mediaBucket, serverFilePath, fileIdentifier);			
				
			// Generate the transcript
			String status = null;
			try {
				log.info("Generating transcript for file: " + bucketName + fileIdentifier);
				Media media=new Media().withMediaFileUri(s3.getUrl(bucketName, fileIdentifier).toString());
				if(medical) {
					Type type = (medType != null && medType.equals("conversation")) ? Type.CONVERSATION : Type.DICTATION;
					StartMedicalTranscriptionJobRequest request = new StartMedicalTranscriptionJobRequest()
							.withMedia(media)
							.withLanguageCode(fromLang)
							.withMedicalTranscriptionJobName(job)
							.withOutputBucketName(bucketName)
							.withType(type)
							.withSpecialty(Specialty.PRIMARYCARE);	// TODO parameterise
						
					StartMedicalTranscriptionJobResult result = transcribeClient.startMedicalTranscriptionJob(request);
					status = result.getMedicalTranscriptionJob().getTranscriptionJobStatus();
				} else {
					StartTranscriptionJobRequest request = new StartTranscriptionJobRequest()
							.withMedia(media)
							.withLanguageCode(fromLang)
							.withTranscriptionJobName(job);
						
					StartTranscriptionJobResult result = transcribeClient.startTranscriptionJob(request);
					status = result.getTranscriptionJob().getTranscriptionJobStatus();
				}
				log.info("Transcribe job status: " + status);	
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
				return e.getMessage();			
			}
			    
			response.append(status);
			
		} else {
			String msg = localisation.getString("aws_t_ns");
			msg = msg.replace("%s1", ext);
			response.append(msg);
		}

		
		return response.toString();
		
	}
	
	/*
	 * Get the transcript
	 */
	public String getTranscriptUri(String job) {
		String uri = null;
		
		GetTranscriptionJobRequest request = new GetTranscriptionJobRequest()
				.withTranscriptionJobName(job);		
		GetTranscriptionJobResult result = transcribeClient.getTranscriptionJob(request);
		if(result != null) {
			TranscriptionJob tj = result.getTranscriptionJob();
			String status = tj.getTranscriptionJobStatus();
			
			if(status != null && status.equals("COMPLETED")) {
				Transcript t = tj.getTranscript();
				uri = t.getTranscriptFileUri();
			}
			
		}
	
		return uri;
	}
	
	/*
	 * Get the medical transcript
	 */
	public String getMedicalTranscriptUri(String job) {
		String uri = null;
		
		GetMedicalTranscriptionJobRequest request = new GetMedicalTranscriptionJobRequest()
				.withMedicalTranscriptionJobName(job);		
		GetMedicalTranscriptionJobResult result = transcribeClient.getMedicalTranscriptionJob(request);
		if(result != null) {
			MedicalTranscriptionJob tj = result.getMedicalTranscriptionJob();
			String status = tj.getTranscriptionJobStatus();
			
			if(status != null && status.equals("COMPLETED")) {
				MedicalTranscript t = tj.getTranscript();
				uri = t.getTranscriptFileUri();
			}
			
		}
	
		return uri;
	}

}
