package org.smap.notifications.interfaces;

import java.io.File;
import java.time.Duration;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.GetMedicalTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.GetMedicalTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.Media;
import software.amazon.awssdk.services.transcribe.model.MedicalTranscript;
import software.amazon.awssdk.services.transcribe.model.MedicalTranscriptionJob;
import software.amazon.awssdk.services.transcribe.model.Specialty;
import software.amazon.awssdk.services.transcribe.model.StartMedicalTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.StartMedicalTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.Transcript;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJob;
import software.amazon.awssdk.services.transcribe.model.Type;

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

	// Transcribe clients are thread-safe and expensive to create, so share one per region
	private static final java.util.concurrent.ConcurrentHashMap<String, TranscribeClient> transcribeClients =
			new java.util.concurrent.ConcurrentHashMap<>();

	TranscribeClient transcribeClient = null;

	public AudioProcessing(String r, String basePath) {

		super(r, basePath);

		transcribeClient = transcribeClients.computeIfAbsent(region, rg -> TranscribeClient.builder()
				.credentialsProvider(DefaultCredentialsProvider.create())
				.region(Region.of(rg))
				.httpClientBuilder(ApacheHttpClient.builder()
						.connectionTimeout(Duration.ofMillis(60000))
						.socketTimeout(Duration.ofMillis(60000))
						.maxConnections(100))
				.overrideConfiguration(ClientOverrideConfiguration.builder().build())
				.build());
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
					s3.getObject(GetObjectRequest.builder().bucket(mediaBucket).key(fileIdentifier).build(),
							tempFile.toPath());
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
				String mediaUri = s3.utilities().getUrl(GetUrlRequest.builder()
						.bucket(bucketName).key(fileIdentifier).build()).toString();
				Media media = Media.builder().mediaFileUri(mediaUri).build();
				if(medical) {
					Type type = (medType != null && medType.equals("conversation")) ? Type.CONVERSATION : Type.DICTATION;
					StartMedicalTranscriptionJobRequest request = StartMedicalTranscriptionJobRequest.builder()
							.media(media)
							.languageCode(fromLang)
							.medicalTranscriptionJobName(job)
							.outputBucketName(bucketName)
							.type(type)
							.specialty(Specialty.PRIMARYCARE)	// TODO parameterise
							.build();

					StartMedicalTranscriptionJobResponse result = transcribeClient.startMedicalTranscriptionJob(request);
					status = result.medicalTranscriptionJob().transcriptionJobStatusAsString();
				} else {
					StartTranscriptionJobRequest request = StartTranscriptionJobRequest.builder()
							.media(media)
							.languageCode(fromLang)
							.transcriptionJobName(job)
							.build();

					StartTranscriptionJobResponse result = transcribeClient.startTranscriptionJob(request);
					status = result.transcriptionJob().transcriptionJobStatusAsString();
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
		
		GetTranscriptionJobRequest request = GetTranscriptionJobRequest.builder()
				.transcriptionJobName(job).build();
		GetTranscriptionJobResponse result = transcribeClient.getTranscriptionJob(request);
		if(result != null) {
			TranscriptionJob tj = result.transcriptionJob();
			String status = tj.transcriptionJobStatusAsString();

			if(status != null && status.equals("COMPLETED")) {
				Transcript t = tj.transcript();
				uri = t.transcriptFileUri();
			}

		}
	
		return uri;
	}
	
	/*
	 * Get the medical transcript
	 */
	public String getMedicalTranscriptUri(String job) {
		String uri = null;
		
		GetMedicalTranscriptionJobRequest request = GetMedicalTranscriptionJobRequest.builder()
				.medicalTranscriptionJobName(job).build();
		GetMedicalTranscriptionJobResponse result = transcribeClient.getMedicalTranscriptionJob(request);
		if(result != null) {
			MedicalTranscriptionJob tj = result.medicalTranscriptionJob();
			String status = tj.transcriptionJobStatusAsString();

			if(status != null && status.equals("COMPLETED")) {
				MedicalTranscript t = tj.transcript();
				uri = t.transcriptFileUri();
			}

		}
	
		return uri;
	}

}
