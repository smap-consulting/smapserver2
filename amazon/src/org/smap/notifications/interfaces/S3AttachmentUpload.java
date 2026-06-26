package org.smap.notifications.interfaces;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/*
 * Static class to upload attachments to S3 
 * Fast (hopefully)
 */
public class S3AttachmentUpload {

	private S3AttachmentUpload() {
		
	}
	
	private static S3Client s3;
	private static String bucket;
	private static String region;
	private static boolean s3Enabled = true;
	
	static Logger log = Logger.getLogger(AWSService.class.getName());
	
	public static void put(String basePath, String filePath) throws Exception {
		
		if(s3Enabled) {
			
			initialise(basePath);		// Initialise first time through
			
			if(s3Enabled) {				// Check again as the initialisation may have set s3Enabled to false
				/*
				 * Send the file
				 */
				File file = new File(filePath);
				String s3Path = filePath.substring(basePath.length() + 1);
				if(file.exists()) {
					s3.putObject(PutObjectRequest.builder().bucket(bucket).key(s3Path).build(),
							Paths.get(filePath));
				} else {
					log.info("Error uploading to S3: File not found: " + file.getAbsolutePath());
				}
			}
		}
		
	}
	
	public static void get(String basePath, String filePath) throws IOException {
		
		if(s3Enabled) {
			
			initialise(basePath);		// Initialise first time through
			
			/*
			 * Get the file
			 */
			String s3Path = filePath.substring(basePath.length() + 1);
			log.info("Getting archived XML file " + filePath + " from bucket " + bucket + " in region " + region);
			ResponseInputStream<GetObjectResponse> s3is = s3.getObject(GetObjectRequest.builder()
					.bucket(bucket).key(s3Path).build());
	        FileOutputStream fos = new FileOutputStream(new File(filePath));
	        byte[] read_buf = new byte[1024];
	        int read_len = 0;
	        while ((read_len = s3is.read(read_buf)) > 0) {
	        	fos.write(read_buf, 0, read_len);
	        }
	        s3is.close();
	        fos.close();
			
		}
		
	}
	
	/*
	 * Check to see if a file exists
	 */
	public static boolean exists(String basePath, String filePath) {
		boolean exists = false;
		
		if(s3Enabled) {
			
			initialise(basePath);		// Initialise first time through
			
			String s3Path = filePath.substring(basePath.length() + 1);
			log.info("Checking for file on S3 " + filePath + " from bucket " + bucket + " in region " + region);
			try {
				s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(s3Path).build());
				exists = true;
			} catch (NoSuchKeyException e) {
				exists = false;
			}
		}
		return exists;
	}
	
	private static void initialise(String basePath) {
		if(s3 == null) {
			
			bucket = getSettingFromFile(basePath + "/settings/bucket");
			if(bucket == null) {
				s3Enabled = false;
				log.info("S3 not enabled");
			} else {
				region = getSettingFromFile(basePath + "/settings/region");

				s3 = S3Client.builder()
						.region(Region.of(region))
						.credentialsProvider(DefaultCredentialsProvider.create())
						.build();
			}
		}
	}
	
	private static String getSettingFromFile(String filePath) {
		
		String setting = null;
		try {
			List<String> lines = Files.readAllLines(new File(filePath).toPath());
			if(lines.size() > 0) {
				setting = lines.get(0);
			}
		} catch (Exception e) {

		}

		return setting;
	}
	
}
