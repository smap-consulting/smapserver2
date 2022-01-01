package org.smap.notifications.interfaces;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Pty Ltd
 * 
 ******************************************************************************/

/*
 * Base class for calling AWS services
 */
public abstract class AWSService {
	

	public AWSService(String r, String b) {
		
	 
	}
	
	public String  setBucket(String mediaBucket, String serverFilePath, String filePath) {
		String bucketName = null;
		
		return bucketName;
	}
}
