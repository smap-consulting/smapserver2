package org.smap.notifications.interfaces;

import com.amazonaws.auth.BasicSessionCredentials;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS STS service
 */
public class STS extends AWSService {

	public STS(String r) {
		
		super(r);
	
	}
	
	public BasicSessionCredentials getSessionCredentials() {
		
        BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
                null,
                null,
                null);
        
		return awsCredentials;
	}

}
