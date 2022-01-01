package org.smap.notifications.interfaces;

import com.amazonaws.auth.BasicSessionCredentials;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Consulting Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS Quicksight service
 */
public class QuickSight extends AWSService {

	public QuickSight(String r, BasicSessionCredentials credentials, String b,
			String d, String a) {
		
		super(r, b);
		

	}
	
	public String registerUser(String userId) {
		
		String userArn = null;

		
		return userArn;
	}
	
	public String getDashboardUrl(String userArn) {

		String url = null;
		
		return url;
	}

}
