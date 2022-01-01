package org.smap.notifications.interfaces;

import java.io.IOException;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * Copyright Smap Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to AWS transcribe service
 */
public class S3 extends AWSService {
	
	public S3(String r, String uri, String b) {		
		super(r, b);
	}
	
	public String get() throws Exception {
		 
		StringBuilder sb = new StringBuilder();
	
		return sb.toString();
	}
	
	public void rm() throws IOException {
		 
	}

}
