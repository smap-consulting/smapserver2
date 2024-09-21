package org.smap.sdal.model;

import java.util.ResourceBundle;

public class AwsSdkEmailServer extends EmailServer {

	public AwsSdkEmailServer(ResourceBundle localisation) {
		super(localisation);
	}
	
	@Override
	public void send(String email, String ccType, String subject, int emailId, String contentString, String filePath,
			String filename) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
