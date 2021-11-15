package org.smap.notifications.interfaces;

import java.util.ResourceBundle;

/*****************************************************************************

This file is part of SMAP.

Copyright Smap Consulting Pty Ltd

 ******************************************************************************/

/*
 * Manage the table that stores details on the forwarding of data onto other systems
 */
public class EmitAwsSMS extends EmitSMS {
	
	public EmitAwsSMS(String senderId, ResourceBundle l) {

	}
	
	// Send an sms
	@Override
	public String sendSMS( 
			String number, 
			String content) throws Exception  {
		
		String responseBody = null;
		
		return responseBody;
	}	

}


