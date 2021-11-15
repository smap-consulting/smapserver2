package org.smap.notifications.interfaces;

/*****************************************************************************

This file is part of SMAP.

Copyright Smap Pty Ltd

 ******************************************************************************/

/*
 * Manage the table that stores details on the forwarding of data onto other systems
 */
public class EmitNotifications {
	
	public static int AWS_REGISTER_ORGANISATION = 0;
	
	public EmitNotifications() {
		
	}
	
	/*
	 * Publish to an SNS topic
	 */
	public void publish(int event, String msg, String subject) {
		
	}

}


