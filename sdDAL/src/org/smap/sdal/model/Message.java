package org.smap.sdal.model;

/*
 * Return message from file upload
 */
public class Message {
	String status;
	String message;
	String name;
	
	public Message(String status, String message, String name) {
		this.status = status;
		this.message = message;
		this.name = name;
	}
}
