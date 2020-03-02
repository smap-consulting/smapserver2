package org.smap.sdal.model;


public class MailoutPerson {
	public int id;
	public String email;
	public String name;
	public String status;
	public String status_loc;		// Localisd name of status
	public String status_details;
	
	public MailoutPerson(int id, String email, String name, String status, String status_details) {
		this.id = id;
		this.email = email;
		this.name = name;
		this.status = status;
		this.status_details = status_details;
	}
	
	public MailoutPerson(String email, String name) {
		this.id = -1;
		this.email = email;
		this.name = name;
		this.status = null;
		
	}
}
