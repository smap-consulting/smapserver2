package org.smap.sdal.model;

public class MailoutPerson {
	public int id;
	public String email;
	public String name;
	public String status;
	public String status_loc;		// Localised name of status
	public String status_details;
	public String url;
	public int submissions;
	
	public Instance initialData;
	public String initial_data;		// JSON version of initial data
	
	public MailoutPerson(int id, String email, 
			String name, 
			String status, 
			String status_details,
			String url) {
		
		this.id = id;
		this.email = email;
		this.name = name;
		this.status = status;
		this.status_details = status_details;
		this.url = url;
		
		if(this.name == null) {
			this.name = "";		// For datatables
		}
		
		if(this.status_details == null) {
			this.status_details = "";		// For datatables
		}
		
		if(this.url == null) {
			this.url = "";		// For datatables
		}
	}
	
	public MailoutPerson(String email, String name, Instance initialData) {
		this.id = -1;
		this.email = email;
		this.name = name;
		this.initialData = initialData;	
	}
}
