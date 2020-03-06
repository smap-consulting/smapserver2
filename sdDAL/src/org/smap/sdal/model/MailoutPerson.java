package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.HashMap;

public class MailoutPerson {
	public int id;
	public String email;
	public String name;
	public String status;
	public String status_loc;		// Localised name of status
	public String status_details;
	
	public HashMap<String, String> initialData;
	
	public MailoutPerson(int id, String email, String name, String status, String status_details) {
		this.id = id;
		this.email = email;
		this.name = name;
		this.status = status;
		this.status_details = status_details;
		
		if(this.status_details == null) {
			this.status_details = "";		// For datatables
		}
	}
	
	public MailoutPerson(String email, String name, HashMap<String, String> initialData) {
		this.id = -1;
		this.email = email;
		this.name = name;
		this.initialData = initialData;	
	}
}
