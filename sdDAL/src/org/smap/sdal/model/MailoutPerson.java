package org.smap.sdal.model;


public class MailoutPerson {
	public int id;
	public String email;
	public String name;
	public String status;
	
	public MailoutPerson(int id, String email, String name, String status) {
		this.id = id;
		this.email = email;
		this.name = name;
		this.status = status;
	}
}
