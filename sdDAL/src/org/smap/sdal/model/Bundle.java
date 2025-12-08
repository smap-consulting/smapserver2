package org.smap.sdal.model;

public class Bundle {
	public String name;
	public String description;
	public boolean bundleRoles;
	
	public Bundle() {
		
	}
	
	public Bundle(String name, String description) {
		this.name = name;
		this.description = description;
	}
}
