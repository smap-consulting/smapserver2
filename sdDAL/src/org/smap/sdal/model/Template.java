package org.smap.sdal.model;

public class Template {
	public int t_id;
	public boolean fromSettings;		// legacy, due to storage of single template in settings
	public String name;				// Template name
	public String templateType;		// pdf || word
	public boolean available;
	
}
