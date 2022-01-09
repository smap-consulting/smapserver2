package org.smap.sdal.model;

public class Template {
	public int id;
	public boolean fromSettings;		// legacy, due to storage of single template in settings
	public String name;					// Template name
	public String templateType;			// pdf || word
	public boolean not_available;		// Set true of the pdf template is not available for use
	public boolean default_template;
	public String filepath;
	
}
