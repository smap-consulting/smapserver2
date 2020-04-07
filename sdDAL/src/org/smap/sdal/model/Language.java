package org.smap.sdal.model;

public class Language {
	public int id;
	public String name;
	public String code;
	public boolean rtl;
	public boolean deleted = false;
	
	public Language () {}
	
	public Language (int id, String name, String code, boolean rtl) {
		this.id = id;
		this.name = name;
		this.code = code;
		this.rtl = rtl;
	}
}
