package org.smap.sdal.model;

public class SetValue {
	
	public static String START = "odk-instance-first-load";
	
	public String event;
	public String value;
	
	public SetValue(String e, String v) {
		event = e;
		value = v;
	}
}
