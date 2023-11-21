package org.smap.sdal.model;

public class SetValue {
	
	public static String START = "odk-instance-first-load";
	public static String TRIGGER = "xforms-value-changed";
	public static String REPEAT = "odk-new-repeat";
	
	public String event;
	public String value;
	public String ref;
	
	public SetValue(String e, String v, String r) {
		event = e;
		value = v;
		ref = r;
	}
}
