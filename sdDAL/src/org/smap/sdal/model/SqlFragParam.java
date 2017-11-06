package org.smap.sdal.model;

public class SqlFragParam {
	private String type;			// text || sql || integer || double
	
	public String sValue;		// text || sql
	public int iValue;			// integer
	public double dValue;		// double
	
	void addTextParam(String v) {
		type = "text";
		sValue = v;
	}
	
	public void setType(String v) {
		type = v;
	}
	
	public String getType() {
		return type;
	}
	
	String debug() {
		if(type.equals("text") || type.equals("sql")) {
			return type + " : " + sValue;
		} else {
			return "";
		}
	}
}
