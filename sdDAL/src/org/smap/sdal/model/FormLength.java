package org.smap.sdal.model;

public class FormLength {
	public static int MAX_FORM_LENGTH = 1580;		// 1,600 for Postgres table size and allow for 20 preloads
	
	public String name;
	public int questionCount;		// Number of questions in the form
	
	public FormLength(String n, int l) {
		name = n;
		questionCount = l;
	}
	
	public boolean isTooLong() {
		return questionCount > MAX_FORM_LENGTH;
	}
}
