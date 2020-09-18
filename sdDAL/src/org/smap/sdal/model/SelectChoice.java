package org.smap.sdal.model;

import java.util.HashMap;

public class SelectChoice {
	public String labelInnerText;
	public String value;
	public int index;
	
	public HashMap<String, String> mlChoices;
	
	public SelectChoice(String v, String l, int idx) {
		labelInnerText = l;
		value = v;
		index = idx;	
	}

	public SelectChoice() {

	}
	
}
