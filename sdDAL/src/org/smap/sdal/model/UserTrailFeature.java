package org.smap.sdal.model;

import java.util.ArrayList;

public class UserTrailFeature {	
	public String ident;
	public String name;		// user name
	public ArrayList<UserTrailPoint> points = new ArrayList<UserTrailPoint> ();
	
	public UserTrailFeature(String ident, String name) {
		this.ident = ident;
		this.name = name;
	}
}
