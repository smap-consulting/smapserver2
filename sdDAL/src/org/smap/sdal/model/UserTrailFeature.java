package org.smap.sdal.model;

import java.util.ArrayList;

public class UserTrailFeature {	
	public String ident;
	public ArrayList<UserTrailPoint> points = new ArrayList<UserTrailPoint> ();
	
	public UserTrailFeature(String ident) {
		this.ident = ident;
	}
}
