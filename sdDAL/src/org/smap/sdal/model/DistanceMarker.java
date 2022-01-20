package org.smap.sdal.model;

import java.util.HashMap;

public class DistanceMarker {
	public float distance;
	public String marker;
	public HashMap<String, String> properties;
	
	public DistanceMarker(float distance, String marker) {
		this.distance = distance;
		this.marker = marker;
	}
	
	public float getDistance() {
		return distance;
	}
}
