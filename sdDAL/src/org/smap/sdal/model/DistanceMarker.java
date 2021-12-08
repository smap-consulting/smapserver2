package org.smap.sdal.model;

public class DistanceMarker {
	public float distance;
	public String marker;
	
	public DistanceMarker(float distance, String marker) {
		this.distance = distance;
		this.marker = marker;
	}
	
	public float getDistance() {
		return distance;
	}
}
