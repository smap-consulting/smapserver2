package org.smap.sdal.model;

public class RateLimitInfo {
	public boolean permitted;
	public int gap;
	public int secsElapsed;  // Seconds since last call of this service	
}
