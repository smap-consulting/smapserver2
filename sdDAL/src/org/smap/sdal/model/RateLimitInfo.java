package org.smap.sdal.model;

public class RateLimitInfo {
	public boolean permitted;
	public int gap;
	public long milliSecsElapsed;  // Milli-Seconds since last call of this service	
}
