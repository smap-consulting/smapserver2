package org.smap.sdal.model;

/*
 * A single bulb in a traffic light.
 * A traffic light can have any number of bulbs
 */
public class TrafficLightBulb {
	
	public String color;
	public String cross;
	public String label;
	
	public TrafficLightBulb() {
		
	}
	public TrafficLightBulb(String color, String cross, String label) {
		this.color = color;
		this.cross = cross;
		this.label = label;
	}
}
