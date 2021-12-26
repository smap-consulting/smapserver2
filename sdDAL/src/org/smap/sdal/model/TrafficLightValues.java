package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Contains values sourced from traffic light questions
 * A question can be in a repeat group hence the number of traffic light values 
 * may differ from the number of questions
 */
public class TrafficLightValues {
	
	public ArrayList<ArrayList<TrafficLightBulb>> lights = new ArrayList<>();
	
}
