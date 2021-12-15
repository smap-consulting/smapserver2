package org.smap.sdal.model;

import java.util.ArrayList;

public class TrafficLight {
	
	public ArrayList<ArrayList<String>> lights = new ArrayList<>();
	
	public void addApp(String [] points) {
		// Point 0 is the appearance tag
		if(points.length > 1) {
			ArrayList<String> light = new ArrayList<String> ();
			for(int i = 1; i < points.length; i++) {
				light.add(points[i]);
			}
			lights.add(light);
		}
	}
}
