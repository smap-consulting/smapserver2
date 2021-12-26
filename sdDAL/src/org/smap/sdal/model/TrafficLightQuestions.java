package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Contains information used for displaying traffic lights
 * including colors, crossed out and labels
 */
public class TrafficLightQuestions {
	
	public ArrayList<ArrayList<TrafficLightBulb>> lights = new ArrayList<>();
	
	public void addApp(String [] points) throws Exception {
		// Point 0 is the appearance tag
		if(points.length > 1) {
			ArrayList<TrafficLightBulb> light = new ArrayList<> ();
		
			for(int i = 1; i < points.length; i++) {
				System.out.println("point " + points[i]);
				String[] components = points[i].split(":");
				String color = components[0];
				String crossItem = "";
				String labelItem = "";
				if(components.length > 1) {
					for(int j  = 1; j < components.length; j++) {
						if(components[j].toLowerCase().startsWith("x")) {
							crossItem = components[j].substring(1);
						} else if(components[j].toLowerCase().startsWith("l")) {
							labelItem = components[j].substring(1);
						} 
					}
				}
				light.add(new TrafficLightBulb(color, crossItem, labelItem));
				
			}
			lights.add(light);
			
		}
	}
}
