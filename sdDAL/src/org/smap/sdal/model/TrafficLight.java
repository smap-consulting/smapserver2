package org.smap.sdal.model;

import java.util.ArrayList;

public class TrafficLight {
	
	public ArrayList<ArrayList<String>> lights = new ArrayList<>();
	public ArrayList<ArrayList<String>> crosses = new ArrayList<>();
	public ArrayList<ArrayList<String>> labels = new ArrayList<>();
	
	public void addApp(String [] points) throws Exception {
		// Point 0 is the appearance tag
		if(points.length > 1) {
			ArrayList<String> light = new ArrayList<String> ();
			ArrayList<String> cross = new ArrayList<String> ();
			ArrayList<String> label = new ArrayList<String> ();
			for(int i = 1; i < points.length; i++) {
				System.out.println("point " + points[i]);
				String[] components = points[i].split(":");
				light.add(components[0]);
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
				cross.add(crossItem);
				label.add(labelItem);
				
			}
			lights.add(light);
			crosses.add(cross);
			labels.add(label);
		}
	}
}
