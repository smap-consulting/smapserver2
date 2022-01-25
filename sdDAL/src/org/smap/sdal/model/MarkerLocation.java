package org.smap.sdal.model;

/*
 * Contains information used for getting details on a specific marker
 */
public class MarkerLocation {
	
	public String type = null;
	public int count = 0;
	
	public MarkerLocation(String [] points) throws Exception {
		// Point 0 is the appearance tag
		// Point 1 is the geocompound question which is stored in the linemap object
		
		if(points.length > 2) {
			type = points[2];
		}
		if(points.length > 3) {
			count = Integer.valueOf(points[3]);
		}
		
	}
}
