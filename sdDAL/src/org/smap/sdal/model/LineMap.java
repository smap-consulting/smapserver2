package org.smap.sdal.model;

import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class LineMap {
	
	public String type;			// map or image
	public String startPoint;
	public String endPoint;
	public ArrayList<String> markers = new ArrayList<>();
	
	public LineMap(String [] points) {
		// Point 0 is the appearance tag
		if(points.length > 1) {
			startPoint = points[1];
		}
		if(points.length > 2) {
			endPoint = points[2];
		}
		if(points.length > 3) {
			for(int i = 3; i < points.length; i++) {
				markers.add(points[i]);
			}
		}
	}
	
	public String getCompoundColumnName() {
		StringBuilder name = new StringBuilder("_cmp");
		if(startPoint != null) {
			name.append("_").append(startPoint);
		}
		if(endPoint != null) {
			name.append("_").append(endPoint);
		}
		if(markers.size() > 0) {
			for(String marker : markers) {
				name.append("_").append(marker);
			}
		}
		return GeneralUtilityMethods.cleanName(name.toString(), true, true, true);
	}
}
