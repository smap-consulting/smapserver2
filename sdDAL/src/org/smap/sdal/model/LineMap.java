package org.smap.sdal.model;

import java.util.ArrayList;

public class LineMap {
	
	public String type;			// map or image
	public String geoCompoundQuestion;
	public String startPoint;	// deprecate
	public String endPoint;		// deprecate
	public ArrayList<String> markers = new ArrayList<>();	// Deprecate
	
	public LineMap(String [] points) {
		// Point 0 is the appearance tag
		if(points.length == 2) {
			geoCompoundQuestion = points[1];
		} else if(points.length > 1) {
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
	
	/*
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
	*/
}
