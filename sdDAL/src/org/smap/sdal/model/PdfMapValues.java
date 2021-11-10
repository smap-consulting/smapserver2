package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Contains details of a change item
 */
public class PdfMapValues {
	public String geometry;				// A geopoint, geoshape or geotrace question
	public String startGeometry;		// A preload value

	// A line with marker values
	public String startLine;
	public String endLine;
	public ArrayList<String> markers;

	public boolean hasGeometry() {
		return geometry != null || startGeometry != null;
	}
	
	public boolean hasLine() {
		return startLine != null && endLine != null;
	}
	
	public boolean hasMarkers() {
		return markers != null && markers.size() > 0;
	}
	
	public String getLineGeometry() {
		StringBuilder sb = new StringBuilder("{\"type\":\"LineString\",\"coordinates\":[");
		
		sb.append(getCoordinates(startLine, false));
		sb.append(",").append(getCoordinates(endLine, false));
		sb.append("]}");
		return sb.toString();
	}
	
	// Get the coordinates of a point
	public String getCoordinates(String geometry, boolean removeBrackets) {
		String coords = null;
		if(geometry != null) {
			int idx = geometry.indexOf("[");
			int idx2 = geometry.lastIndexOf("]");
			
			coords = geometry.substring(idx, idx2 + 1);
			if(removeBrackets) {
				coords = coords.substring(1, coords.length() - 1);
			}
		}
		return(coords);
	}
}
