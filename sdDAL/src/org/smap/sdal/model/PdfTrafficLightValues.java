package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/*
 * Contains information needed to add a traffic light view
 */
public class PdfTrafficLightValues {

	public String geometry;				// A geopoint, geoshape or geotrace question
	public String startGeometry;		// A preload value

	// A line with marker values
	public String startLine;
	public String endLine;
	public ArrayList<String> markers;
	public ArrayList<DistanceMarker> orderedMarkers;	// Markers converted into the sequence for use in a line

	public boolean hasGeometry() {
		return geometry != null || startGeometry != null;
	}
	
	public boolean hasLine() {
		return startLine != null && endLine != null;
	}
	
	public boolean hasMarkers() {
		return markers != null && markers.size() > 0;
	}
	
	public String getStraightLineGeometry() {
		StringBuilder sb = new StringBuilder("{\"type\":\"LineString\",\"coordinates\":[");
		
		sb.append(getCoordinates(startLine, false));
		sb.append(",").append(getCoordinates(endLine, false));
		sb.append("]}");
		return sb.toString();
	}
	
	public String getLineGeometryWithMarkers(int idx) {
		
		StringBuilder sb = new StringBuilder("{\"type\":\"LineString\",\"coordinates\":[");		
		
		sb.append(getCoordinates(startLine, false));
		for(int i = 0; i < orderedMarkers.size(); i++) {
			DistanceMarker marker = orderedMarkers.get(i);
			if(idx == -1 || i <= idx) {
				sb.append(",").append(getCoordinates(marker.marker, false));
			}
		}
		if(idx == -1) {
			sb.append(",").append(getCoordinates(endLine, false));
		}
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
	
	public PreparedStatement getDistancePreparedStatement(Connection sd) throws SQLException {
		String sql = "SELECT ST_Distance(gg1, gg2) As spheroid_dist "
				+ "FROM (SELECT "
				+ "?::geography as gg1,"
				+ "?::geography as gg2"
				+ ") As foo";
		return sd.prepareStatement(sql);
	}
}
