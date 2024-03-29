package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
	public ArrayList<DistanceMarker> orderedMarkers;	// Markers converted into the sequence for use in a line
	
	public boolean geoCompound = false;		// Geocompound section
	public int idxStart;
	public int idxEnd;
	public int lastFaultIdx;
	public ArrayList<Integer> idxMarkers = new ArrayList<>();

	public boolean hasGeometry() {
		return (geometry != null && geometry.trim().length() > 0) || (startGeometry != null && startGeometry.trim().length() > 0);
	}
	
	public boolean hasLine() {
		return startLine != null && endLine != null;
	}
	
	public boolean hasMarkers() {
		return (markers != null && markers.size() > 0) || (geoCompound && orderedMarkers != null && orderedMarkers.size() > 0);
	}
	
	public String getStraightLineGeometry() {
		StringBuilder sb = new StringBuilder("{\"type\":\"LineString\",\"coordinates\":[");
		
		sb.append(getCoordinates(startLine, false));
		sb.append(",").append(getCoordinates(endLine, false));
		sb.append("]}");
		return sb.toString();
	}
	
	/*
	 * Get a geojson line string
	 * If a non negative idx is passed then only get markers up to that index
	 */
	public String getLineGeometryWithMarkers(int idx) {
		
		StringBuilder sb = new StringBuilder("{\"type\":\"LineString\",\"coordinates\":[");		
		
		sb.append(getCoordinates(startLine, false));
		if(orderedMarkers != null) {
			for(int i = 0; i < orderedMarkers.size(); i++) {
				DistanceMarker marker = orderedMarkers.get(i);
				if(idx == -1 || i <= idx) {
					sb.append(",").append(getCoordinates(marker.markerLocation, false));
				}
			}
		}
		if(idx == -1) {
			sb.append(",").append(getCoordinates(endLine, false));
		}
		sb.append("]}");
		return sb.toString();
	}
	
	/*
	 * Get a sub section of a geojson line string
	 * If a non negative idx is passed then only get points up to that index
	 */
	public String getLineGeometryBetweenPoints(int pointIdx1, int pointIdx2) {
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		Line line = gson.fromJson(geometry, Line.class);
		
		// Remove line segments before idx1
		if(pointIdx1 > 0) {
			for(int i = pointIdx1 -1 ; i >= 0; i--) {  
				line.coordinates.remove(i);
			}
		}
		
		// Remove line segments after idx2
		if(pointIdx2 < line.coordinates.size() - 1) {
			for(int i = line.coordinates.size() - 1; i > pointIdx2; i--) {  
				line.coordinates.remove(i);
			}
		}
			
		return gson.toJson(line);
	}
	
	/*
	 * Get the coordinates of a point index in a linestring
	 */
	public String getPointCoordinates(int pointIdx) {
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		Line line = gson.fromJson(geometry, Line.class);
		String value = "";
		
		if(pointIdx >= 0 && pointIdx < line.coordinates.size()) {
			
			ArrayList<Double> coords = line.coordinates.get(pointIdx);
			if(coords.size() > 0) {
				value = String.valueOf(coords.get(1)) + " " + String.valueOf(coords.get(0));
			}
		
		}
			
		return value;
	}
	
	// Get the coordinates of a point
	public String getCoordinates(String geometry, boolean removeBrackets) {
		String coords = null;
		if(geometry != null) {
			int idx = geometry.indexOf("[");
			int idx2 = geometry.lastIndexOf("]");
			if(idx >= 0 && idx2 > idx) {
				coords = geometry.substring(idx, idx2 + 1);
				if(removeBrackets) {
					coords = coords.substring(1, coords.length() - 1);
				}
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
