package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Geometry where the coordinates are an array of strings
 */
public class Point extends Geometry {
	public ArrayList<Double> coordinates;
	public Point() {
		type = "POINT";
	}
	public Point(Double lon, Double lat) {
		type = "POINT";
		coordinates = new ArrayList<Double> ();
		coordinates.add(lon);
		coordinates.add(lat);
	}
	
	public String getAsText() {
		StringBuffer out = new StringBuffer("POINT(");
		out.append(String.valueOf(coordinates.get(0))).append(" ").append(coordinates.get(1));
		out.append(")");
		return out.toString();
	}
	
}
