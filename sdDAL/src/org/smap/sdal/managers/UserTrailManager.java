package org.smap.sdal.managers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.UserTrailFeature;


/*****************************************************************************

This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

 ******************************************************************************/

/*
 * Manages access to the user trail data
 */
public class UserTrailManager {

	private static Logger log =
			Logger.getLogger(UserTrailManager.class.getName());
	private static ResourceBundle localisation;
	private String tz;

	public UserTrailManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}

	/*
	 * Generate a KML file in the reports directory and return its name
	 */
	public String generateKML(Connection sd, HashMap<String, String> params, String basePath) throws SQLException, IOException {
		
		String filepath = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtDistance = null;
		
		/*
		 * Extract parameters
		 */
		Timestamp startDate = new Timestamp(0);
		String startDateString = params.get("startDate");
		if(startDateString != null) {
			try {
				startDate = new Timestamp(Long.valueOf(startDateString));
			} catch(Exception e) {
				
			}
		}
		
		Timestamp endDate = new Timestamp(0);
		String endDateString = params.get("endDate");
		if(endDateString != null) {
			try {
				endDate = new Timestamp(Long.valueOf(endDateString));
			} catch(Exception e) {
				
			}
		}
		
		// User for whom to get the location trail
		int uId = 0;
		String uIdString = params.get("userId");
		if(uIdString != null) {
			try {
				uId = Integer.valueOf(uIdString);
			} catch(Exception e) {
				
			}
		}
		
		// Maximum distance between poojts for which it is assumed there is a single line
		int mps = 200;
		String mpsString = params.get("mps");
		if(mpsString != null) {
			try {
				mps = Integer.valueOf(mpsString);
			} catch(Exception e) {
				
			}
		}
				
		try {
			String sqlDistance = "SELECT ST_Distance( "
					+ "ST_Transform(?::geometry, 3857),"
					+ "ST_Transform(?::geometry, 3857)"
					+ ")";
			pstmtDistance = sd.prepareStatement(sqlDistance);
	
			StringBuffer sql = new StringBuffer("SELECT ut.id as id, " +
					"ST_X(the_geom::geometry) as x, " +		
					"ST_Y(the_geom::geometry) as y, " +
					"ut.event_time as event_time " +
				"FROM user_trail ut, users u  " +
				"where u.id = ut.u_id ");
			
			if(startDateString != null) {
				sql.append("and ut.event_time >= ? ");
			}
			if(endDateString != null) {
				sql.append("and ut.event_time <  ? ");
			}
			sql.append("and ut.u_id = ? " +
					"order by ut.event_time asc, ut.id asc");
			
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			if(startDateString != null) {
				pstmt.setTimestamp(idx++, startDate);
			}
			if(endDateString != null) {
				pstmt.setTimestamp(idx++, endDate);
			}
			pstmt.setInt(idx++, uId);
			
			/*
			 * Export KML
			 */		
			GeneralUtilityMethods.createDirectory(basePath + "/reports");
			filepath = basePath + "/reports/" + String.valueOf(UUID.randomUUID()) + ".kml";	// Use a random sequence to keep survey name unique
			File tempFile = new File(filepath);
			PrintWriter writer = new PrintWriter(tempFile);
			writeKmlHeader(writer);
			
			ArrayList<ArrayList<UserTrailFeature>> featureList = getKmlFeatures(pstmt, pstmtDistance, mps);
			DecimalFormat df = new DecimalFormat("#.0000");
			for(ArrayList<UserTrailFeature> features : featureList) {
				if(features.size() == 0) {
					// ignore
				} else if(features.size() == 1) { 
					// point
					writer.println("<Placemark>");
					writer.println("<styleUrl>#trail_style</styleUrl>");
					writer.println("<Point>");
					
					writer.println("<coordinates>");
					for(UserTrailFeature f : features) {					
					    String lon = df.format(f.coordinates[0]);
					    String lat = df.format(f.coordinates[1]);
					    writer.println(lon + "," + lat + ",0");
					}
					writer.println("</coordinates>");
					
					writer.println("</Point>");
					writer.println("</Placemark>");
					
				} else if(features.size() > 1) {	// line
					// Add line
					writer.println("<Placemark>");
					writer.println("<styleUrl>#trail_style</styleUrl>");
					writer.println("<LineString>");
					writer.println("<tessellate>1</tessellate>");
					
					writer.println("<coordinates>");
					for(UserTrailFeature f : features) {	
					    String lon = df.format(f.coordinates[0]);
					    String lat = df.format(f.coordinates[1]);
						writer.println(lon + "," + lat + ",0");
					}
					writer.println("</coordinates>");
					
					writer.println("</LineString>");
					writer.println("</Placemark>");
					
				}
			}
			
			writeKmlFooter(writer);
			writer.flush();
			writer.close();
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
			if(pstmtDistance != null) try {pstmtDistance.close();} catch (Exception e) {}
		}
		
		return filepath;
	
	}
	
	private void writeKmlHeader(PrintWriter writer) {
		writer.println("<?xml version=\"1.0\" encoding=\"utf-8\" ?>");
		writer.println("<kml xmlns=\"http://www.opengis.net/kml/2.2\"");
		writer.println("xmlns:gx=\"http://www.google.com/kml/ext/2.2\">");
		
		writer.println("<Document>");
		
		writer.println("<Style id=\"trail_style\">");
		writer.println("<LineStyle>");
		writer.println("<color>ff0000ff</color>");
		writer.println("<width>5</width>");
		writer.println("</LineStyle>");
		writer.println("</Style>");
	}
	
	private void writeKmlFooter(PrintWriter writer) {
		writer.println("</Document>");
		writer.println("</kml>");
	}
	
	/*
	 * Get features, consecutive points are converted to lines unless the break distance between points is exceeded
	 */
	ArrayList<ArrayList<UserTrailFeature>> getKmlFeatures(PreparedStatement pstmt, PreparedStatement pstmtDistance, int breakDistance) throws SQLException {
		ArrayList<ArrayList<UserTrailFeature>> featureList = new ArrayList<> ();
		
		ArrayList<UserTrailFeature> features = new ArrayList<UserTrailFeature> ();
		boolean havePrev = false;
		Double prevX = 0.0;
		Double prevY = 0.0;
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			UserTrailFeature f = new UserTrailFeature();
			f.coordinates[0] = rs.getDouble("x");
			f.coordinates[1] = rs.getDouble("y");
			if(havePrev) {
				if(isGreaterThanBreakDistance(pstmtDistance, prevX, prevY, f.coordinates[0], f.coordinates[1], breakDistance)) {
					featureList.add(features);
					features = new ArrayList<UserTrailFeature> ();
				}
			} 
			prevX = f.coordinates[0];
			prevY = f.coordinates[1];
			havePrev = true;
			features.add(f);
		}
		featureList.add(features);
		
		return featureList;
	}
	
	private boolean isGreaterThanBreakDistance(PreparedStatement pstmtDistance, double prevX, double prevY, double x, double y, int breakDistance) throws SQLException {
		
		StringBuilder p1 = new StringBuilder("SRID=4326;POINT(");
		p1.append(prevX).append(" ").append(prevY).append(")");
		StringBuilder p2 = new StringBuilder("SRID=4326;POINT(");
		p2.append(x).append(" ").append(y).append(")");
		
		pstmtDistance.setString(1, p1.toString());
		pstmtDistance.setString(2, p2.toString());
		
		log.info(pstmtDistance.toString());
		ResultSet rs = pstmtDistance.executeQuery();
		if(rs.next()) {
			return rs.getInt(1) > breakDistance;
		}
		return false;
	}

}
