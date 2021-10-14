package org.smap.sdal.managers;

import java.io.File;
import java.io.FileOutputStream;
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

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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
	
	private class TravelDistance {
		public String user;
		public int km;
		
		public TravelDistance(String user, int km) {
			this.user = user;
			this.km = km;
		}
	}
	
	private class Parameters {
		public Timestamp startDate;
		public Timestamp endDate;
		public int uId;
		public int mps;
		
		String startDateString = null;
		String endDateString = null;
		String uIdString = null;
		String mpsString = null;
		
		public Parameters(HashMap<String, String> params) {
			
			startDate = new Timestamp(0);
			startDateString = params.get(BackgroundReportsManager.PARAM_START_DATE);
			if(startDateString != null) {
				try {
					startDate = new Timestamp(Long.valueOf(startDateString));
				} catch(Exception e) {
					
				}
			}
			
			endDate = new Timestamp(0);
			endDateString = params.get(BackgroundReportsManager.PARAM_END_DATE);
			if(endDateString != null) {
				try {
					endDate = new Timestamp(Long.valueOf(endDateString));
				} catch(Exception e) {
					
				}
			}
			
			// User for whom to get the location trail
			uId = 0;
			uIdString = params.get(BackgroundReportsManager.PARAM_USER_ID);
			if(uIdString != null) {
				try {
					uId = Integer.valueOf(uIdString);
				} catch(Exception e) {
					
				}
			}
			
			// Maximum distance between pojnts for which it is assumed there is a single line
			mps = 200;
			mpsString = params.get(BackgroundReportsManager.PARAM_MPS);
			if(mpsString != null) {
				try {
					mps = Integer.valueOf(mpsString);
				} catch(Exception e) {
					
				}
			}
		}
	}

	/*
	 * Generate a KML file in the reports directory and return its name
	 */
	public String generateKML(Connection sd, HashMap<String, String> params, String basePath) throws SQLException, IOException {
		
		String filename = String.valueOf(UUID.randomUUID()) + ".kml";

		Parameters p = new Parameters(params);   // Extract parameters
		ArrayList<ArrayList<UserTrailFeature>> featureList = getGeomFeatures(sd, p);  // Get the features

		/*
		 * Export KML
		 */		
		GeneralUtilityMethods.createDirectory(basePath + "/reports");
		String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
		File tempFile = new File(filepath);
		PrintWriter writer = new PrintWriter(tempFile);
		writeKmlHeader(writer);


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
		
		return filename;
	
	}
	
	/*
	 * Generate a KML file in the reports directory and return its name
	 */
	public String generateDistanceReport(Connection sd, HashMap<String, String> params, String basePath) throws SQLException, IOException {
		String filename = String.valueOf(UUID.randomUUID()) + ".xlsx";
		
		Parameters p = new Parameters(params);   // Extract parameters
		ArrayList<ArrayList<UserTrailFeature>> featureList = getGeomFeatures(sd, p);  // Get the features
		TravelDistance distance = getTravelDistances(sd, filename, GeneralUtilityMethods.getUserName(sd, p.uId), featureList);
		
		GeneralUtilityMethods.createDirectory(basePath + "/reports");
		String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
		FileOutputStream outputStream = new FileOutputStream(filepath);
		
		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet(localisation.getString("rep_data"));
		
		wb.write(outputStream);
		outputStream.close();
		return filename;
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
	private ArrayList<ArrayList<UserTrailFeature>> getGeomFeatures(Connection sd, Parameters p) throws SQLException {
		
		ArrayList<ArrayList<UserTrailFeature>> featureList = new ArrayList<> ();
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtDistance = null;
		
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
			
			if(p.startDateString != null) {
				sql.append("and ut.event_time >= ? ");
			}
			if(p.endDateString != null) {
				sql.append("and ut.event_time <  ? ");
			}
			sql.append("and ut.u_id = ? " +
					"order by ut.event_time asc, ut.id asc");
			
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			if(p.startDateString != null) {
				pstmt.setTimestamp(idx++, p.startDate);
			}
			if(p.endDateString != null) {
				pstmt.setTimestamp(idx++, p.endDate);
			}
			pstmt.setInt(idx++, p.uId);
			
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
					if(isGreaterThanBreakDistance(pstmtDistance, prevX, prevY, f.coordinates[0], f.coordinates[1], p.mps)) {
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
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
			if(pstmtDistance != null) try {pstmtDistance.close();} catch (Exception e) {}
		}
		
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
	

	private TravelDistance getTravelDistances(Connection sd, String filename, String user, ArrayList<ArrayList<UserTrailFeature>> featureList) throws SQLException {
		
		TravelDistance td = new TravelDistance(user, 0);
		
		return td;
	}

}
