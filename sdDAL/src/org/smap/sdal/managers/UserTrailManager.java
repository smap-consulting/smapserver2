package org.smap.sdal.managers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Trail;
import org.smap.sdal.model.UserTrailFeature;
import org.smap.sdal.model.UserTrailPoint;



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
		public int distance;	// meters
		
		public TravelDistance(String user, int distance) {
			this.user = user;
			this.distance = distance;
		}
	}
	
	public class Parameters {
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
	public String generateKML(Connection sd, int pId, HashMap<String, String> params, String basePath) throws SQLException, IOException {
		
		String filename = String.valueOf(UUID.randomUUID()) + ".kml";

		Parameters pobj = new Parameters(params);   // Extract parameters
		ArrayList<UserTrailFeature> featureList = getGeomFeatures(sd, pId, pobj, true);  // Get the features

		/*
		 * Export KML
		 */		
		GeneralUtilityMethods.createDirectory(basePath + "/reports");
		String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
		File tempFile = new File(filepath);
		PrintWriter writer = new PrintWriter(tempFile);
		writeKmlHeader(writer);


		HashMap<String, Integer> userColors = new HashMap<> ();
		DecimalFormat df = new DecimalFormat("#.0000");
		int userCount = 0;
		for(UserTrailFeature feature : featureList) {
			if(feature.points.size() == 0) {
				// ignore
			} else if(feature.points.size() == 1) { 
				// point
				writer.println("<Placemark>");
				writer.println("<name>");
				writer.println(feature.ident);
				writer.println("</name>");
				writer.println("<styleUrl>#trail_style</styleUrl>");
				
				writer.println("<Point>");

				writer.println("<coordinates>");
				for(UserTrailPoint p : feature.points) {					
					String lon = df.format(p.coordinates[0]);
					String lat = df.format(p.coordinates[1]);
					writer.println(lon + "," + lat + ",0");
				}
				writer.println("</coordinates>");

				writer.println("</Point>");
				writer.println("</Placemark>");

			} else if(feature.points.size() > 1) {	// line
				// Add line
				writer.println("<Placemark>");
				
				// Add the color
				writer.print("<styleUrl>#ts");
				Integer colorIdx = userColors.get(feature.ident);
				if(colorIdx == null) {
					colorIdx = new Integer(userCount++ % 3);
					userColors.put(feature.ident, colorIdx);
				}
				writer.print(colorIdx);
				writer.println("</styleUrl>");
				
				writer.println("<name>");
				writer.println(feature.ident);
				writer.println("</name>");
				writer.println("<LineString>");
				writer.println("<tessellate>1</tessellate>");

				writer.println("<coordinates>");
				for(UserTrailPoint p : feature.points) {	
					String lon = df.format(p.coordinates[0]);
					String lat = df.format(p.coordinates[1]);
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
	 * Generate a GeoJson file
	 */
	public ArrayList<UserTrailFeature> generateGeoJson(Connection sd, int pId, HashMap<String, String> params, boolean wgs84) throws SQLException, IOException {
		
		
		Trail trail = new Trail();
		trail.features = new ArrayList<UserTrailFeature> ();
		
		Parameters pobj = new Parameters(params);   // Extract parameters
		ArrayList<UserTrailFeature> featureList = getGeomFeatures(sd, pId, pobj, wgs84);  // Get the features

		return featureList;
	
	}
	
	/*
	 * Generate a Distance file in the reports directory and return its name
	 */
	public String generateDistanceReport(Connection sd, int pId, HashMap<String, String> params, String basePath) throws SQLException, IOException {
		String filename = String.valueOf(UUID.randomUUID()) + ".xlsx";
		
		Parameters p = new Parameters(params);   // Extract parameters
		ArrayList<UserTrailFeature> featureList = getGeomFeatures(sd, pId, p, true);  // Get the features
		HashMap<String, TravelDistance> distances = getTravelDistances(sd, filename, featureList);
		
		GeneralUtilityMethods.createDirectory(basePath + "/reports");
		String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
		FileOutputStream outputStream = new FileOutputStream(filepath);
		
		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet(localisation.getString("rep_data"));
		
		/*
		 * Set styles
		 */
		CreationHelper createHelper = wb.getCreationHelper();
		Font boldFont = wb.createFont();
		boldFont.setBold(true);
		
		CellStyle settingsStyle = wb.createCellStyle();
		settingsStyle.setFillForegroundColor(IndexedColors.CORNFLOWER_BLUE.getIndex());
		settingsStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		
		CellStyle headerStyle = wb.createCellStyle();
		headerStyle.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
		headerStyle.setFont(boldFont);
		headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		
		CellStyle dateStyle = wb.createCellStyle();
		dateStyle.setDataFormat(createHelper.createDataFormat().getFormat("d/m/yyyy"));
		
		int rowIdx = 0;
		
		/*
		 * Write the settings information
		 */
		sheet.setColumnWidth(1, 25 * 256);
		Row row = sheet.createRow(rowIdx++);
		Cell cell = row.createCell(0);
        cell.setCellStyle(settingsStyle);
        cell.setCellValue(localisation.getString("c_from"));
        
        cell = row.createCell(1);
        cell.setCellStyle(dateStyle);
        if(tz != null) {        	
        	cell.setCellValue(GeneralUtilityMethods.localDate(new Date(p.startDate.getTime()), tz));
        } else {
        	cell.setCellValue(p.startDate);
        }  
        
        row = sheet.createRow(rowIdx++);
    	cell = row.createCell(0);
        cell.setCellStyle(settingsStyle);
        cell.setCellValue(localisation.getString("c_to"));
        
        cell = row.createCell(1);
        cell.setCellStyle(dateStyle);
        if(tz != null) {
        	cell.setCellValue(GeneralUtilityMethods.localDate(new Date(p.endDate.getTime()), tz));
        } else {
        	cell.setCellValue(p.endDate);
        }
     
        // Timezone
        row = sheet.createRow(rowIdx++);
    	cell = row.createCell(0);
        cell.setCellStyle(settingsStyle);
        cell.setCellValue(localisation.getString("a_tz"));
        
        cell = row.createCell(1);
        cell.setCellValue(tz);
		
        rowIdx++;	// Blank line
        
        /*
         * Write the column headings
         */
        row = sheet.createRow(rowIdx++);
        cell = row.createCell(0);
        cell.setCellStyle(headerStyle);
        cell.setCellValue(localisation.getString("mf_u"));
        
        cell = row.createCell(1);
        cell.setCellStyle(headerStyle);
        cell.setCellValue(localisation.getString("rep_distance"));
        
        /*
         * Write the data
         */
        DataFormat format = wb.createDataFormat();
        CellStyle distanceCellStyle = wb.createCellStyle();
        distanceCellStyle.setDataFormat(format.getFormat("#,##0"));
        
        for(String user : distances.keySet()) {
        	TravelDistance distance = distances.get(user);
	        row = sheet.createRow(rowIdx++);
	        cell = row.createCell(0);
	        cell.setCellValue(distance.user);
	        
	        cell = row.createCell(1);
	        cell.setCellStyle(distanceCellStyle);
	        cell.setCellValue(distance.distance);
        }
        
        /*
         * Finalise
         */
		wb.write(outputStream);
		wb.close();
		outputStream.close();
		return filename;
	}
	
	private void writeKmlHeader(PrintWriter writer) {
		writer.println("<?xml version=\"1.0\" encoding=\"utf-8\" ?>");
		writer.println("<kml xmlns=\"http://www.opengis.net/kml/2.2\"");
		writer.println("xmlns:gx=\"http://www.google.com/kml/ext/2.2\">");
		
		writer.println("<Document>");
		
		writer.println("<Style id=\"ts0\">");
		writer.println("<LineStyle>");
		writer.println("<color>ff0000ff</color>");
		writer.println("<width>5</width>");
		writer.println("</LineStyle>");
		writer.println("</Style>");
		
		writer.println("<Style id=\"ts1\">");
		writer.println("<LineStyle>");
		writer.println("<color>ff00ff00</color>");
		writer.println("<width>5</width>");
		writer.println("</LineStyle>");
		writer.println("</Style>");
		
		writer.println("<Style id=\"ts2\">");
		writer.println("<LineStyle>");
		writer.println("<color>ffff0000</color>");
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
	private ArrayList<UserTrailFeature> getGeomFeatures(Connection sd, int pId, Parameters p, boolean wgs84) throws SQLException {
		
		ArrayList<UserTrailFeature> featureList = new ArrayList<> ();
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtDistance = null;
		PreparedStatement pstmtUsers = null;
		
		try {
			String sqlUsersProject = "select u.id, u.ident, u.name from users u, user_project up "
					+ "where u.id = up.u_id "
					+ "and not temporary "
					+ "and up.p_id = ?";
			String sqlSingleUser = "select u.id, u.ident, u.name from users u "
					+ "where u.id = ? ";
			
			pstmtUsers = p.uId > 0 ? sd.prepareStatement(sqlSingleUser) : sd.prepareStatement(sqlUsersProject);
			pstmtUsers.setInt(1,p.uId > 0 ? p.uId : pId);
			
			String sqlDistance = "SELECT ST_Distance( "
					+ "ST_Transform(?::geometry, 3857),"
					+ "ST_Transform(?::geometry, 3857)"
					+ ")";
			pstmtDistance = sd.prepareStatement(sqlDistance);
	
			StringBuffer sql = null;
			if(wgs84) {
				sql = new StringBuffer("SELECT ut.id as id, " +
						"ST_X(the_geom::geometry) as x, " +		
						"ST_Y(the_geom::geometry) as y, " +
						"ut.event_time as event_time, " +
						"extract(epoch from ut.event_time) * 1000 as raw_time " + 
						"FROM user_trail ut, users u  " +
						"where u.id = ut.u_id ");
			} else {
				sql = new StringBuffer("SELECT ut.id as id, " +
						"ST_X(ST_Transform(ut.the_geom, 3857)) as x, " +		
						"ST_Y(ST_Transform(ut.the_geom, 3857)) as y, " +
						"ut.event_time as event_time, " +
						"extract(epoch from ut.event_time) * 1000 as raw_time " + 
						"FROM user_trail ut, users u  " +
						"where u.id = ut.u_id ");
			}
			
			if(p.startDateString != null) {
				sql.append("and ut.event_time >= ? ");
			}
			if(p.endDateString != null) {
				sql.append("and ut.event_time < ? ");
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
			
			/*
			 * Process for each user
			 */
			ResultSet rsUsers = pstmtUsers.executeQuery();
			log.info("Getting users for report: " + pstmtUsers.toString());
			while (rsUsers.next()) {		
				pstmt.setInt(idx, rsUsers.getInt("id"));	
				String userIdent = rsUsers.getString("ident");
				String userName = rsUsers.getString("name");
			
				UserTrailFeature feature = new UserTrailFeature(userIdent, userName);
			
				boolean havePrev = false;
				Double prevX = 0.0;
				Double prevY = 0.0;
				log.info("Get location features: " + pstmt.toString());
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					UserTrailPoint f = new UserTrailPoint();
					f.coordinates[0] = rs.getDouble("x");
					f.coordinates[1] = rs.getDouble("y");
					f.rawTime = rs.getLong("raw_time");
					if(havePrev) {
						if(isGreaterThanBreakDistance(pstmtDistance, prevX, prevY, f.coordinates[0], f.coordinates[1], p.mps, wgs84)) {
							featureList.add(feature);
							feature = new UserTrailFeature(userIdent, userName);
						}
					} 
					prevX = f.coordinates[0];
					prevY = f.coordinates[1];
					havePrev = true;
					feature.points.add(f);
				}
				featureList.add(feature);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
			if(pstmtDistance != null) try {pstmtDistance.close();} catch (Exception e) {}
			if(pstmtUsers != null) try {pstmtUsers.close();} catch (Exception e) {}
		}
		
		return featureList;
	}
	
	private boolean isGreaterThanBreakDistance(PreparedStatement pstmtDistance, double prevX, double prevY, double x, double y, 
			int breakDistance, boolean wgs84) throws SQLException {
		
		StringBuilder p1 = wgs84 ? new StringBuilder("SRID=4326;POINT(") : new StringBuilder("SRID=3857;POINT(");
		p1.append(prevX).append(" ").append(prevY).append(")");
		StringBuilder p2 = wgs84 ? new StringBuilder("SRID=4326;POINT(") : new StringBuilder("SRID=3857;POINT(");
		p2.append(x).append(" ").append(y).append(")");
		
		pstmtDistance.setString(1, p1.toString());
		pstmtDistance.setString(2, p2.toString());
		
		ResultSet rs = pstmtDistance.executeQuery();
		if(rs.next()) {
			return rs.getInt(1) > breakDistance;
		}
		return false;
	}
	

	private HashMap<String, TravelDistance> getTravelDistances(Connection sd, String filename, ArrayList<UserTrailFeature> featureList) throws SQLException {
		
		HashMap<String, TravelDistance> distances = new HashMap<>();
		
		String sql = "select ST_Length(the_geog) "
				+ "from (select ST_GeographyFromText(?) As the_geog) as foo";

		PreparedStatement pstmt = sd.prepareStatement(sql);
		ResultSet rs;
		try {
		
			for(UserTrailFeature f : featureList) {
				TravelDistance td = distances.get(f.ident);
				if(td == null) {
					td = new TravelDistance(f.ident, 0);
				}
				if(f.points.size() > 1) {				
					StringBuilder lineString = new StringBuilder("SRID=4326;LINESTRING(");
					boolean first = true;
					for(UserTrailPoint point : f.points) {
						if(!first) {
							lineString.append(",");
						}
						first = false;
						lineString.append(point.coordinates[0])
							.append(" ")
							.append(point.coordinates[1]);
					}
					lineString.append(")");
					
					pstmt.setString(1, lineString.toString());
                    //log.info("Report: " + pstmt.toString());
					rs = pstmt.executeQuery();
					if(rs.next()) {
						td.distance += rs.getInt(1);
					}			
				}
				distances.put(f.ident, td);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
		
		return distances;
	}

}
