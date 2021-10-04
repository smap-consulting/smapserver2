package surveyKPI;

/*
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

*/

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.constants.SmapExportTypes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/usertrail")
public class UserTrail extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(UserTrail.class.getName());
	
	public class Feature {
		public int id;
		public double[] coordinates = new double[2];
		public Timestamp time;
		public long rawTime;
	}
	
	public class Trail {
		String userName = null;
		public ArrayList<Feature> features = null;
		
	}
	
	public class Survey {
		public int id;
		public double[] coordinates = new double[2];
		public Timestamp time;
	}
	
	public class SurveyList {
		String userName = null;
		public ArrayList<Survey> surveys = null;
		
	}
	
	public UserTrail() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
	}

	
	/*
	 * Get the trail of points 
	 */
	@GET
	@Produces("application/json")
	@Path("/trail")
	public Response getTrail(@Context HttpServletRequest request, 
			@QueryParam("userId") int uId,
			@QueryParam("startDate") long start_t,
			@QueryParam("endDate") long end_t) {

		Response response = null;

		Timestamp startDate = new Timestamp(start_t);
		Timestamp endDate = new Timestamp(end_t);

		String user = request.getRemoteUser();
		String connectionString = "usertrail - trail";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		a.isValidUser(sd, user, uId);
		// End Authorisation
		
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		try {
			
			StringBuffer sql = new StringBuffer("SELECT ut.id as id, ST_X(ST_Transform(ut.the_geom, 3857)) as x, " +
						"ST_Y(ST_Transform(ut.the_geom, 3857)) as y, ut.event_time as event_time, " +
						"extract(epoch from ut.event_time) * 1000 as raw_time, " + 
						"u.name as user_name " +	
					"FROM user_trail ut, users u  " +
					"where u.id = ut.u_id ");
			
			if(start_t > 0) {
				sql.append("and ut.event_time >= ? ");
			}
			if(end_t > 0) {
				sql.append("and ut.event_time <  ? ");
			}
			sql.append("and ut.u_id = ? " +
					"order by ut.event_time asc limit 5000");
			
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			if(start_t > 0) {
				pstmt.setTimestamp(idx++, startDate);
			}
			if(end_t > 0) {
				pstmt.setTimestamp(idx++, endDate);
			}
			pstmt.setInt(idx++, uId);

			log.info("Get User Trail: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			 
			Trail trail = new Trail();
			trail.features = new ArrayList<Feature> ();
			 
			while (resultSet.next()) {
				
				if(trail.userName == null) {
					trail.userName = resultSet.getString("user_name");
				}
				
				Feature f = new Feature();
				f.id = resultSet.getInt("id");
				f.time = resultSet.getTimestamp("event_time");	
				f.rawTime = resultSet.getLong("raw_time");
				f.coordinates[0] = resultSet.getDouble("x");
				f.coordinates[1] = resultSet.getDouble("y");
				trail.features.add(f);
			}
			 
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(trail);
			response = Response.ok(resp).build();
			 

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Exception", e);
			response = Response.serverError().entity(e.getMessage()).build();

		} finally {
			try {if(resultSet != null) {resultSet.close();}	} catch (SQLException e) {	}	
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Export the locations
	 */
	@GET
	@Produces("application/json")
	@Path("/export")
	public Response export(@Context HttpServletRequest request, 
			@QueryParam("userId") int uId,
			@QueryParam("startDate") long start_t,
			@QueryParam("endDate") long end_t,
			@PathParam("filename") String filename,
			@QueryParam("format") String format,
			@Context HttpServletResponse response) {

		ResponseBuilder builder = Response.ok();
		Response responseVal = null;

		Timestamp startDate = new Timestamp(start_t);
		Timestamp endDate = new Timestamp(end_t);

		String user = request.getRemoteUser();
		String connectionString = "usertrail - trail";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		a.isValidUser(sd, user, uId);
		// End Authorisation
		if(filename == null) {
			filename = "locations";
		}
		
		PreparedStatement pstmtDistance = null;
		PreparedStatement pstmt = null;

		try {
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String escapedFileName = null;
			try {
				escapedFileName = URLDecoder.decode(filename, "UTF-8");
				escapedFileName = URLEncoder.encode(escapedFileName, "UTF-8");
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			escapedFileName = escapedFileName.replace("+", " "); // Spaces ok for file name within quotes
			escapedFileName = escapedFileName.replace("%2C", ","); // Commas ok for file name within quotes
			
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
			
			if(start_t > 0) {
				sql.append("and ut.event_time >= ? ");
			}
			if(end_t > 0) {
				sql.append("and ut.event_time <  ? ");
			}
			sql.append("and ut.u_id = ? " +
					"order by ut.event_time asc, ut.id asc");
			
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			if(start_t > 0) {
				pstmt.setTimestamp(idx++, startDate);
			}
			if(end_t > 0) {
				pstmt.setTimestamp(idx++, endDate);
			}
			pstmt.setInt(idx++, uId);
			
			/*
			 * Export KML
			 */
			String basePath = GeneralUtilityMethods.getBasePath(request);					
			String filepath = basePath + "/temp/" + String.valueOf(UUID.randomUUID()) + ".kml";	// Use a random sequence to keep survey name unique
			File tempFile = new File(filepath);
			PrintWriter writer = new PrintWriter(tempFile);
			writeKmlHeader(writer);
			
			ArrayList<ArrayList<Feature>> featureList = getKmlFeatures(pstmt, pstmtDistance, 200);
			DecimalFormat df = new DecimalFormat("#.0000");
			for(ArrayList<Feature> features : featureList) {
				if(features.size() == 0) {
					// ignore
				} else if(features.size() == 1) { 
					// point
					writer.println("<Placemark>");
					writer.println("<styleUrl>#trail_style</styleUrl>");
					writer.println("<Point>");
					
					writer.println("<coordinates>");
					for(Feature f : features) {					
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
					for(Feature f : features) {	
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

			if(tempFile.exists()) {
				builder = Response.ok(tempFile);
				builder.header("Content-Disposition", "attachment;Filename=\"" + escapedFileName + ".kml\"");			
				builder.header("Content-type","application/vnd.google-earth.kml+xml");
				responseVal = builder.build();
			} else {
				throw new ApplicationException(localisation.getString("msg_no_data"));
			}
			
			/*
			 
			String basePath = GeneralUtilityMethods.getBasePath(request);					
			String filepath = basePath + "/temp/" + String.valueOf(UUID.randomUUID());	// Use a random sequence to keep survey name unique
			String database_name = "survey_definitions";
			String scriptPath = basePath + "_bin" + File.separator + "getshape.sh";
			Process proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", scriptPath + " " + 
					database_name + " " +
					"na " +		// Not applicable
					"\"" + pstmt.toString() + "\" " +
					filepath + 
					" " + format});
			int code = proc.waitFor();
			if(code > 0) {
				int len;
				if ((len = proc.getErrorStream().available()) > 0) {
					byte[] buf = new byte[len];
					proc.getErrorStream().read(buf);
					log.info("Command error:\t\"" + new String(buf) + "\"");
				}
			} else {
				int len;
				
				if ((len = proc.getErrorStream().available()) > 0) {
					byte[] buf = new byte[len];
					proc.getErrorStream().read(buf);
					log.info("Command error:\t\"" + new String(buf) + "\"");
				}
				
				if ((len = proc.getInputStream().available()) > 0) {
					byte[] buf = new byte[len];
					proc.getInputStream().read(buf);
					log.info("Completed getshape process:\t\"" + new String(buf) + "\"");
				}
			}
			
			log.info("Process exitValue: " + code);
			if(code == 0) {
				File file = new File(filepath + ".zip");

				if(file.exists()) {
					builder = Response.ok(file);
					if(format.equals(SmapExportTypes.KML)) {
						builder.header("Content-Disposition", "attachment;Filename=\"" + escapedFileName + ".kmz\"");
					} else {
						builder.header("Content-Disposition", "attachment;Filename=\"" + escapedFileName + ".zip\"");
					}
					builder.header("Content-type","application/zip");
					responseVal = builder.build();
				} else {
					throw new ApplicationException(localisation.getString("msg_no_data"));
				}

			} else {
				throw new ApplicationException("Error exporting file");
			}
			*/
			 

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtDistance != null) {pstmtDistance.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;
	}

	/*
	 * Get the survey locations 
	 */
	@GET
	@Produces("application/json")
	@Path("/surveys")
	public Response getSurveyLocations(@Context HttpServletRequest request, 
			@QueryParam("projectId") int projectId,
			@QueryParam("userId") int uId,
			@QueryParam("startDate") long start_t,
			@QueryParam("endDate") long end_t) {

		Response response = null;

		log.info("Get Survey Locations: Project id:" + projectId);

		String user = request.getRemoteUser();
		String connectionString = "usertrail - surveys";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		a.isValidProject(sd, request.getRemoteUser(), projectId);
		// End Authorisation
		
		Timestamp startDate = new Timestamp(start_t);
		Timestamp endDate = new Timestamp(end_t);
		
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		try {
			
			String sql = "SELECT t.id as id, ST_X(ST_Transform(t.the_geom, 3857)) as x, " +		// keep this
						"ST_Y(ST_Transform(t.the_geom, 3857)) as y, t.completion_time as completion_time, " +	// keep this	
						"u.name as user_name " +	
					"FROM task_completion t, user_project up, users u  " +
					"where up.p_id = ? " + 	
					"and up.u_id = t.u_id " +
					"and up.u_id = u.id " +
					"and t.completion_time >= ? " +
					"and t.completion_time < ? " +
					"and t.u_id = ? " +
					"order by t.completion_time asc;";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, projectId);
			pstmt.setTimestamp(2, startDate);
			pstmt.setTimestamp(3, endDate);
			pstmt.setInt(4, uId);

			log.info("Events List: " + sql + " : " + uId + " : " + projectId + " : " + startDate + " : " + endDate);

			resultSet = pstmt.executeQuery();
			 
			SurveyList sl = new SurveyList();
			sl.surveys = new ArrayList<Survey> ();
			 
			while (resultSet.next()) {
				
				if(sl.userName == null) {
					sl.userName = resultSet.getString("user_name");
				}
				
				Survey s = new Survey();
				s.id = resultSet.getInt("id");
				s.time = resultSet.getTimestamp("completion_time");	
				s.coordinates[0] = resultSet.getDouble("x");
				s.coordinates[1] = resultSet.getDouble("y");
				sl.surveys.add(s);
			}
			 
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(sl);
			response = Response.ok(resp).build();
			 

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Exception", e);
			response = Response.serverError().entity(e.getMessage()).build();

		} finally {
			try {if(resultSet != null) {resultSet.close();}	} catch (SQLException e) {	}	
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
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
	ArrayList<ArrayList<Feature>> getKmlFeatures(PreparedStatement pstmt, PreparedStatement pstmtDistance, int breakDistance) throws SQLException {
		ArrayList<ArrayList<Feature>> featureList = new ArrayList<> ();
		
		ArrayList<Feature> features = new ArrayList<Feature> ();
		boolean havePrev = false;
		Double prevX = 0.0;
		Double prevY = 0.0;
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			Feature f = new Feature();
			f.coordinates[0] = rs.getDouble("x");
			f.coordinates[1] = rs.getDouble("y");
			if(havePrev) {
				if(isGreaterThanBreakDistance(pstmtDistance, prevX, prevY, f.coordinates[0], f.coordinates[1], breakDistance)) {
					featureList.add(features);
					features = new ArrayList<Feature> ();
					havePrev = false;
					
				}
			} else {
				havePrev = true;
			}
			prevX = f.coordinates[0];
			prevY = f.coordinates[1];
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

