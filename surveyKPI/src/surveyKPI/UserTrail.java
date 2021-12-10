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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.BackgroundReportsManager;
import org.smap.sdal.managers.UserTrailManager;
import org.smap.sdal.model.Trail;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/usertrail")
public class UserTrail extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(UserTrail.class.getName());
	
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
			@QueryParam("startDate") String start_t,
			@QueryParam("endDate") String end_t,
			@QueryParam("mps") int mps,
			@QueryParam("tz") String tz) {

		Response response = null;
		
		if(tz == null) {
			tz = "UTC";
		}
		
		if(mps == 0) {
			mps = 200;
		}

		//Timestamp startDate = new Timestamp(start_t);
		//Timestamp endDate = new Timestamp(end_t);

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
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// Create a params object
			HashMap<String, String> params = new HashMap<>();
			params.put(BackgroundReportsManager.PARAM_START_DATE, start_t);
			params.put(BackgroundReportsManager.PARAM_END_DATE, end_t);
			params.put(BackgroundReportsManager.PARAM_USER_ID, String.valueOf(uId));
			params.put(BackgroundReportsManager.PARAM_MPS, String.valueOf(mps));
			
			UserTrailManager utm = new UserTrailManager(localisation, tz);
			Trail trail = new Trail();
			trail.features = utm.generateGeoJson(sd, 0, params, false);	// Set project to 0 as we are getting data for a single user, specify 3857 coordinate system
			
			/*
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
					"order by ut.event_time asc");
			
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
			trail.features = new ArrayList<UserTrailPoint> ();
			 
			while (resultSet.next()) {
				
				if(trail.userName == null) {
					trail.userName = resultSet.getString("user_name");
				}
				
				UserTrailPoint f = new UserTrailPoint();
				f.id = resultSet.getInt("id");
				f.time = resultSet.getTimestamp("event_time");	
				f.rawTime = resultSet.getLong("raw_time");
				f.coordinates[0] = resultSet.getDouble("x");
				f.coordinates[1] = resultSet.getDouble("y");
				trail.features.add(f);
			}
			 */
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(trail);
			response = Response.ok(resp).build();
			 

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();

		} finally {
			try {if(resultSet != null) {resultSet.close();}	} catch (SQLException e) {	}	
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
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
			@QueryParam("endDate") long end_t,
			@QueryParam("tz") String tz) {

		Response response = null;
		
		if(tz == null) {
			tz = "UTC";
		}

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



	

}

