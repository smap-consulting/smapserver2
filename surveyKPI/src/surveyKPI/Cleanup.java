package surveyKPI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Export a log report in xlsx
 */
@Path("/cleanup")
public class Cleanup extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(Cleanup.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public Cleanup() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get surveys for a project
	 */
	@GET
	@Produces("application/json")
	@Path("/templates/{project}")
	public Response getTemplateNames (@Context HttpServletRequest request, 
			@PathParam("project") int pId,
			@Context HttpServletResponse response) {

		Response responseVal = null;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - cleanup - template names";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		PreparedStatement pstmt = null;
		HashMap<String, String> surveys = new HashMap<> ();
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {
			String sql = "select display_name from survey where p_id = ?";
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, pId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				surveys.put(GeneralUtilityMethods.convertDisplayNameToFileName(rs.getString(1)), rs.getString(1));
			}
			responseVal = Response.ok(gson.toJson(surveys)).build();
				
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}
	
	/*
	 * Get survey details from an ident
	 */
	@GET
	@Produces("application/json")
	@Path("/survey/{ident}")
	public Response getSurveyDetails (@Context HttpServletRequest request, 
			@PathParam("ident") String sIdent,
			@Context HttpServletResponse response) {

		Response responseVal = null;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - cleanup - survey details";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		PreparedStatement pstmt = null;
		HashMap<String, String> survey = new HashMap<> ();
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {
			String sql = "select display_name from survey where ident = ?";
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sIdent);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				survey.put("exists", "yes");
				survey.put("name", rs.getString(1));
			} else {
				survey.put("exists", "no");
			}
			responseVal = Response.ok(gson.toJson(survey)).build();
				
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}


}
