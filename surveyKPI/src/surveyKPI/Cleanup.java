package surveyKPI;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
				surveys.put(GeneralUtilityMethods.convertDisplayNameToFileName(rs.getString(1), false), rs.getString(1));
			}
			responseVal = Response.ok(gson.toJson(surveys)).build();
				
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error: " + e.getMessage()).build();
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
		PreparedStatement pstmtAge = null;
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
				
				/*
				 * Get the date that this survey was erased
				 * The survey ident contains the initial survey id, this survey id would have
				 *  been marked as erased at the same time as any later versions of the survey. 
				 *  Hence use that date.
				 */
				try {
					int sId = Integer.valueOf(sIdent.substring(sIdent.indexOf('_') + 1));
					sql = "select log_time from log where s_id = ? and event = 'erase'";
					pstmtAge = sd.prepareStatement(sql);
					pstmtAge.setInt(1, sId);
					rs = pstmtAge.executeQuery();
					if(rs.next()) {
						survey.put("erased", rs.getDate(1).toString());
					}
					
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
			}
			responseVal = Response.ok(gson.toJson(survey)).build();
				
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error: " + e.getMessage()).build();
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
			if(pstmtAge != null) try {pstmtAge.close();}catch(Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}
	
	/*
	 * Get survey details from an ident
	 */
	@POST
	@Produces("application/json")
	@Path("/deletetemplate")
	public Response deleteSurveyTemplate (@Context HttpServletRequest request, 
			@FormParam("path") String path,
			@Context HttpServletResponse response) {

		Response responseVal = null;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - cleanup - survey details";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		log.info("Request to delete template: " + path);

		try {
			if(path.startsWith("templates/")) {
					
				String basePath = GeneralUtilityMethods.getBasePath(request);
				File f = new File(basePath + "/" + path);
				if(f.exists()) {
					f.delete();
					
					log.info("Cleanup: Deleted template file: " + f.getAbsolutePath());
					responseVal = Response.status(Status.OK).entity("").build();
				} else {
					Response.status(Status.NOT_FOUND).build();
				}
			
			} else {
					log.info("Error: File " + path + " is not a template");
					responseVal = Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error: File " + path + " is not a template").build();
			}
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.INTERNAL_SERVER_ERROR).entity("Error: " + e.getMessage()).build();
		} finally {

			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}


}
