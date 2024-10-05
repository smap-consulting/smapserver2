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
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.model.SMSNumber;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Services for managing SMS numbers
 */

@Path("/smsnumbers")
public class SMSNumbers extends Application {
	
	Authorise a = null;
	Authorise aOwner = null;

	private static Logger log =
			 Logger.getLogger(SMSNumbers.class.getName());
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm").create();
	
	LogManager lm = new LogManager(); // Application log
	
	public SMSNumbers() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		
		// Only allow server owners and organisational administrators to view or update number data
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
		aOwner = new Authorise(null, Authorise.ADMIN);
	
	}
	
	/*
	 * Get the sms numbers for the organisation or whole server depending on user permissions
	 */
	@GET
	@Produces("application/json")
	public Response getSMSNumbers(
			@Context HttpServletRequest request,
			@QueryParam("org") boolean orgOnly
			) { 

		Response response = null;
		String connectionString = "surveyKPI-getSMSNumbers";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());		
		// End Authorisation
			
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SMSManager mgr = new SMSManager(localisation, "UTC");
			ArrayList<SMSNumber> numbers = mgr.getOurNumbers(sd, request.getRemoteUser(), orgOnly);
			
			response = Response.ok(gson.toJson(numbers)).build();
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	

	/*
	 * Add a new number
	 */
	@Path("/number/add")
	@POST
	public Response addNmber(@Context HttpServletRequest request,
			@FormParam("ourNumber") String ourNumber,
			@FormParam("oId") int oId,
			@QueryParam("tz") String tz) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-add sms number";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aOwner.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		if(tz == null) {
			tz = "UTC";
		}
		
		String sql = "insert into sms_number (element_identifier, time_modified, our_number, o_id) "
				+ "values (gen_random_uuid(), now(), ?, ?)";
		PreparedStatement pstmt = null;
		
		try {	
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ourNumber);
			pstmt.setInt(2, oId);
			pstmt.executeUpdate();
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	/*
	 * Edit a number
	 */
	@Path("/number/edit")
	@POST
	public Response editNmber(@Context HttpServletRequest request,
			@FormParam("ourNumber") String ourNumber,
			@FormParam("oId") int oId,
			@FormParam("sIdent") String sIdent,
			@FormParam("theirNumberQuestion") String theirNumberQuestion,
			@FormParam("messageQuestion") String messageQuestion,
			@QueryParam("tz") String tz) throws SQLException { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-edit sms number";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		if(tz == null) {
			tz = "UTC";
		}
				
		PreparedStatement pstmt = null;
		
		try {	
			/*
			 * Data authorisation
			 */
			boolean isOwner = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.OWNER_ID);
			if(!isOwner) {   // Validate number
				a.isValidNumber(sd, request.getRemoteUser(), ourNumber);
			}
			if(sIdent != null) {
				a.surveyInUsersOrganisation(sd, request.getRemoteUser(), sIdent);  // Make it a super user request and ignore roles
			}
			
			/*
			 * Validation
			 * 1. TODO validate that the question names are in the specified survey
			 */
			/*
			 * Construct the SQL
			 */
			StringBuilder sql = new StringBuilder("update sms_number ")
					.append("set time_modified = now()")
					.append(", survey_ident  = ? ")
					.append(", their_number_question  = ? ")
					.append(", message_question  = ? ");
			if(isOwner) {
				sql.append(", o_id = ? ");
			}
			sql.append("where our_number = ?");
			
			/*
			 * Update
			 */
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setString(1, sIdent);
			pstmt.setString(2, theirNumberQuestion);
			pstmt.setString(3, messageQuestion);
			if(isOwner) {
				pstmt.setInt(4, oId);
			}
			pstmt.setString(5, ourNumber);
			log.info("update number: " + pstmt.toString());
			pstmt.executeUpdate();
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	/*
	 * Delete a number
	 */
	@Path("/number/{identifier}")
	@DELETE
	public Response deleteNumber(@Context HttpServletRequest request,
			@PathParam("identifier") String identifier,
			@QueryParam("tz") String tz) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-delete sms number";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aOwner.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		if(tz == null) {
			tz = "UTC";
		}
		
		String sql = "delete from sms_number "
				+ "where element_identifier::text = ?";
		PreparedStatement pstmt = null;
		
		try {	
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, identifier);
			pstmt.executeUpdate();
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
}

