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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.Bundle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Services for managing bundles
 */

@Path("/bundle")
public class BundleService extends Application {
	
	Authorise a = null;

	private static Logger log =
			 Logger.getLogger(BundleService.class.getName());
	
	LogManager lm = new LogManager(); // Application log
	
	Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	
	public BundleService() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.SECURITY);
		authorisations.add(Authorise.ORG);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ANALYST);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get the bundle settings for passed in survey
	 */
	@GET
	@Path("/settings/{survey_id}")
	@Produces("application/json")
	public Response getBundleSettings(
			@Context HttpServletRequest request,
			@PathParam("survey_id") int sId
			) { 

		Bundle bundle = new Bundle();
		
		Response response = null;
		String connectionString = "surveyKPI-getBundleSettings";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());	
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		String sql = "select bundle_roles from bundle "
				+ "where group_survey_ident = (select group_survey_ident from survey where s_id = ?)";
		PreparedStatement pstmt = null;
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				bundle.bundleRoles = rs.getBoolean("bundle_roles");
			}

			String resp = gson.toJson(bundle);
			response = Response.ok(resp).build();
			
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	

}

