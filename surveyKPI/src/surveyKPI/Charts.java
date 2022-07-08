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
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.ChartDefn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/charts")
public class Charts extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(Charts.class.getName());


	public Charts() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
		
	}
	
	/*
	 * Save charts for a user
	 */
	@Path("/save/{sIdent}")
	@POST
	public Response saveCharts(@Context HttpServletRequest request,
			@PathParam("sIdent") int sId,
			@FormParam("chartArray") String chartArray) {
		
		Response response = null;
		String connectionString = "surveyKPI-Charts-Save";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, false);
		// End Authorisation
		
		Type type = new TypeToken<ArrayList<ChartDefn>>(){}.getType();
		Gson gson =  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		ArrayList chartDefns = gson.fromJson(chartArray, type);

		String sql = "update survey_settings set chart_view = ? "
				+ "where s_ident = ? "
				+ "and u_id = ?";

		PreparedStatement pstmt = null;
		
		try {
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, gson.toJson(chartDefns));
			pstmt.setString(2, sIdent);
			pstmt.setInt(3, uId);
			pstmt.executeUpdate();
			
			response = Response.ok("").build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Client Protocol Exception", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			if(pstmt != null) {try {pstmt.close();}catch(Exception e) {}}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}

}