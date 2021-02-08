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

/*
 * Custom reports for TDH
 */
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import utilities.TDHReportsManager;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/tdh")
public class TDH extends Application {

	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(TDH.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public TDH() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();
		
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);

		
		a = new Authorise(authorisations, null);
		
	}

	@GET
	@Path("/individual/{beneficiary}/{filename}")
	public Response getIndividual(@Context HttpServletRequest request,
			@PathParam("beneficiary") String beneficiary,			
			@PathParam("filename") String filename,
			@Context HttpServletResponse response
			) { 
		
		String connectionString = "surveyKPI-TDH - Individual";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		// End Authorisation
		
		Response responseVal = null;
		Connection cResults = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			cResults = ResultsDataSource.getConnection(connectionString);
			TDHReportsManager rm = new TDHReportsManager(localisation, "UTC");
			rm.getIndividualReport(sd,cResults,
					filename,
					request.getRemoteUser(),
					request,
					response,
					beneficiary);
			
			
		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
			responseVal = Response.serverError().build();
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			responseVal = Response.serverError().build();
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}

		return responseVal;
	}
}


