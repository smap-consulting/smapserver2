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

import java.sql.Connection;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.MiscPDFManager;

/*
 * Creates a PDF
 * HTML Fragments 
 *   <h3>  - Use for group Labels
 *   .group - group elements
 *   .hint - Hints
 */

@Path("/usage/{oId}")
public class UsageReports extends Application {
	
	Authorise a = new Authorise(null, Authorise.ORG);
	
	private static Logger log =
			 Logger.getLogger(UsageReports.class.getName());
	
	//public static Font WingDings = null;
	//public static Font defaultFont = null;

	
	@GET
	@Produces("application/x-download")
	public Response getMonthly (@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("oId") int oId,
			@QueryParam("month") int month,			// 1 - 12
			@QueryParam("year") int year,
			@QueryParam("period") String period) throws Exception {

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("createPDF");	
		a.isAuthorised(sd, request.getRemoteUser());		
		// End Authorisation 
		
		// Get the base path
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		// Get the organisation name
		String org_name = GeneralUtilityMethods.getOrganisationName(sd, oId);
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";	// Set default for timezone
			
			MiscPDFManager pm = new MiscPDFManager(localisation, tz);
			pm.createUsagePdf(
					sd,
					response.getOutputStream(),
					basePath, 
					response,
					oId,
					month,
					year,
					period,
					org_name);
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			throw new Exception("Exception: " + e.getMessage());
		} finally {
			
			SDDataSource.closeConnection("createPDF", sd);	
			
		}
		return Response.ok("").build();
	}
	

}
