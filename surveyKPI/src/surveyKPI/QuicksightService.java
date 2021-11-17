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

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.notifications.interfaces.QuickSight;
import org.smap.notifications.interfaces.STS;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.OrganisationManager;
import org.smap.sdal.model.DashboardDetails;

import com.amazonaws.auth.BasicSessionCredentials;

/*
 * Manages access to quicksight dashboard
 */

@Path("/quicksight")
public class QuicksightService extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(QuicksightService.class.getName());
	
	LogManager lm = new LogManager();		// Application log

	public QuicksightService() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.DASHBOARD);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get the dashboard url
	 */
	@GET
	@Produces("application/json")
	@Path("/dashboard")
	public Response getDashboard(
			@Context HttpServletRequest request
			) throws IOException {
		
		Response response = null;
		Connection sd = null; 
		String connection = "surveyKPI - Quicksight - getDashboard";
		
		// Authorisation - Access
		sd = SDDataSource.getConnection(connection);
		a.isAuthorised(sd, request.getRemoteUser());
		// End authorisation
	
		try {

			OrganisationManager om = new OrganisationManager(null);
			DashboardDetails dbd = om.getDashboardDetails(sd, request.getRemoteUser());
			String basePath = GeneralUtilityMethods.getBasePath(request);
			
			if(dbd != null && dbd.region != null) {
				String region = dbd.region;
				STS sts = new STS(region, basePath);
				BasicSessionCredentials credentials = sts.getSessionCredentials(dbd.roleArn, dbd.roleSessionName);
				log.info("xoxoxoxoxo accessKey: " + credentials.getAWSAccessKeyId());
				
				QuickSight quicksight = new QuickSight(region, credentials, basePath, 
						"dashboardid",
						"awsaccountid");
				
				String userArn = quicksight.registerUser(request.getRemoteUser());
				log.info("xoxoxoxoxo User ARN:  " + userArn);			
				
				String url = quicksight.getDashboardUrl(userArn);		
				log.info("xoxoxoxoxo Dashboard URL:  " + url);
				      
				response = Response.ok(url).build();
			} else {
				throw new Exception("No quicksight region specified for this organisation");
			}
	
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connection, sd);
		}
		
		return response;
	}
	
}
