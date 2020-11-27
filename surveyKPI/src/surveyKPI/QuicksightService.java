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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
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

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.notifications.interfaces.AudioProcessing;
import org.smap.notifications.interfaces.QuickSight;
import org.smap.notifications.interfaces.STS;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MiscPDFManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.Location;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.TaskBulkAction;
import org.smap.sdal.model.TaskEmailDetails;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskServerDefn;

import utilities.XLSTaskManager;

import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.quicksight.model.GetDashboardEmbedUrlRequest;
import com.amazonaws.services.quicksight.model.GetDashboardEmbedUrlResult;
import com.amazonaws.services.quicksight.model.IdentityType;
import com.amazonaws.services.quicksight.model.RegisterUserResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Manages Tasks
 */

@Path("/quicksight")
public class QuicksightService extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(QuicksightService.class.getName());
	
	LogManager lm = new LogManager();		// Application log

	public QuicksightService() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.MANAGE_TASKS);
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

			String region = "us-east-1";
			STS sts = new STS(region);
			BasicSessionCredentials credentials = sts.getSessionCredentials();
			log.info("xoxoxoxoxo accessKey: " + credentials.getAWSAccessKeyId());
			
			QuickSight quicksight = new QuickSight(region, credentials);
			
			String userArn = quicksight.registerUser(request.getRemoteUser());
			log.info("xoxoxoxoxo User ARN:  " + userArn);			
			
			String url = quicksight.getDashboardUrl(userArn);		
			log.info("xoxoxoxoxo Dashboard URL:  " + url);
			      
			response = Response.ok(url).build();
	
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connection, sd);
		}
		
		return response;
	}
	
}
