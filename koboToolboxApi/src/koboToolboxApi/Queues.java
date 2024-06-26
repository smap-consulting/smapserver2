package koboToolboxApi;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QueueManager;
import org.smap.sdal.managers.UserLocationManager;
import org.smap.sdal.model.Queue;
import org.smap.sdal.model.QueueItem;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Provides access to data on queues
 * This information covers all organisations and only the server owner should have access
 */
@Path("/v1/queues")
public class Queues extends Application {
	
	Authorise a = null;
	private static Logger log =
			Logger.getLogger(UserLocationManager.class.getName());

	LogManager lm = new LogManager();		// Application log

	public Queues() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);
		a = new Authorise(authorisations, null);
	}

	/*
	 * Returns counts of created and closed cases over time
	 */
	@GET
	@Path("/{queue}")
	@Produces("application/json")
	public Response getOpenClosed(@Context HttpServletRequest request,
			@PathParam("queue") String queueName) throws ApplicationException { 

		Response response = null;
		String connectionString = "API - getQueues";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		Connection cResults = null;
		try {			
			cResults = ResultsDataSource.getConnection(connectionString);
			
			QueueManager qm = new QueueManager();
			Queue q = new Queue();
			boolean error = false;
			
			if(queueName.equals(qm.SUBMISSIONS)) {
				q = qm.getSubmissionQueueData(sd);
			} else if(queueName.equals(qm.RESTORE)) {
				q = qm.getRestoreQueueData(sd);
			} else if(queueName.equals(qm.S3UPLOAD)) {
				q = qm.getS3UploadQueueData(sd);
			} else if(queueName.equals(qm.SUBEVENT)) {
				q = qm.getSubEventQueueData(sd);
			} else if(queueName.equals(qm.MESSAGE)) {
				q = qm.getMessageQueueData(sd);
			} else {
				error = true;		
			}
			
			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			if(error) {
				response = Response.ok("Unknown queue: " + queueName).build();
			} else {
				response = Response.ok(gson.toJson(q)).build();
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}

		return response;
	}
	
	/*
	 * Return errors for a queue
	 * Set period to 1 week
	 */
	@GET
	@Path("/{queue}/items")
	@Produces("application/json")
	public Response getItems(@Context HttpServletRequest request,
			@PathParam("queue") String queueName,
			@QueryParam("status") String status,
			@QueryParam("month") int month,
			@QueryParam("year") int year,
			@QueryParam("tz") String tz
			) { 

		Response response = null;
		String connectionString = "API - getQueueErrors";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		if(tz == null) {
			tz = "UTC";
		}
		
		Connection cResults = null;
		try {			
			cResults = ResultsDataSource.getConnection(connectionString);
			
			QueueManager qm = new QueueManager();
			ArrayList<QueueItem> items = new ArrayList<>();
			
			if(queueName.equals(qm.SUBMISSIONS)) {
				//qm.getSubmissionQueueEvents(sd, items);
			} if(queueName.equals(qm.S3UPLOAD)) {
				qm.getS3UploadQueueEvents(sd, items, month, year, status, tz);
			} else {
				log.info("Unknown queue name: " + queueName);
			}
			
			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			response = Response.ok(gson.toJson(items)).build();
			
		} catch (SQLException e) {
			e.printStackTrace();
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}

		return response;
	}
	
}

