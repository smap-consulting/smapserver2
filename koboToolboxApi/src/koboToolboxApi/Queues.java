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

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QueueManager;
import org.smap.sdal.managers.UserLocationManager;
import org.smap.sdal.model.Queue;
import org.smap.sdal.model.QueueItem;
import org.smap.sdal.model.QueueTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
	 * Returns counts of created and closed queue entries in the last minute
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
		
		try {			
			
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
			} else if(queueName.equals(qm.MESSAGE_DEVICE)) {
				q = qm.getMessageDeviceQueueData(sd);
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
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Returns history of queue changes
	 * 
	 */
	@GET
	@Path("/history/{interval}")
	@Produces("application/json")
	public Response getHistory(@Context HttpServletRequest request,
			@PathParam("interval") int interval,
			@QueryParam("tz") String tz) throws ApplicationException { 

		Response response = null;
		String connectionString = "API - getQueueHistory";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		String sql = "select "
				+ "to_char(timezone(?, recorded_at), 'YYYY-MM-DD HH24:MI:SS') as recorded_at,"
				+ "payload "
				+ "from monitor_data "
				+ "where recorded_at > now() - interval '" + interval + " days' "
				+ "order by recorded_at desc";
		PreparedStatement pstmt = null;
		
		Type type = new TypeToken<HashMap<String, Queue>>() {}.getType();
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		ArrayList<QueueTime> data = new ArrayList<>();
		
		try {			
			
			if(tz == null) {
				tz = GeneralUtilityMethods.getOrganisationTZ(sd, 
						GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser()));
			}
			tz = (tz == null) ? "UTC" : tz;
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tz);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				data.add(new QueueTime(rs.getString("recorded_at"), 
						gson.fromJson(rs.getString("payload"), type)));
			}
			
			response = Response.ok(gson.toJson(data)).build();
			
		} catch (Exception e) {
			e.printStackTrace();
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
			SDDataSource.closeConnection(connectionString, sd);
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

