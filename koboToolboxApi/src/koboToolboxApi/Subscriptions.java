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
import model.LogItem;
import model.LogItemDt;
import model.LogsDt;
import model.SubItemDt;
import model.SubsDt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;

/*
 * Provides access to collected data
 */
@Path("/v1/subscriptions")
public class Subscriptions extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(Subscriptions.class.getName());
	
	public Subscriptions() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	
	/*
	 * Get subscription entries
	 */
	@GET
	@Produces("application/json")
	public Response getSubscriptions(@Context HttpServletRequest request,
			@QueryParam("dt") boolean dt
			) { 
		
		String connectionString = "API - get subscriptions";
		Response response = null;
		String user = request.getRemoteUser();
		ArrayList<SubItemDt> data = new ArrayList<> ();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);			
			
			// Get the data
			String sql = "select id, email, unsubscribed, opted_in, opted_in_sent,"
					+ "name "
					+ "from people "
					+ "where o_id = ? "
					+ "order by email asc";
			
			pstmt = sd.prepareStatement(sql);
			int paramCount = 1;
			pstmt.setInt(paramCount++, oId);
			
			log.info("Get data: " + pstmt.toString());
			rs = pstmt.executeQuery();
				
			while (rs.next()) {
					
				SubItemDt item = new SubItemDt();

				item.id = rs.getInt("id");
				item.email = rs.getString("email");
				item.name = rs.getString("name");
				if(item.name == null) {
					item.name = "";
				}
				
				/*
				 * Get status
				 */
				String status = "";
				boolean unsubscribed = rs.getBoolean("unsubscribed");
				boolean optedin = rs.getBoolean("opted_in");
				if(unsubscribed) {
					status = localisation.getString("c_unsubscribed");
				} else if(optedin) {
					status = localisation.getString("c_s2");
				} else {
					String optedInSent = rs.getString("opted_in_sent");
					if(optedInSent != null) {
						status = localisation.getString("c_pending");
					} else {
						status = localisation.getString("c_new");
					}
				}
				item.status = status;
				
				data.add(item);
			}
						
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			if(dt) {
				SubsDt subs = new SubsDt();
				subs.data = data;
				response = Response.ok(gson.toJson(subs)).build();
			} else {
				response = Response.ok(gson.toJson(data)).build();
			}
			
	
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}

}

