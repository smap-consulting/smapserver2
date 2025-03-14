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
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.model.ConversationItemDetails;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/message")
public class Messages extends Application {
	
	Authorise a = null;
	Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
	
	private static Logger log =
			 Logger.getLogger(Messages.class.getName());


	public Messages() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		a = new Authorise(authorisations, null);
		
	}

	/*
	 * Create a new case from a message that had been applied to an existing case
	 */
	@POST
	@Path("/newcase")
	@Produces("application/json")
	public Response createNewCaseFromMessage(@Context HttpServletRequest request,			
			@FormParam("idx") int idx,
			@FormParam("sId") int sId,
			@FormParam("groupSurvey") String groupSurvey,
			@FormParam("instanceid") String instanceid,
			@FormParam("comment") String comment
			) { 
		
		Response response = null;
		String connectionString = "SurveyKPI - turn message into new case";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
			
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		try {				
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			

			/*
			 * Remove the conversation item from the source case
			 */
			SMSManager sim = new SMSManager(localisation, "UTC");
			ConversationItemDetails message = sim.removeConversationItemFromRecord(sd, cResults, sId, idx, instanceid, 
					request.getRemoteUser()); 
			
			/*
			 * Create new entry in upload event
			 */	
    		sim.saveMessage(sd, message, request.getServerName(), UUID.randomUUID().toString(), SMSManager.NEW_CASE, comment);

    		response = Response.ok().build();
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Exception", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}
		
		return response;
		
	}
	

}

