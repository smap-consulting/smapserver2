/*****************************************************************************

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

 ******************************************************************************/

package surveyMobileAPI;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyTableManager;
import org.smap.sdal.model.KeyValueSimp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/*
 * Lookup reference data that can also be downloaded as a CSV file
 */

@Path("/lookup")
public class Lookup extends Application{

	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log =
			 Logger.getLogger(Lookup.class.getName());
	
	LogManager lm = new LogManager();		// Application log

	/*
	 * Get a record from the reference data identified by the filename and key column
	 */
	@GET
	@Path("/{survey_ident}/{filename}/{key_column}/{key_value}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getInstance(@Context HttpServletRequest request,
			@PathParam("survey_ident") String surveyIdent,		// Survey that needs to lookup some data
			@PathParam("filename") String fileName,				// CSV filename, could be the identifier of another survey
			@PathParam("key_column") String keyColumn,
			@PathParam("key_value") String keyValue
			) throws IOException {

		Response response = null;
		String connectionString = "surveyMobileAPI-Lookup";
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		
		log.info("Lookup: Filename=" + fileName + " key_column=" + keyColumn + " key_value=" + keyValue);

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);		
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		// End Authorisation
		Connection cResults = null;
		PreparedStatement pstmt = null;
		
		// Extract the data
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			
		
			ArrayList<KeyValueSimp> results = new ArrayList<> ();
			if(fileName != null) {
				if(fileName.startsWith("linked_s")) {
					cResults = ResultsDataSource.getConnection(connectionString);	
					
					int sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
					int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser(), sId);
					SurveyTableManager stm = new SurveyTableManager(sd, cResults, localisation, oId, sId, fileName, request.getRemoteUser());
					stm.initData(pstmt, keyColumn, keyValue);
					results = stm.getLine();
				}
			}
			response = Response.ok(gson.toJson(results)).build();
		
		}  catch (Exception e) {
			log.log(Level.SEVERE,"Exception", e);
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}  finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}} 
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}
				
		return response;
	}
	

	
	

    


 
}

