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

package surveyKPI;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.smap.sdal.Utilities.ApplicationException;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.PDFReportsManager;
import org.smap.sdal.managers.XLSXReportsManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.ReportParameters;
import org.smap.sdal.model.SurveyViewDefn;

/*
 * Allow a temporary user to complete an action
 * There is an action service in both surveyMobileAPI and in surveyKPI.  The reason being only surveyKPI currently
 *  has access to Apache POI for reports
 */

@Path("/action")
public class ActionServiceKPI extends Application {

	Authorise auth = null;

	private static Logger log = Logger.getLogger(ActionServiceKPI.class.getName());

	public ActionServiceKPI() {

		ArrayList<String> authorisations = new ArrayList<String>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.MANAGE); // Enumerators with MANAGE access can process managed forms
		auth = new Authorise(authorisations, null);
	}
	
	SurveyViewDefn mfc = null;

	/*
	 * Get data identified in an action
	 */
	@GET
	@Path("/{ident}")
	public Response getAnonymousReport(
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("ident") String userIdent)
			throws IOException {

		Response responseVal = null;
		StringBuffer outputString = new StringBuffer();
		String requester = "surveyKPI-getAnonymousReport";

		Connection sd = SDDataSource.getConnection(requester);
		Connection cResults = ResultsDataSource.getConnection(requester);

		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// 1. Get details on the action to be performed using the user credentials
			ActionManager am = new ActionManager(localisation, "UTC");		// Time zone should be ignored, real time zone will be retrieved from the action
			Action a = am.getAction(sd, userIdent);

			// 2. If temporary user does not exist then throw exception
			if (a == null) {
				throw new Exception(localisation.getString("mf_adnf"));
			}
			
			// Authorisation - Access Don't validate user rights as this is for an anonymous report
			auth.surveyExists(sd, a.surveyIdent);
			// End Authorisation

			// 3. Get parameters
			ReportParameters p = new ReportParameters();
			p.setParameters(a.parameters);	
			
			// Default to the top level form
			int sId = GeneralUtilityMethods.getSurveyId(sd, a.surveyIdent);
			if(p.fId == 0) {
				Form f = GeneralUtilityMethods.getTopLevelForm(sd, sId);
				p.fId = f.id;
			}
			
			// 4. Get report		
			if(a.reportType == null || a.reportType.equals("xlsx")) {
				GeneralUtilityMethods.setFilenameInResponse(a.name + "." + "xlsx", response); // Set file name
				XLSXReportsManager rm = new XLSXReportsManager(localisation);
				responseVal = rm.getNewReport(
						sd,
						cResults,
						userIdent,
						request.getScheme(),
						request.getServerName(),
						GeneralUtilityMethods.getBasePath(request),
						response.getOutputStream(),
						sId,
						a.surveyIdent,
						p.split_locn,
						p.meta,		// Get altitude and location
						p.merge_select_multiple,
						p.language,
						p.exp_ro,
						p.embedImages,
						p.excludeParents,
						p.hxl,
						p.fId,
						p.startDate,
						p.endDate,
						p.dateId,
						p.filter,
						p.meta,
						p.tz);
			} else if(a.reportType.equals("pdf")) {
				PDFReportsManager prm = new PDFReportsManager(localisation);
				prm.getReport(sd, 
						cResults, 
						userIdent, 
						true,		// Temporary User
						request, 
						response, 
						sId, 
						a.surveyIdent,
						a.filename, 
						p.landscape, 
						p.language, 
						p.startDate, 
						p.endDate, 
						p.dateId, 
						p.filter);
			} else {
				throw new Exception(localisation.getString("Unknown report type: " + a.reportType));
			}

			responseVal = Response.status(Status.OK).entity(outputString.toString()).build();
		} catch (AuthorisationException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw e;
		} catch (ApplicationException e) {
			log.info("Error: " + e.getMessage());
			responseVal = Response.status(Status.OK).entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			responseVal = Response.status(Status.OK).entity(e.getMessage()).build();
		}  finally {
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
		}

		return responseVal;
	}


}
