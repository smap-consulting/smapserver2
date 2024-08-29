package surveyMobileAPI.managers;

import java.sql.Connection;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.NotFoundException;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.SurveyData;
import org.smap.server.utilities.GetXForm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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

/*
 * This class supports access to unique information in the database
 * All surveys in a bundle share the same unique key
 */
public class WebFormManager {
	
	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log = Logger.getLogger(WebFormManager.class.getName());
	LogManager lm = new LogManager();		// Application log
	
	private ResourceBundle localisation;
	private String tz;
	
	public WebFormManager(ResourceBundle l, String timezone) {
		localisation = l;
		tz = timezone;
	}
	
	/*
	 * Get instance data as JSON
	 */
	public Response getInstanceData(Connection sd, HttpServletRequest request, String formIdent,
			String updateid, int taskKey, String user, boolean simplifyMedia) {

		Response response = null;

		log.info("webForm:" + formIdent + " updateid:" + updateid + " user: " + user);

		Survey survey = null;
		StringBuffer outputString = new StringBuffer();
		boolean superUser = false;

		// Authorisation
		if (user != null) {
			a.isAuthorised(sd, user);
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			survey = sm.getSurveyId(sd, formIdent); // Get the survey id from the templateName / key
			if (survey == null) {
				throw new NotFoundException();
			}

			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, user);
			} catch (Exception e) {
			}
			a.isValidSurvey(sd, user, survey.surveyData.id, false, superUser); // Validate that the user can access this
																				// survey
			a.isBlocked(sd, survey.surveyData.id, false); // Validate that the survey is not blocked

			if(taskKey > 0) {
				a.isValidTask(sd, request.getRemoteUser(), taskKey);
			}
		} else {
			throw new AuthorisationException();
		}
		// End Authorisation

		// Get the data
		try {

			// Get the XML of the Form
			SurveyTemplate template = new SurveyTemplate(localisation);
			template.readDatabase(survey.surveyData.id, false);

			String instanceXML = null;
			String dataKey = "instanceid";
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);

			GetXForm xForm = new GetXForm(localisation, user, tz);
			instanceXML = xForm.getInstanceXml(survey.surveyData.id, formIdent, template, dataKey, updateid, 0, simplifyMedia, 
					false, taskKey, urlprefix, null, false);

			SurveyData surveyData = new SurveyData();
			surveyData.instanceStrToEdit = instanceXML.replace("\n", "").replace("\r", "");
			surveyData.instanceStrToEditId = updateid;
			surveyData.files = xForm.getFilenames();
			surveyData.paths = xForm.getMediaPaths();

			Gson gsonResp = new GsonBuilder().disableHtmlEscaping().create();
			outputString.append(gsonResp.toJson(surveyData));

			response = Response.status(Status.OK).entity(outputString.toString()).build();

			log.info("userevent: " + user + " : instanceData : " + formIdent + " : updateId : " + updateid);

		} catch (Exception e) {
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			lm.writeLog(sd, survey.surveyData.id, user, LogManager.ERROR, "Failed to get instance data: " + e.getMessage(), 0, request.getServerName());
			log.log(Level.SEVERE, e.getMessage(), e);
		}

		return response;
	}
}
