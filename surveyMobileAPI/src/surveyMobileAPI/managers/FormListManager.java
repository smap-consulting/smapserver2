package surveyMobileAPI.managers;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXB;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.ServerConfig;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TranslationManager;
import org.smap.sdal.model.ODKForm;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.XformsJavaRosa;
import org.smap.server.utilities.GetXForm;

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
public class FormListManager {
	
	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log = Logger.getLogger(FormListManager.class.getName());
	LogManager lm = new LogManager();		// Application log
	/*
	 * Get the form list
	 */
	public Response getFormList(HttpServletRequest request) throws IOException, ApplicationException {

		
		Response response = null;
		String connectionString = "surveyMobileAPI-FormList";
		Connection sd = SDDataSource.getConnection(connectionString);
		
		String user = request.getRemoteUser();
		if(user == null) {
			user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
		}
		if(user == null) {
			throw new AuthorisationException("Unknown User");
		}
	    a.isAuthorised(sd, user);	//Authorisation - Access 

		String host = ServerConfig.getHost(request); // request.getServerName();
		int portNumber = ServerConfig.getPortNumber(request); //request.getLocalPort();
		log.log(Level.INFO, "Server Conf - portNumber", portNumber);
		String javaRosaVersion = request.getHeader("X-OpenRosa-Version");
		ArrayList<org.smap.sdal.model.Survey> surveys = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			surveys = sm.getSurveys(sd, 
					user, 
					false, 
					false, 
					0, 
					superUser, 
					false, 
					false, 		// get group details
					true,		// only data survey
					false,		// links
					null);		// url prefix
			
			// Determine whether or not a manifest identifying media files exists for this survey
			TranslationManager translationMgr = new TranslationManager();
			for (int i = 0; i < surveys.size(); i++ ) {
				Survey s = surveys.get(i);
				s.setHasManifest(translationMgr.hasManifest(sd, user, s.getId())); 
			}
			
			XformsJavaRosa formList = processXForm(host, portNumber, surveys);	
			
			// Convert response into xml
			String resp = null;
			StringWriter writer = new StringWriter();
			JAXB.marshal(formList, writer);
			resp = writer.toString();
			
			response = Response.ok(resp).header("X-OpenRosa-Version", javaRosaVersion).build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
			response = Response.serverError().entity(e.getMessage()).build();
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
		
			SDDataSource.closeConnection(connectionString, sd);
			
		}
		
		return response;

	}
	
	/*
	 * Return a form in XML format
	 */
	public String getForm(HttpServletRequest request, String templateName, String deviceId) {
		log.info("formXML:" + templateName);

		// Authorisation - Access

		Survey survey = null;
		
		boolean superUser = false;
		ResourceBundle localisation = null;
		String response = null;	

		String user = null;
		String connectionString = "surveyMobileAPI-FormXML";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {

			user = request.getRemoteUser();
			if(user == null) {
				user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
			}
			if(user == null) {
				throw new AuthorisationException("Unknown User");
			}
			
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, user);
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			} catch (Exception e) {
				log.info("Error getting localisation");
			}

			String tz = "UTC";

			// Authorisation
			a.isAuthorised(sd, user);
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			survey = sm.getSurveyId(sd, templateName);	// Get the survey id from the templateName / key
			a.isValidSurvey(sd, user, survey.surveyData.id, false, superUser);	// Validate that the user can access this survey

			// Extract the data
			SurveyTemplate template = new SurveyTemplate(localisation);
			template.readDatabase(survey.surveyData.id, false);
			GetXForm xForm = new GetXForm(localisation, user, tz);
			response = xForm.get(template, false, true, false, user, request);
			log.info("userevent: " + user + " : download survey : " + templateName);		

			// Record that this form was downloaded by this user
			GeneralUtilityMethods.recordFormDownload(sd, user, survey.surveyData.ident, survey.surveyData.version, deviceId);
		} catch (AuthorisationException ae) { 
			throw ae;
		} catch (ApplicationException e) {
			response = e.getMessage();
			String msg = localisation.getString("msg_err_template");
			msg= msg.replace("%s1", response);
			lm.writeLog(sd, survey.surveyData.id, user, LogManager.ERROR, msg, 0, null);
		} catch (Exception e) {
			response = e.getMessage();
			log.log(Level.SEVERE, response, e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		

		return response;
	}
		
	private XformsJavaRosa processXForm(String host, int portNumber, 
			ArrayList<org.smap.sdal.model.Survey> surveys) {

		String port = "";
		String responsePath = "";
		String protocol = "";

		//XFormListProxy formList = new XFormListProxy();
		XformsJavaRosa formList = new XformsJavaRosa();
		formList.xform = new ArrayList<ODKForm> ();
		
		if(portNumber != 80  && portNumber != 443) {
			port = ":" + String.valueOf(portNumber);
		}
		
		if(portNumber == 80) {
			protocol = "http://";
		} else {
			protocol = "https://";
		}

		// Extract the data
		try {
			
			for (int i = 0; i < surveys.size(); i++ ) {
				//XFormProxy form = new XFormProxy();
				ODKForm form = new ODKForm();
				Survey s = surveys.get(i);
				form.formID = String.valueOf(s.getIdent());
				form.name = s.getDisplayName();
				form.version = String.valueOf(s.surveyData.version);
				form.hash = "md5:version:" + form.version + ":" + form.formID;		// If the version changes the form should be re-downloaded
				form.downloadUrl = protocol + host + port + responsePath + "/formXML?key=" + form.formID;
				if(s.hasManifest()) {
					form.manifestUrl = protocol + host + port + responsePath +
							"/xformsManifest?key=" + s.getIdent();
				}
				formList.xform.add(form);
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "", e);
			return null;
		} 		

		return formList;
	}
}
