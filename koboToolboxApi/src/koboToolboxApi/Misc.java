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
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.model.SurveyTemplateManager;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.PdfUtilities;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.PDFSurveyManager;
import org.smap.sdal.managers.SharedResourceManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TimeZoneManager;
import org.smap.sdal.managers.UsageManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.SurveyIdent;
import org.smap.sdal.model.UserSimple;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Provides access to end points that were previously directly accessed via 
 * surveyKPI.  Hence the path "misc".  Most of these could be moved to other locations
 * in the API
 */
@Path("/v1/misc")
public class Misc extends Application {

	private static Logger log =
			 Logger.getLogger(Misc.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	boolean forDevice = true;	// Attachment URL prefixes for API should have the device/API format
	
	public Misc() {
		
	}

	/*
	 * Get usage for a specific month and user
	 */
	@GET
	@Path("/adminreport/usage/{year}/{month}/{user}")
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@PathParam("year") int year,
			@PathParam("month") int month,
			@PathParam("user") String userIdent,
			@QueryParam("project") boolean byProject,
			@QueryParam("survey") boolean bySurvey,
			@QueryParam("device") boolean byDevice,
			@QueryParam("inc_temp") boolean includeTemporaryUsers,
			@QueryParam("inc_alltime") boolean includeAllTimeUsers,
			@QueryParam("o_id") int oId,
			@QueryParam("tz") String tz,
			@Context HttpServletResponse response) {

		UsageManager um = new UsageManager();
		return um.getUsageForMonth(request, response,
				oId, userIdent, year, month, 
				bySurvey, byProject, byDevice,
				tz);

	}
	
	/*
	 * Return available media files
	 */
	@GET
	@Produces("application/json")
	@Path("/media")
	public Response getMedia(
			@Context HttpServletRequest request,
			@QueryParam("survey_id") int sId,
			@QueryParam("getall") boolean getall
			) throws IOException {

		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.getSharedMedia(request, sId, getall, forDevice);
	}
	
	/*
	 * Upload a single shared resource file
	 */
	@POST
	@Produces("application/json")
	@Path("/media")
	public Response uploadSingleSharedResourceFile(
			@Context HttpServletRequest request) {
		
		// No check for Ajax in API
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.uploadSharedMedia(request);
	}
	
	/*
	 * Get a list of users
	 */
	@GET
	@Produces("application/json")
	@Path("/userList/simple")
	public Response getUsersSimple(
			@Context HttpServletRequest request
			) { 

		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.SECURITY);
		authorisations.add(Authorise.ORG);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.MANAGE_TASKS);
		Authorise aSimpleList = new Authorise(authorisations, null);
		
		Response response = null;
		String connectionString = "surveyKPI-getUsersSimple";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aSimpleList.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		ArrayList<UserSimple> users = null;
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);	
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			boolean isOnlyViewData = GeneralUtilityMethods.isOnlyViewData(sd, request.getRemoteUser());
			UserManager um = new UserManager(localisation);
			users = um.getUserListSimple(sd, oId, true, isOnlyViewData, request.getRemoteUser(), false);		// Always sort by name
			String resp = gson.toJson(users);
			response = Response.ok(resp).build();
						
			
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Get a list of surveys with their idents that are accessible to a user
	 */
	@GET
	@Path("/surveys/idents")
	@Produces("application/json")
	public Response getSurveyIdents(@Context HttpServletRequest request) { 
		
		ArrayList<String> authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		Authorise aUpdate = new Authorise(authorisations, null);
		
		String connectionString = "surveyKPI - Get Survey Idents";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString );	
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		
		Response response = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			ArrayList<SurveyIdent> surveyIdents = sm.getSurveyIdentList(sd, request.getRemoteUser(), superUser);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(surveyIdents);
			response = Response.ok(resp).build();			
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {			
			SDDataSource.closeConnection(connectionString , sd);				
		}

		return response;
	}
	
	/*
	 * Upload a survey form
	 * curl -u neil -i -X POST -H "Content-Type: multipart/form-data" -F "projectId=1" -F "templateName=age" -F "tname=@x.xls" http://localhost/surveyKPI/upload/surveytemplate
	 */
	@POST
	@Produces("application/json")
	@Path("/upload/surveytemplate")
	public Response uploadForm(
			@Context HttpServletRequest request) {
		
		// Allow non Ajax calls
		
		SurveyTemplateManager sm = new SurveyTemplateManager();
		return sm.uploadTemplate(request);
	}
	
	/*
	 * Return available Time Zones
	 */
	@GET
	@Produces("application/json")
	@Path("/timezones")
	public Response getTimezones() {

		TimeZoneManager tmz = new TimeZoneManager();
		return tmz.get();
		
	}
	
	/*
	 * Get a PDF of a record of data identified by the instance id
	 */
	@GET
	@Path("/pdf/{sIdent}")
	public Response getPDFService (@Context HttpServletRequest request, 
			@Context HttpServletResponse resp,
			@PathParam("sIdent") String sIdent,
			@QueryParam("instance") String instanceId,
			@QueryParam("language") String language,
			@QueryParam("landscape") boolean landscape,
			@QueryParam("pdftemplate") int pdfTemplateId,
			@QueryParam("filename") String filename,
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("reference_surveys") boolean referenceSurveys,	// Follow links to child surveys,
			@QueryParam("launched_only") boolean onlyGetLaunched			// Only get launched reference surveys
			) throws Exception {
		
		log.info("API call to create PDF from survey:" + sIdent + " for record: " + instanceId);
		Response response = null;
		String connectionString = "createPDF";
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		Authorise a = new Authorise(authorisations, null);	
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);	
		// Get the users locale
		Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
		ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
		a.isAuthorised(sd, request.getRemoteUser());
		
		String errorMsg = null;
		try {
			a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		} catch (Exception e) {
			errorMsg = "Error:" + localisation.getString("mf_snf");			
		}
		
		if(errorMsg == null && pdfTemplateId > 0) {
			try {
				a.isValidPdfTemplate(sd, request.getRemoteUser(), pdfTemplateId);
			} catch (Exception e) {
				errorMsg = "Error:" + localisation.getString("mf_tnf");			
			}
		} else if(pdfTemplateId == 0) {
			pdfTemplateId = -2;	  // Set default to auto
		}
		
		if(errorMsg != null) {
			return Response.serverError().entity(errorMsg).build();		// Don't throw an authorisation exception just report the error
		}
		// End Authorisation 
		
		lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.CREATE_PDF, "Create PDF for instance: " + instanceId, 0, request.getServerName());
		
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		
		// Get the base path
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		try {
			
			// validate timezone
			if(tz == null) {
				tz = "UTC";
			}
			if(!GeneralUtilityMethods.isValidTimezone(sd, tz)) {
				throw new ApplicationException("Invalid Timezone: " + tz);
			}
			
			boolean generateBlank =  (instanceId == null) ? true : false;	// If false only show selected options
			SurveyManager sm = new SurveyManager(localisation, tz);
			org.smap.sdal.model.Survey survey = sm.getById(
					sd, 
					cResults, 
					request.getRemoteUser(), 
					false,
					sId, 
					true, 
					basePath, 
					instanceId, 
					true, 			// get results
					generateBlank, 
					true, 
					false, 
					true, 
					"real", 
					false, 
					false, 
					superUser, 
					"geojson",
					referenceSurveys,
					onlyGetLaunched,
					false);	       // Don't merge set values into default value
			PDFSurveyManager pm = new PDFSurveyManager(localisation, sd, cResults, survey, request.getRemoteUser(), tz);
			
			String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
			// hyperlink prefix assumes that the hyperlink will be used by a human, hence always use client authentication
			String hyperlinkPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, false);
			
			if(pdfTemplateId == -2) { // auto
				pdfTemplateId = GeneralUtilityMethods.testForPdfTemplate(sd, cResults, localisation, survey, request.getRemoteUser(),
						instanceId, tz);
			}
			
			OutputStream os = null;
			String filePath = null;
			
			if(survey.surveyData.compress_pdf) {		// Write the PDF to a temporary file			
				filePath = basePath + "/temp/" + String.valueOf(UUID.randomUUID() + ".pdf");
				File tempFile = new File(filePath);
				os = new FileOutputStream(tempFile);
				log.info("Writing PDF to temporary file: " + filePath);
				
			} else {			// Write directly to the servlet output stream
				os = resp.getOutputStream();
			}
				
			// Create PDF	
			pm.createPdf(
					os,
					basePath, 
					attachmentPrefix,
					hyperlinkPrefix,
					request.getRemoteUser(),
					language, 
					pdfTemplateId,
					generateBlank,
					filename,
					landscape,
					resp);
			
			if(survey.surveyData.compress_pdf) {
				// Remove blank pages from the temporary file and write it to a new temporary file
				String newFilePath = basePath + "/temp/" + String.valueOf(UUID.randomUUID() + ".pdf");
				PdfUtilities.removeBlankPages(filePath, newFilePath);
				
				// Compress the temporary file and write it to the servlet output stream
				os.close();
				os = resp.getOutputStream();
				PdfUtilities.resizePdf(newFilePath, os);
			}
			
			response = Response.ok("").build();
			
		} catch(Exception e) {
			lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.CREATE_PDF, e.getMessage(), 0, request.getServerName());
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);	
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}
		return response;
	}
}

