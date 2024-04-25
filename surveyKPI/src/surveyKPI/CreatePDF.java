package surveyKPI;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.PdfUtilities;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.PDFSurveyManager;
import org.smap.sdal.managers.SurveyManager;



/*
 * Downloads a record in PDF format
 * HTML Fragments 
 *   <h3>  - Use for group Labels
 *   .group - group elements
 *   .hint - Hints
 */

@Path("/pdf/{sIdent}")
public class CreatePDF extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(CreatePDF.class.getName());
	
	boolean forDevice = false;	// Attachment URL prefixes should be in the client format
	
	LogManager lm = new LogManager();		// Application log

	public CreatePDF() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		a = new Authorise(authorisations, null);	
	}
	
	@GET
	//@Produces("application/x-download")
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
		
		log.info("Create PDF from survey:" + sIdent + " for record: " + instanceId);
		Response response = null;
		String connectionString = "createPDF";
		
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
