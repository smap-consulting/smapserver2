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

import java.sql.Connection;
import java.sql.SQLException;
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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.FileManager;
import org.smap.sdal.managers.SharedResourceManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Template;

/*
 * Authorises the user and then
 * Downloads a file
 */

@Path("/file/{filename}")
public class GetFile extends Application {
	
	Authorise a = null;
	Authorise aOrg = new Authorise(null, Authorise.ORG);
	
	private static Logger log =
			 Logger.getLogger(GetFile.class.getName());

	public GetFile() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ENUM);
		a = new Authorise(authorisations, null);	
	}
	
	@GET
	@Path("/organisation")
	@Produces("application/x-download")
	public Response getOrganisationFileUser (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@QueryParam("settings") boolean settings,
			@QueryParam("thumbs") boolean thumbs,
			@QueryParam("org") int requestedOrgId) throws Exception {
		
		log.info("------- " + filename);
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.getOrganisationFile(request, response, request.getRemoteUser(), requestedOrgId, filename, settings, false, thumbs);
	}
	
	@GET
	@Path("/report")
	@Produces("application/x-download")
	public Response getBackgroundReport (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@QueryParam("reportname") String reportname) throws Exception {
		
		log.info("------- " + filename);
		if(reportname == null) {
			reportname = filename;
		}
		return getReportFile(request, response, request.getRemoteUser(), filename, reportname);
	
	}
	
	/*
	 * Get file authenticated with a key
	 */
	@GET
	@Produces("application/x-download")
	@Path("/organisation/key/{key}")
	public Response getOrganisationFilekey(
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@PathParam("key") String key,
			@QueryParam("settings") boolean settings,
			@QueryParam("thumbs") boolean thumbs,
			@QueryParam("org") int requestedOrgId) throws SQLException {
		
		String user = null;	
		String connectionString = "surveyKPI-Get File Key";
		Connection sd = SDDataSource.getConnection(connectionString);
		
		log.info("Getting file authenticated with a key");
		try {
			user = GeneralUtilityMethods.getDynamicUser(sd, key);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		if (user == null) {
			log.info("User not found for key");
			throw new AuthorisationException();
		}
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.getOrganisationFile(request, response, user, requestedOrgId, filename, 
				settings, false, thumbs);
	}
	
	/*
	 * Get file for anonymous user
	 */
	@GET
	@Produces("application/x-download")
	@Path("/organisation/user/{ident}")
	public Response getOrganisationFileAnon(
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@QueryParam("thumbs") boolean thumbs,
			@PathParam("ident") String user) throws SQLException {
				
		log.info("Getting file authenticated with a key");
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.getOrganisationFile(request, response, user, 0, filename, false, true, thumbs);
	}
	
	@GET
	@Path("/users")
	@Produces("application/x-download")
	public Response getUsersFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@QueryParam("type") String type) throws Exception {
		
		int uId = 0;
		Response r = null;
		String connectionString = "Get Users File";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);	
		a.isAuthorised(sd, request.getRemoteUser());		
		try {		
			uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
		} catch(Exception e) {
			// ignore error
		}
		// End Authorisation 
		
		// Only allow valid categories of files
		if(type != null) {
			if(!type.equals("sig")) {
				type = null;
			}
		}
		
		log.info("Get File: " + filename + " for user: " + uId);
		try {
			String basepath = GeneralUtilityMethods.getBasePath(request);
			String filepath = basepath + "/media/users/" + uId + "/" + (type != null ? (type + "/") : "") + filename;
			log.info("Getting user file: " + filepath);
			FileManager fm = new FileManager();
			fm.getFile(response, filepath, filename);
			
			r = Response.ok("").build();
			
		}  catch (Exception e) {
			log.info("Error getting file:" + e.getMessage());
			r = Response.serverError().build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}
	
	/*
	 * Get template pdf file
	 * Legacy - deprecate
	 */
	@GET
	@Path("/surveyPdfTemplate/{sId}")
	@Produces("application/x-download")
	public Response getPdfTemplateFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,			
			@PathParam("filename") String filename,
			@PathParam("sId") int sId,
			@QueryParam("archive") boolean archive,
			@QueryParam("recovery") boolean recovery) throws Exception {
		
		log.info("Get PDF Template File:  for survey: " + sId);
		
		Response r = null;
		String connectionString = "SurveyKPI - Deprecate - Get PDF template file";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidDelSurvey(sd, request.getRemoteUser(), sId, superUser);
		// End Authorisation 
		
		try {
			String basepath = GeneralUtilityMethods.getBasePath(request);
			
			if(!archive) {
				// Ignore the provided filename, get the filename from the survey details
				String displayName = GeneralUtilityMethods.getSurveyName(sd, sId);
				filename = GeneralUtilityMethods.convertDisplayNameToFileName(displayName, false);
				if(recovery) {
					filename += "__prev___template.pdf";
				} else {
					filename += "_template.pdf";
				}
			}
			
			int pId = GeneralUtilityMethods.getProjectId(sd, sId);
			String folderPath = basepath + "/templates/" + pId ;						
			String filepath = folderPath + "/" + filename;
			
			FileManager fm = new FileManager();
			fm.getFile(response, filepath, filename);
			
			r = Response.ok("").build();
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Error getting file", e);
			r = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}
	
	/*
	 * Get a template change history file
	 */
	@GET
	@Path("/change_survey/{sIdent}")
	@Produces("application/x-download")
	public Response getChangeHistoryFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,			
			@PathParam("filename") String name,
			@PathParam("sIdent") String sIdent,
			@QueryParam("name") String downloadName) throws Exception {
		
		log.info("Get change history File:  for survey: " + sIdent);
		
		Response r = null;
		String connectionString = "Get Change History File";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurveyIdent(sd, request.getRemoteUser(), sIdent, false, superUser);
		// End Authorisation 
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);	
			
			String basepath = GeneralUtilityMethods.getBasePath(request);
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			Template t = sm.getTemplate(sd, sIdent, name, basepath);
			
			if(t.filepath == null) {
				// Template may have been deleted and the user is attempting to recover
				t.filepath = basepath + "/templates/survey/" + sIdent + "/" + name;
			}
			FileManager fm = new FileManager();
			if(downloadName == null) {
				downloadName = name;
			}
			fm.getFile(response, t.filepath, downloadName);
			
			r = Response.ok("").build();
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Error getting file", e);
			r = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}
	
	/*
	 * Get new template pdf file
	 */
	@GET
	@Path("/pdfTemplate/{sId}")
	@Produces("application/x-download")
	public Response getNewPdfTemplateFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,			
			@PathParam("filename") String name,
			@PathParam("sId") int sId) throws Exception {
		
		log.info("Get PDF Template File:  for survey: " + sId);
		
		Response r = null;
		String connectionString = "Get Template PDF File";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidDelSurvey(sd, request.getRemoteUser(), sId, superUser);
		// End Authorisation 
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);	
			
			String basepath = GeneralUtilityMethods.getBasePath(request);
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			Template t = sm.getTemplate(sd, sIdent, name, basepath);
			
			if(t.filepath == null) {
				// Template may have been deleted and the user is attempting to recover
				t.filepath = basepath + "/templates/survey/" + sIdent + "/" + name;
			}
			FileManager fm = new FileManager();
			fm.getFile(response, t.filepath, name + ".pdf");
			
			r = Response.ok("").build();
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Error getting file", e);
			r = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}
	
	/*
	 * Get survey level resource file
	 */
	@GET
	@Path("/survey/{sId}")
	@Produces("application/x-download")
	public Response getSurveyFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@PathParam("sId") int sId,
			@QueryParam("thumbs") boolean thumbs,
			@QueryParam("linked") boolean linked) throws Exception {
		
		log.info("Get File: " + filename + " for survey: " + sId);
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.getSurveyFile(request, response,filename, sId, thumbs, linked);	
	}
	
	/*
	 * Get shared history file
	 */
	@GET
	@Path("/history/{id}")
	@Produces("application/x-download")
	public Response getSharedHistoryFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@PathParam("id") int id,
			@QueryParam("sIdent") String sIdent) throws Exception {
		
		Response r = null;
		String connectionString = "SurveyKPI - Get Shared History File";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		if(sIdent != null) {
			a.isValidSurveyIdent(sd, request.getRemoteUser(), sIdent, false, superUser);
		}
		// End Authorisation 
		
		try {
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			FileManager fm = new FileManager();
			r = fm.getSharedHistoryFile(sd,  response, oId, filename, sIdent, id); 
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Error getting file", e);
			r = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}

	/*
	 * Get a report file
	 */
	private Response getReportFile(
			HttpServletRequest request, 
			HttpServletResponse response, 
			String user, 
			String filename, 
			String reportname) {
		
		Response r = null;
		String connectionString = "Get Report File";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);	
		a.isAuthorised(sd, user);		
		// End Authorisation 
		
		
		try {
			
			FileManager fm = new FileManager();
			r = fm.getBackgroundReport(sd, request, response, user, 
					filename, reportname);
			
		}  catch (Exception e) {
			log.info("Error: Failed to get file:" + e.getMessage());
			r = Response.status(Status.NOT_FOUND).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}

}
