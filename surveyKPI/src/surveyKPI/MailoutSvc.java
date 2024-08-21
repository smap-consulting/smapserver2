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
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.DocumentUploadManager;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.Organisation;
import utilities.XLSMailoutManager;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Manage subscribers
 * This is closely related to the subscribers service
 */
@Path("/mailout")
public class MailoutSvc extends Application {

	private static Logger log =
			 Logger.getLogger(MailoutSvc.class.getName());
	
	Authorise a = null;	
	
	public MailoutSvc() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);		
	}
	
	/*
	 * Delete a mailout campaign
	 */
	@DELETE
	public Response deleteMailout(@Context HttpServletRequest request,
			@FormParam("mailoutId") int mailoutId) { 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey - delete mailout";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(mailoutId > 0) {
			a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		}
		// End Authorisation
		
		try {	
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			MailoutManager mm = new MailoutManager(localisation);	
			mm.deleteMailout(sd, mailoutId, oId);
			
			response = Response.ok("").build();
			
		} catch (Exception e) {
			String msg = e.getMessage();
			log.info(msg);
			if(msg == null) {
				msg = "System Error";
			}
		    response = Response.serverError().entity(msg).build();
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	/*
	 * Export mailout emails
	 */
	@GET
	@Path ("/xls/{mailoutId}")
	@Produces("application/x-download")
	public Response getXLSTasksService (@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("mailoutId") int mailoutId,
			@QueryParam("tz") String tz,
			@QueryParam("filetype") String filetype,
			@QueryParam("inc_status") String incStatus) throws Exception {

		String connectionString = "Download Mailout Emails";
		Connection sd = SDDataSource.getConnection(connectionString);	
		// Authorisation - Access

		a.isAuthorised(sd, request.getRemoteUser());		
		if(mailoutId > 0) {
			a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		} else {
			throw new AuthorisationException("no mailout id");
		}
		// End Authorisation 
		
		// Set file type to "xlsx" unless "xls" has been specified
		if(filetype == null || !filetype.equals("xls")) {
			filetype = "xlsx";
		}
		
		if(tz == null) {
			tz = "UTC";
		}
		
		log.info("Exporting tasks with timzone: " + tz);
		
		try {
			
			// Localisation
			Organisation organisation = UtilityMethodsEmail.getOrganisationDefaults(sd, null, request.getRemoteUser());
			Locale locale = new Locale(organisation.locale);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			MailoutManager mm = new MailoutManager(localisation);
			
			String filename = null;
			Mailout mo = mm.getMailoutDetails(sd, mailoutId);		// Get the mailout name
			filename = mo.name + "." + filetype;
			
			GeneralUtilityMethods.setFilenameInResponse(filename, response); // Set file name
			
			ArrayList<MailoutPerson> mop = mm.getMailoutPeople(
					sd, 
					mailoutId,
					organisation.id, 
					false);			// Spreadsheet can handle html
			
			// Create Mailout XLS File
			XLSMailoutManager xmo = new XLSMailoutManager(filetype, request.getScheme(), request.getServerName());
			xmo.createXLSFile(response.getOutputStream(), mop, localisation, tz);
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			throw new Exception("Exception: " + e.getMessage());
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);	
			
		}
		return Response.ok("").build();
	}
	
	/*
	 * Import mailout emails from an xls file
	 */
	@POST
	@Produces("application/json")
	@Path("/xls/{mailoutId}")
	public Response uploadEmails(
			@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId
			) throws IOException {
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		boolean clear = false;
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();		

		log.info("userevent: " + request.getRemoteUser() + " : upload mailout emails from xls file for mailout id: " + mailoutId);

		fileItemFactory.setSizeThreshold(20*1024*1024); 	// 20 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
	
		Connection sd = null; 
		String fileName = null;
		String filetype = null;
		FileItem file = null;

		String requester = "Mailouts - Mailout Emails Upload";
		
		try {
			
			sd = SDDataSource.getConnection(requester);
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";	// Set default for timezone
			
			/*
			 * Parse the request
			 */
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();

			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();
				
				// Get form parameters
				
				if(item.isFormField()) {
					log.info("Form field:" + item.getFieldName() + " - " + item.getString());
					if(item.getFieldName().equals("mp_clear")) {
						clear = Boolean.valueOf(item.getString());
					}
					
				} else if(!item.isFormField()) {
					// Handle Uploaded files.
					log.info("Field Name = "+item.getFieldName()+
						", File Name = "+item.getName()+
						", Content type = "+item.getContentType()+
						", File Size = "+item.getSize());
					
					
					fileName = item.getName();
					
					/*
					 * Validate the upload
					 */
					DocumentUploadManager dum = new DocumentUploadManager(localisation);
					dum.validateDocument(fileName, item, DocumentUploadManager.SETTINGS_IMPORT_TYPES);

					if(fileName.endsWith("xlsx") || fileName.endsWith("xlsm")) {
						filetype = "xlsx";
					} else if(fileName.endsWith("xls")) {
						filetype = "xls";
					} else {
						log.info("unknown file type for item: " + fileName);
						continue;	
					}
					
					file = item;
				}
			}
	
			if(file != null && mailoutId > 0) {
				// Authorisation - Access
				a.isAuthorised(sd, request.getRemoteUser());
				if(mailoutId > 0) {
					a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
				} else {
					throw new AuthorisationException("no mailout id");
				}
				// End authorisation

				// Process xls file
				XLSMailoutManager xmm = new XLSMailoutManager();
				ArrayList<MailoutPerson> mop = xmm.getXLSMailoutList(filetype, file.getInputStream(), localisation, tz);	
						
				// Save mailout emails to the database
				MailoutManager mm = new MailoutManager(localisation);
				
				if(clear) {
					mm.deleteUnsentEmails(sd, mailoutId);
				}
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				
				mm.writeEmails(sd, oId, mop, mailoutId, "new");
				
					
			}
			
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
	
			SDDataSource.closeConnection(requester, sd);
			
		}
		
		return response;
		
	}
	
	/*
	 * Send any unsent emails
	 */
	@GET
	@Path("/send/{mailoutId}")
	public Response sendMailouts(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId,
			@QueryParam("retry") boolean retry
			) {

		Response response = null;
		String connectionString = "surveyKPI-Send Emails";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		// End Authorisation
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
						
			MailoutManager mm = new MailoutManager(localisation);
			mm.sendEmails(sd, mailoutId, retry); 
				
			response = Response.ok("").build();
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
		    response = Response.serverError().build();
		    
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Generate links for any emails without links
	 */
	@GET
	@Path("/gen/{mailoutId}")
	public Response genMailoutLInks(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId
			) { 

		Response response = null;
		String connectionString = "surveyKPI-Generatate Email Links";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		// End Authorisation
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
						
			MailoutManager mm = new MailoutManager(localisation);
			mm.genEmailLinks(sd, mailoutId, request.getServerName());
				
			response = Response.ok("").build();
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
		    response = Response.serverError().build();
		    
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

}

