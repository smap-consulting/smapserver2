package surveyKPI;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;

/*
 * Exports media
 *  Pass in a list of question identifiers in order to generate the 
 *   name of the media file from the identifier
 *    
 */
@Path("/exportProject/{pId}")
public class ExportProject extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(ExportProject.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	boolean forDevice = false;	// Attachment URL prefixes should be in the client format
	
	public ExportProject() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Export a project in a zip file
	 * 1.  Include all survey templates
	 */
	@GET
	public Response exportProject (@Context HttpServletRequest request, 
			@PathParam("pId") int pId,
			@Context HttpServletResponse response
			) {

		ResponseBuilder builder = Response.ok();
		Response responseVal = null;
		ResourceBundle localisation = null;
		String connectionString = "surveyKPI-ExportProject";
		String escapedFileName = null;
		
		log.info("userevent: " + request.getRemoteUser() + " Export project " + pId);
		

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidProject(sd, request.getRemoteUser(), pId);
		// End Authorisation

		if(pId != 0) {
			
			Connection cResults = null;
			PreparedStatement pstmtGetData = null;
			PreparedStatement pstmtIdent = null;
			
			try {
		
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				escapedFileName = GeneralUtilityMethods.getProjectName(sd, pId);
				
				/*
				 * 1. Create the target folder
				 */
				String basePath = GeneralUtilityMethods.getBasePath(request);
				String filePath = basePath + "/temp/" + String.valueOf(UUID.randomUUID());	// Use a random sequence to keep survey name unique
				File folder = new File(filePath);
				folder.mkdir();
				
				/*
				 * 3. Zip the folder up
				 */
				int code = 0;	
				String scriptPath = basePath + "_bin" + File.separator + "getshape.sh";
				Process proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", scriptPath + " " + 
							"none " +
							"none " +
							"none " +
        					filePath + " media"});
        					
				code = proc.waitFor();
					
	            log.info("Process exitValue: " + code);
	        		
	            if(code == 0) {
	            	
	            	int len;
					if ((len = proc.getInputStream().available()) > 0) {
						byte[] buf = new byte[len];
						proc.getInputStream().read(buf);
						log.info("Completed process:\t\"" + new String(buf) + "\"");
					}
					
	            	File file = new File(filePath + ".zip");
	            	if(file.exists()) {
		            	builder = Response.ok(file);
		            	builder.header("Content-Disposition", "attachment;Filename=\"" + escapedFileName + ".zip\"");
		            	responseVal = builder.build();
	            	} else {
	            		throw new ApplicationException(localisation.getString("msg_no_surveys"));
	            	}
		            
				} else {
					int len;
					if ((len = proc.getErrorStream().available()) > 0) {
						byte[] buf = new byte[len];
						proc.getErrorStream().read(buf);
						log.info("Command error:\t\"" + new String(buf) + "\"");
					}
	                responseVal = Response.serverError().entity("Error exporting project").build();
	            }
			
			} catch (ApplicationException e) {
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				// Return an OK status so the message gets added to the web page
				// Prepend the message with "Error: ", this will be removed by the client
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Error", e);
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} finally {
				

				try {if (pstmtGetData != null) {pstmtGetData.close();}} catch (SQLException e) {}
				try {if (pstmtIdent != null) {pstmtIdent.close();}} catch (SQLException e) {}
				
				
				SDDataSource.closeConnection(connectionString, sd);
			}
		}
		
		return responseVal;
		
	}

}
