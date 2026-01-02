package surveyMobileAPI.managers;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.NotFoundException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.legacy.MissingTemplateException;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.AssignmentDetails;
import surveyMobileAPI.XFormData;

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
public class UploadManager {
	
	Authorise a = null;
	
	private static Logger log = Logger.getLogger(UploadManager.class.getName());
	LogManager lm = new LogManager();		// Application log
	
	private static final String OPEN_ROSA_VERSION_HEADER = "X-OpenRosa-Version";
	private static final String OPEN_ROSA_VERSION = "1.0";
	
	private static final String RESPONSE_MSG1 = "<OpenRosaResponse xmlns=\"http://openrosa.org/http/response\">";
	private static final String RESPONSE_MSG2 = "</OpenRosaResponse>";
	
	public UploadManager() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ENUM);
		authorisations.add(Authorise.MANAGE);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Header response to submission request
	 */
	public void setHeaderResponse(@Context HttpServletRequest request,  
			@Context HttpServletResponse resp) {
		String url = request.getScheme() + "://" + request.getServerName() + "/submission";
		log.info("URL:" + url); 
		resp.setHeader("location", url);
		resp.setHeader(OPEN_ROSA_VERSION_HEADER,  OPEN_ROSA_VERSION);
		resp.setStatus(HttpServletResponse.SC_OK);
	}
	
	/*
	 * Process a submission
	 */
	public Response submission(@Context HttpServletRequest request, 
			String instanceId, String key, String deviceId) {
		Response response = null;

		String connectionString = "surveyMobileAPI-Upload";
		Connection sd = SDDataSource.getConnection(connectionString);
		
		/*
		 * Authenticate the user using either the user id associated with the key, if provided, or the
		 *  user id they provided on login
		 */
		String user = null;
		boolean isDynamicUser = false;
		if(key != null) {
			isDynamicUser = true;  // Do not require roles for a dynamic user
			try {
				user = GeneralUtilityMethods.getDynamicUser(sd, key);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else {
			user = request.getRemoteUser();
		}
		
		// If the user is still null try token authentication
		if(user == null) {
			try {
				user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		
		if(user != null) {
			a.isAuthorised(sd, user);
			log.info("User: " + user);
		} 
		
		// End Authorisation

		// Extract the data
		ResourceBundle localisation = null;
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			if(user == null) {
				if(key == null) {
					log.info("Error: Attempting to upload results: user not found");
					throw new AuthorisationException();
				} else {
					// This is for a task where the temporary user has been deleted
					AssignmentDetails aDetails = GeneralUtilityMethods.getAssignmentStatusForTempUser(sd, key);
					String message = null;
					SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
					
					if(aDetails == null || aDetails.status == null) {
						throw new AuthorisationException();
					} else if(aDetails.status.equals("submitted")) {
						message = localisation.getString("wf_fs");
						message = message.replace("%s1", sdf.format(aDetails.completed_date));
					} else if(aDetails.status.equals("cancelled")) {
						message = localisation.getString("wf_fc");
						message = message.replace("%s1", sdf.format(aDetails.cancelled_date));
					} else if(aDetails.status.equals("deleted")) {
						message = localisation.getString("wf_fc");
						message = message.replace("%s1", sdf.format(aDetails.deleted_date));
					}
					throw new ApplicationException(message);
				}
			}
			
			log.info("Upload Started ================= " + instanceId + " ==============");
			log.info("Url:" + request.getRequestURI());
			XFormData xForm = new XFormData();
			xForm.loadMultiPartMime(request, user, instanceId, deviceId, isDynamicUser);
			log.info("Server:" + request.getServerName());
			log.info("Info: Upload finished ---------------- " + instanceId + " ------------");
			
			response = Response.status(Status.CREATED).
					entity(RESPONSE_MSG1 + "<message>" + localisation.getString("c_success") +"</message>" + RESPONSE_MSG2)
					.type("text/xml")
					.header(OPEN_ROSA_VERSION_HEADER, OPEN_ROSA_VERSION)
					.build();
					
		} catch (ApplicationException e) {
			log.info(getErrorMessage(key, e.getMessage()));
			response = Response.status(Status.FORBIDDEN).entity(getErrorMessage(key, e.getMessage())).build();
		} catch (AuthorisationException e) {
			String msg = e.getMessage();
			if(msg == null) {
				msg = "Forbidden";
			}
			log.info(msg);
			response = Response.status(Status.FORBIDDEN).entity(getErrorMessage(key, msg)).build();
		} catch (NotFoundException e) {
			log.info(e.getMessage());
			response = Response.status(Status.NOT_FOUND).entity(getErrorMessage(key, e.getMessage())).build();
		} catch (MissingTemplateException e) {
			log.log(Level.SEVERE, "", e);
			response = Response.status(Status.NOT_FOUND).entity(getErrorMessage(key, e.getMessage())).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "", e);
			String msg = e.getMessage();
			if(msg != null && msg.contains("exceeds its maximum permitted size")) {
				msg = localisation.getString("msg_file_size");
				String fileName = e.getMessage();
				fileName = fileName.substring("The field ".length(), fileName.indexOf(" exceeds"));
				msg = msg.replace("%s1", fileName);
			}
			response = Response.status(Status.BAD_REQUEST).entity(getErrorMessage(key, msg)).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	/*
	 * Format an error message
	 * If the submission was called with a key value set then respond with JSON
	 */
	private String getErrorMessage(String key, String error) {
		String msg = error;
		
		if(key != null) {
			msg = "{" +
					"\"status\": \"error\"," +
					"\"message\": \"" + error + "\"" +
					"}";
		}
		return msg;
	}
}
