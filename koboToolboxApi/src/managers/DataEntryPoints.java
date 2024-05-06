package managers;

import javax.servlet.http.HttpServletRequest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.model.DataEndPoint;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DataEntryPoints {

	Authorise a = null;
	Authorise aSuper = null;
	boolean forDevice = true;	// Attachment URL prefixes for API should have the device/API format
	
	private static Logger log =
			Logger.getLogger(DataEntryPoints.class.getName());
	
	public DataEntryPoints() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		a = new Authorise(authorisations, null);

		ArrayList<String> authorisationsSuper = new ArrayList<String> ();	
		authorisationsSuper.add(Authorise.ANALYST);
		authorisationsSuper.add(Authorise.VIEW_DATA);
		authorisationsSuper.add(Authorise.VIEW_OWN_DATA);
		authorisationsSuper.add(Authorise.ADMIN);
		aSuper = new Authorise(authorisationsSuper, null);
	}
	
	public Response getData(String version,
			Connection sd, 
			String connectionString,
			HttpServletRequest request,
			String remoteUser) {
		
		Response response = null;
		
		if(remoteUser == null) {
			return Response.status(Status.UNAUTHORIZED).build();
		}
		aSuper.isAuthorised(sd, remoteUser);
		// End Authorisation
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, remoteUser));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			DataManager dm = new DataManager(localisation, "UTC");
			
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			ArrayList<DataEndPoint> data = dm.getDataEndPoints(sd, remoteUser, false, urlprefix, version);

			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(data);
			response = Response.ok(resp).build();
		} catch (SQLException e) {
			e.printStackTrace();
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	public Response getSingleDataRecord(Connection sd,
			String connectionString,
			HttpServletRequest request,
			String remoteUser,
			String sIdent,
			String uuid,
			String meta,
			String hierarchy,
			String merge,
			String tz) {
		
		Response response;
		Connection cResults = null;
		
		// Authorisation - Access
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, remoteUser);
		} catch (Exception e) {
		}
		
		try {
			
			cResults = ResultsDataSource.getConnection(connectionString);
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, remoteUser));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			if(!GeneralUtilityMethods.isApiEnabled(sd, remoteUser)) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}
			
			DataManager dm = new DataManager(localisation, tz);
			int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);	
			
			a.isAuthorised(sd, remoteUser);
			a.isValidSurvey(sd, remoteUser, sId, false, superUser);
			// End Authorisation
			
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
			
			boolean includeHierarchy = false;
			boolean includeMeta = false;		// Default to false for single record (Historical consistency reason)
			String mergeExp = "no";
			if(meta != null && (meta.equals("true") || meta.equals("yes"))) {
				includeMeta = true;
			}
			if(hierarchy != null && (hierarchy.equals("true") || hierarchy.equals("yes"))) {
				includeHierarchy = true;
			}
			if(merge != null && (merge.equals("true") || merge.equals("yes"))) {
				mergeExp = "yes";
			}
			
			if(includeHierarchy) {
				response = dm.getRecordHierarchy(sd, cResults, request,
						sIdent,
						sId,
						uuid,
						mergeExp, 			// If set to yes then do not put choices from select multiple questions in separate objects
						localisation,
						tz,				// Timezone
						includeMeta,
						urlprefix,
						attachmentPrefix
						);	
			} else {
				response = dm.getSingleRecord(
						sd,
						cResults,
						request,
						sIdent,
						sId,
						uuid,
						mergeExp, 			// If set to yes then do not put choices from select multiple questions in separate objects
						localisation,
						tz,				// Timezone
						includeMeta,
						urlprefix,
						attachmentPrefix
						);	
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			String resp = "{error: " + e.getMessage() + "}";
			response = Response.serverError().entity(resp).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}
		
		return response;
	}
}
