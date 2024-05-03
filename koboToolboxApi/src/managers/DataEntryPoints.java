package managers;

import javax.servlet.http.HttpServletRequest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.model.DataEndPoint;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DataEntryPoints {

	Authorise a = null;
	Authorise aSuper = null;
	
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
	
	public Response getData(Connection sd, 
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
			ArrayList<DataEndPoint> data = dm.getDataEndPoints(sd, remoteUser, false, urlprefix);

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
}
