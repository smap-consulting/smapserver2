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
import javax.ws.rs.Consumes;
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
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.model.LanguageCode;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.RoleName;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import utilities.XLSProjectsManager;
import utilities.XLSRolesManager;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Services for language codes
 */

@Path("/language_codes")
public class LanguageCodes extends Application {
	
	Authorise a = null;

	private static Logger log =
			 Logger.getLogger(LanguageCodes.class.getName());
	
	public LanguageCodes() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.MANAGE_TASKS);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get a list of language codes
	 */
	@GET
	@Produces("application/json")
	public Response getLanguageCodes(
			@Context HttpServletRequest request,
			@QueryParam("translate") boolean translate,
			@QueryParam("transcribe") boolean transcribe		
			) { 

		Response response = null;
		String connectionString = "surveyKPI = get language codes";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());		
		// End Authorisation
		
		boolean hasWhere = false;
		StringBuffer sql = new StringBuffer("select code from language_codes");
		if(translate) {
			sql.append(hasWhere ? " where translate" : " and translate");
		}
		if(transcribe) {
			sql.append(hasWhere ? " where transcribe" : " and transcribe");
		}
		sql.append(" order by code asc");
		
		PreparedStatement pstmt = null;
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			ArrayList<LanguageCode> codes = new ArrayList<LanguageCode> ();
			pstmt = sd.prepareStatement(sql.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				codes.add(new LanguageCode(rs.getString(1), localisation.getString(rs.getString(1))));
			}
			response = Response.ok(gson.toJson(codes)).build();
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {			
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
			SDDataSource.closeConnection("surveyKPI-getRoles", sd);
		}

		return response;
	}
	
	
}

