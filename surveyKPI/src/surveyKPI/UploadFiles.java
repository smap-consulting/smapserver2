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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.managers.SurveyTemplateManager;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CsvTableManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SharedResourceManager;
import org.smap.sdal.model.MediaResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/upload")
public class UploadFiles extends Application {

	// Allow analysts and admin to upload resources for the whole organisation
	Authorise auth = null;

	LogManager lm = new LogManager();		// Application log
	
	boolean forDevice = false;

	public UploadFiles() {

		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		auth = new Authorise(authorisations, null);
	}

	private static Logger log =
			Logger.getLogger(UploadFiles.class.getName());
	
	/*
	 * Upload a single shared resource file
	 */
	@POST
	@Produces("application/json")
	@Path("/media")
	public Response uploadSingleSharedResourceFile(
			@Context HttpServletRequest request) {
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.uploadSharedMedia(request);
	}

	/*
	 * Return available files
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
	 * Upload a survey form
	 * curl -u neil -i -X POST -H "Content-Type: multipart/form-data" -F "projectId=1" -F "templateName=age" -F "tname=@x.xls" http://localhost/surveyKPI/upload/surveytemplate
	 */
	@POST
	@Produces("application/json")
	@Path("/surveytemplate")
	public Response uploadForm(
			@Context HttpServletRequest request) {
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		SurveyTemplateManager sm = new SurveyTemplateManager();
		return sm.uploadTemplate(request);
	}
	

}