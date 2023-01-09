package surveyKPI;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletRequest;

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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LinkageManager;
import org.smap.sdal.model.LinkageItem;
import org.smap.sdal.model.Match;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.machinezoo.sourceafis.FingerprintImage;
import com.machinezoo.sourceafis.FingerprintImageOptions;
import com.machinezoo.sourceafis.FingerprintTemplate;

import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/match")
public class MatchService extends Application {

	private static Logger log =
			 Logger.getLogger(MatchService.class.getName());
	
	Authorise a = null;
	
	public MatchService() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.LINKS);
		a = new Authorise(authorisations, null);	
	}
	
	/*
	 * Get the fingerprints that match the passed in fingerprint
	 */
	@GET
	@Path("/fingerprint/image")
	@Produces("application/json")
	public Response matchFingerprintImage(@Context HttpServletRequest request,
			@QueryParam("image") String image,			// URL of an image
			@QueryParam("threshold") double threshold) { 	

		Response response = null;
		String connectionString = "SurveyKPI - match a fingerprint";
		

		// Authorisation
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());		
		
		if(threshold == 0.0) {
			threshold = 30.0;
		}
			
		log.info("Get fingerprint for image: " + image + " Threshold: " + threshold);
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			String basePath = GeneralUtilityMethods.getBasePath(request);
			
			String extension = image.substring(image.lastIndexOf('.') + 1);
			URL url = new URL(image);
			BufferedImage tempImg = ImageIO.read(url);
			File file = new File(basePath + "/temp/fp_" + UUID.randomUUID() + "." + extension);
			ImageIO.write(tempImg, extension, file);
			URI uri = file.toURI();
			
			FingerprintTemplate probe = new FingerprintTemplate(
				    new FingerprintImage(
				        Files.readAllBytes(Paths.get(uri)),
				        new FingerprintImageOptions()
				            .dpi(500)));
			
			LinkageManager linkMgr = new LinkageManager(localisation);
			ArrayList<Match> matches = linkMgr.matchSingleTemplate(sd, oId, probe, threshold);
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(matches);
			response = Response.ok(resp).build();
			
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
	
	/*
	 * Get the linkage items in a record
	 */
	@GET
	@Path("/record/{survey_ident}/{instanceid}")
	@Produces("application/json")
	public Response linkageItemsInRecord(@Context HttpServletRequest request,
			@PathParam("survey_ident") String sIdent,
			@PathParam("instanceid") String instanceId) {

		Response response = null;
		String connectionString = "SurveyKPI - linkage itemst";
		

		// Authorisation
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());		
	
		log.info("Get linkage items for survey: " + sIdent + " InstanceId: " + instanceId);
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			String basePath = GeneralUtilityMethods.getBasePath(request);
			
			LinkageManager linkMgr = new LinkageManager(localisation);
			ArrayList<LinkageItem> items = linkMgr.getRecordLinkages(sd, sIdent, instanceId);
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(items)).build();
			
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
}

