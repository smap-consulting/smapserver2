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
import javax.ws.rs.Consumes;
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

import org.smap.model.SurveyTemplate;
import org.smap.model.TableManager;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.LinkageManager;
import org.smap.sdal.managers.SurveyViewManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.ActionLink;
import org.smap.sdal.model.AutoUpdate;
import org.smap.sdal.model.Filter;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Link;
import org.smap.sdal.model.ManagedFormItem;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.SurveyViewDefn;
import org.smap.sdal.model.TableColumn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Manage Access to reports
 * Should replace ReportListSvcDeprecate
 */
@Path("/reporting")
public class Reports extends Application {
	
	Authorise a = null;
	Authorise aSuper = new Authorise(null, Authorise.ANALYST);
	
	private static Logger log =
			 Logger.getLogger(Review.class.getName());
	
	public Reports() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.MANAGE);		// Enumerators with MANAGE access can process managed forms
		a = new Authorise(authorisations, null);		
	}

	
	/*
	 * Get link to a report
	 */
	@GET
	@Produces("application/json")
	@Path("/link/{name}/{sId}")
	public Response getLink(
			@Context HttpServletRequest request, 
			@PathParam("name") String name,
			@PathParam("sId") int sId,
			@QueryParam("reportType") String type,
			@QueryParam("roles") String roles,
			@QueryParam("filename") String filename,
			@QueryParam("split_locn") boolean split_locn,
			@QueryParam("merge_select_multiple") boolean merge_select_multiple,
			@QueryParam("language") String language,
			@QueryParam("exp_ro") boolean exp_ro,
			@QueryParam("embedimages") boolean embedImages,
			@QueryParam("excludeparents") boolean excludeParents,
			@QueryParam("hxl") boolean hxl,
			@QueryParam("form") int form,
			@QueryParam("from") Date startDate,
			@QueryParam("to") Date endDate,
			@QueryParam("dateId") int dateId,
			@QueryParam("filter") String filter,
			@QueryParam("meta") boolean meta
			) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI - Reports - GetLink");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		try {

			// Get the users locale
			//Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request.getRemoteUser()));
			//ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser(), 0);
			int pId = 0;
		
			ActionManager am = new ActionManager();
			Action action = new Action("report");
			action.sId = sId;
			action.pId = pId;	
			
			action.pId = GeneralUtilityMethods.getProjectId(sd, sId);
			action.reportType = type;
			action.name = name;
			action.filename = (filename == null) ? "report" : filename;
			
			// Parameters
			action.parameters = new ArrayList<KeyValueSimp> ();
			
			if(split_locn) {
				action.parameters.add(new KeyValueSimp("split_locn", "true"));
			}
			if(merge_select_multiple) {
				action.parameters.add(new KeyValueSimp("merge_select_multiple", "true"));
			}
			if(language != null) {
				action.parameters.add(new KeyValueSimp("language", language));
			}
			if(exp_ro) {
				action.parameters.add(new KeyValueSimp("exp_ro", "true"));
			}
			if(embedImages) {
				action.parameters.add(new KeyValueSimp("embed_images", "true"));
			}
			if(excludeParents) {
				action.parameters.add(new KeyValueSimp("excludeParents", "true"));
			}
			if(hxl) {
				action.parameters.add(new KeyValueSimp("hxl", "true"));
			}
			if(form > 0) {
				action.parameters.add(new KeyValueSimp("form", String.valueOf(form)));
			}
			if(startDate != null) {
				action.parameters.add(new KeyValueSimp("startDate", String.valueOf(startDate)));
			}
			if(endDate != null) {
				action.parameters.add(new KeyValueSimp("endDate", String.valueOf(endDate)));
			}
			if(dateId > 0) {
				action.parameters.add(new KeyValueSimp("dateId", String.valueOf(dateId)));
			}
			if(filter != null) {
				action.parameters.add(new KeyValueSimp("filter", filter));
			}
			if(meta) {
				action.parameters.add(new KeyValueSimp("meta", "true"));
			}
			
			if(roles != null) {
				String [] rArray = roles.split(",");
				if(rArray.length > 0) {
					action.roles = new ArrayList<Role> ();
					for (int i = 0; i < rArray.length; i++) {
						Role r = new Role();
						try {
							r.id = Integer.parseInt(rArray[i]);
							action.roles.add(r);
						} catch (Exception e) {
							log.info("Error: Invalid Role Id: " + rArray[i] + " : " + e.getMessage());
						}
					}
				}
			}
			
			log.info("Creating action for report: " + "");	// TODO
			ActionLink al = new ActionLink();
			al.link = request.getScheme() +
					"://" +
					request.getServerName() + 
					am.getLink(sd, action, oId);
			
			Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(al, ActionLink.class);
			response = Response.ok(resp).build();
				
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			
			SDDataSource.closeConnection("surveyKPI - Reports - GetLink", sd);
		}
		
		return response;
	}
	

}

