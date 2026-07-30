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
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ReferenceFilterManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.ChangeElement;
import org.smap.sdal.model.ReferenceFilter;
import org.smap.sdal.model.SettingChange;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Services for managing reference data filters.  A reference filter restricts the reference
 * data a survey bundle (the linker) pulls from a source survey.  Defined at the group level,
 * so it applies to every survey in the linker bundle - the same as roles.
 */

@Path("/referencefilter")
public class ReferenceFilters extends Application {

	Authorise aUpdate = null;

	private static Logger log =
			 Logger.getLogger(ReferenceFilters.class.getName());

	Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();

	public ReferenceFilters() {
		// Same authorisation as editing survey settings
		ArrayList<String> authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		aUpdate = new Authorise(authorisations, null);
	}

	/*
	 * Get the configured filters and the linkable source surveys for a linker survey
	 */
	@Path("/survey/{sId}")
	@GET
	@Produces("application/json")
	public Response getFilters(
			@Context HttpServletRequest request,
			@PathParam("sId") int sId
			) {

		Response response = null;
		String connectionString = "surveyKPI-getReferenceFilters";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			String linkerSIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);

			ReferenceFilterManager rfm = new ReferenceFilterManager(localisation);

			HashMap<String, Object> result = new HashMap<>();
			result.put("filters", rfm.getFilters(sd, linkerSIdent));
			result.put("sources", rfm.getLinkableSources(sd, linkerSIdent));

			response = Response.ok(gson.toJson(result)).build();
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Save (create or update) a reference filter
	 */
	@Path("/survey/{sId}")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces("application/json")
	public Response saveFilter(
			@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@FormParam("filter") String filterString
			) {

		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
			log.info("Error: Non ajax request");
			throw new AuthorisationException();
		}

		Response response = null;
		String connectionString = "surveyKPI-saveReferenceFilter";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			ReferenceFilter rf = gson.fromJson(filterString, ReferenceFilter.class);
			// The linker is always the survey in the path - do not trust the client
			rf.linkerSIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);

			ReferenceFilterManager rfm = new ReferenceFilterManager(localisation);

			// Get the current settings first so only the fields that actually changed are logged
			ReferenceFilter old = rfm.getFilter(sd, rf.linkerSIdent, rf.linkedSIdent, false);

			rfm.saveFilter(sd, rf);

			ArrayList<SettingChange> changes = new ArrayList<>();
			String oldFilter = (old == null || old.filter == null) ? "" : old.filter;
			String newFilter = rf.filter == null ? "" : rf.filter;
			if(!oldFilter.equals(newFilter)) {
				changes.add(new SettingChange(localisation.getString("rf_filter"), oldFilter, newFilter));
			}
			int oldMax = old == null ? 0 : old.maxRecords;
			if(oldMax != rf.maxRecords) {
				changes.add(new SettingChange(localisation.getString("ed_mrr"),
						String.valueOf(oldMax), String.valueOf(rf.maxRecords)));
			}
			logChange(sd, localisation, sId, request.getRemoteUser(), rf.linkedSIdent, changes);

			response = Response.ok().build();
		} catch (ApplicationException e) {
			response = Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Delete a reference filter for a (linker, source) pair
	 */
	@Path("/survey/{sId}/{linkedIdent}")
	@DELETE
	@Produces("application/json")
	public Response deleteFilter(
			@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("linkedIdent") String linkedIdent
			) {

		Response response = null;
		String connectionString = "surveyKPI-deleteReferenceFilter";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isAuthorised(sd, request, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			String linkerSIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);

			ReferenceFilterManager rfm = new ReferenceFilterManager(localisation);

			// Get the current settings first so the removed values can be logged
			ReferenceFilter old = rfm.getFilter(sd, linkerSIdent, linkedIdent, false);

			rfm.deleteFilter(sd, linkerSIdent, linkedIdent);

			if(old != null) {
				ArrayList<SettingChange> changes = new ArrayList<>();
				if(old.filter != null && old.filter.trim().length() > 0) {
					changes.add(new SettingChange(localisation.getString("rf_filter"), old.filter, ""));
				}
				if(old.maxRecords > 0) {
					changes.add(new SettingChange(localisation.getString("ed_mrr"),
							String.valueOf(old.maxRecords), "0"));
				}
				logChange(sd, localisation, sId, request.getRemoteUser(), linkedIdent, changes);
			}

			response = Response.ok().build();
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Record a reference filter change in the survey change log.
	 * Does nothing if no setting actually changed.
	 */
	private void logChange(Connection sd, ResourceBundle localisation, int sId, String user,
			String linkedIdent, ArrayList<SettingChange> changes) throws Exception {

		if(changes.size() == 0) {
			return;
		}

		ChangeElement change = new ChangeElement();
		change.action = "reference_filter";
		change.origSId = sId;
		change.settingsChanges = changes;

		// Plain text fallback for readers that do not understand the structured data
		StringBuilder msg = new StringBuilder(GeneralUtilityMethods.getSurveyNameFromIdent(sd, linkedIdent));
		for(SettingChange sc : changes) {
			msg.append(", ").append(sc.label).append(": ")
				.append(sc.oldVal).append(" -> ").append(sc.newVal);
		}
		change.msg = msg.toString();

		new SurveyManager(localisation, "UTC").writeChangeLog(sd, sId, user, change);
	}
}
