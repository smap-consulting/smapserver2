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
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.OpsMonitorManager;
import org.smap.sdal.model.OpsItem;
import org.smap.sdal.model.OpsOverview;
import org.smap.sdal.model.OpsSettings;
import org.smap.sdal.model.OpsUnitDetail;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Senior-manager Operations Monitor services.
 * See docs/manager-reporting-solution.md (Part C).
 */
@Path("/ops")
public class OpsMonitor extends Application {

	Authorise a = null;

	private static Logger log = Logger.getLogger(OpsMonitor.class.getName());

	public OpsMonitor() {
		ArrayList<String> authorisations = new ArrayList<String>();
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}

	/*
	 * The L0 overview: KPI tiles, per-unit rollup, open alerts and the backlog trend
	 * for the requesting user's organisation.
	 */
	@GET
	@Path("/overview")
	@Produces("application/json")
	public Response getOverview(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-OpsMonitor-overview";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation

		Connection cResults = null;
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			OpsMonitorManager om = new OpsMonitorManager(localisation);

			String resp = om.getCachedOverview(oId);
			if(resp == null) {
				cResults = ResultsDataSource.getConnection(connectionString);
				OpsOverview ov = om.getOverview(sd, cResults, oId);
				Gson gson = new GsonBuilder().disableHtmlEscaping().create();
				resp = gson.toJson(ov);
				om.putCachedOverview(oId, resp);
			}

			response = Response.ok(resp).build();

		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;
	}

	/*
	 * L2: org-wide at-risk record list. type = overdue | stale | all.
	 */
	@GET
	@Path("/items")
	@Produces("application/json")
	public Response getItems(@Context HttpServletRequest request, @QueryParam("type") String type) {

		Response response = null;
		String connectionString = "surveyKPI-OpsMonitor-items";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		Connection cResults = null;
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			OpsMonitorManager om = new OpsMonitorManager(localisation);
			cResults = ResultsDataSource.getConnection(connectionString);
			ArrayList<OpsItem> items = om.getItems(sd, cResults, oId, type);

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(items)).build();

		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;
	}

	/*
	 * Per-organisation settings (stale interval, RAG thresholds, trend window).
	 */
	@GET
	@Path("/settings")
	@Produces("application/json")
	public Response getSettings(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-OpsMonitor-getSettings";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			OpsMonitorManager om = new OpsMonitorManager(null);
			OpsSettings s = om.getSettings(sd, oId);
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(s)).build();
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	@POST
	@Path("/settings")
	@Produces("application/json")
	public Response saveSettings(@Context HttpServletRequest request, @FormParam("settings") String settings) {

		Response response = null;
		String connectionString = "surveyKPI-OpsMonitor-saveSettings";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			OpsSettings s = gson.fromJson(settings, OpsSettings.class);
			if(s == null) {
				s = new OpsSettings();
			}
			OpsMonitorManager om = new OpsMonitorManager(null);
			om.saveSettings(sd, oId, s, request.getRemoteUser());
			response = Response.ok("{}").build();
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * L1 drill-down: detail for a single unit (role).
	 */
	@GET
	@Path("/unit/{role}")
	@Produces("application/json")
	public Response getUnit(@Context HttpServletRequest request, @PathParam("role") String role) {

		Response response = null;
		String connectionString = "surveyKPI-OpsMonitor-unit";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation

		Connection cResults = null;
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			OpsMonitorManager om = new OpsMonitorManager(localisation);

			String resp = om.getCachedUnit(oId, role);
			if(resp == null) {
				cResults = ResultsDataSource.getConnection(connectionString);
				OpsUnitDetail detail = om.getUnitDetail(sd, cResults, oId, role);
				Gson gson = new GsonBuilder().disableHtmlEscaping().create();
				resp = gson.toJson(detail);
				om.putCachedUnit(oId, role, resp);
			}

			response = Response.ok(resp).build();

		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;
	}
}
