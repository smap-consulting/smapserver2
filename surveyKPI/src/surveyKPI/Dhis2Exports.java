package surveyKPI;

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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.Dhis2ExportConfigManager;
import org.smap.sdal.managers.Dhis2ExportManager;
import org.smap.sdal.model.Dhis2Export;
import org.smap.sdal.model.Dhis2ImportSummary;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Exports of a survey bundle's data into a DHIS2 data set
 *
 * Held per bundle, so the path carries a group survey ident rather than a survey id
 */
@Path("/dhis2/exports")
public class Dhis2Exports extends Application {

	private static Logger log = Logger.getLogger(Dhis2Exports.class.getName());

	Authorise adminAuth;

	public Dhis2Exports() {
		ArrayList<String> auth = new ArrayList<>();
		auth.add(Authorise.ADMIN);
		adminAuth = new Authorise(auth, null);
	}

	// -------------------------------------------------------------------------
	// GET, the exports defined for a bundle
	// -------------------------------------------------------------------------

	@GET
	@Path("/{groupSurveyIdent}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getExports(@Context HttpServletRequest request,
			@PathParam("groupSurveyIdent") String groupSurveyIdent) {

		String conn = "surveyKPI-Dhis2Exports-get";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			ArrayList<Dhis2Export> exports = new Dhis2ExportConfigManager()
					.getExports(sd, oId, groupSurveyIdent);
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			return Response.ok(gson.toJson(exports)).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// PUT, create or update an export and its mapped values
	// -------------------------------------------------------------------------

	@PUT
	@Path("/{groupSurveyIdent}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response saveExport(@Context HttpServletRequest request,
			@PathParam("groupSurveyIdent") String groupSurveyIdent, String body) {

		String conn = "surveyKPI-Dhis2Exports-put";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			Dhis2Export export = new Gson().fromJson(body, Dhis2Export.class);
			export.group_survey_ident = groupSurveyIdent;

			String invalid = validate(export);
			if(invalid != null) {
				return Response.status(Response.Status.BAD_REQUEST).entity(invalid).build();
			}

			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			int id = new Dhis2ExportConfigManager().save(sd, oId, export);
			export.id = id;
			return Response.ok(new Gson().toJson(export)).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// DELETE
	// -------------------------------------------------------------------------

	@DELETE
	@Path("/id/{id}")
	public Response deleteExport(@Context HttpServletRequest request, @PathParam("id") int id) {

		String conn = "surveyKPI-Dhis2Exports-delete";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			new Dhis2ExportConfigManager().delete(sd, oId, id);
			return Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// POST run, send the values for a period range
	// -------------------------------------------------------------------------

	/*
	 * Defaults to a dry run.  DHIS2 validates and reports without storing, which is the only way
	 * to see whether a mapping is right before it writes into a ministry's reporting system
	 */
	@POST
	@Path("/id/{id}/run")
	@Produces(MediaType.APPLICATION_JSON)
	public Response runExport(@Context HttpServletRequest request,
			@PathParam("id") int id,
			@QueryParam("startDate") String startDate,
			@QueryParam("endDate") String endDate,
			@QueryParam("commit") boolean commit) {

		String connSd = "surveyKPI-Dhis2Exports-run";
		String connRel = "surveyKPI-Dhis2Exports-run-results";
		Connection sd = SDDataSource.getConnection(connSd);
		Connection cResults = null;
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			Dhis2ExportConfigManager cm = new Dhis2ExportConfigManager();
			Dhis2Export export = cm.getExport(sd, oId, id);
			if(export == null) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			cResults = ResultsDataSource.getConnection(connRel);

			Dhis2ImportSummary summary = new Dhis2ExportManager()
					.export(sd, cResults, oId, export, startDate, endDate, !commit);

			if(commit) {
				cm.recordExport(sd, oId, id, summarise(summary));
			}

			return Response.ok(new Gson().toJson(summary)).build();

		} catch (Exception e) {
			/*
			 * An export can fail for reasons the user can act on: no connection, a question that
			 * has been renamed out of the bundle, no data in the period.  Say which
			 */
			log.info("DHIS2 export failed for " + id + ": " + e.getMessage());
			try {
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
				if(commit) {
					new Dhis2ExportConfigManager().recordExport(sd, oId, id, e.getMessage());
				}
			} catch (Exception ex) {
				// Recording the failure must not replace reporting it
			}
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();

		} finally {
			SDDataSource.closeConnection(connSd, sd);
			if(cResults != null) {
				ResultsDataSource.closeConnection(connRel, cResults);
			}
		}
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private String summarise(Dhis2ImportSummary s) {
		StringBuilder sb = new StringBuilder();
		sb.append(s.imported).append(" imported, ")
			.append(s.updated).append(" updated, ")
			.append(s.ignored).append(" ignored");
		if(!s.conflicts.isEmpty()) {
			sb.append(", ").append(s.conflicts.size()).append(" conflict")
				.append(s.conflicts.size() == 1 ? "" : "s");
		}
		return sb.toString();
	}

	private String validate(Dhis2Export export) {
		if(export.dataset_uid == null || export.dataset_uid.trim().length() == 0) {
			return "A DHIS2 data set is required";
		}
		if(export.orgunit_question == null || export.orgunit_question.trim().length() == 0) {
			return "A question holding the organisation unit is required";
		}
		if(export.items == null || export.items.isEmpty()) {
			return "At least one value must be mapped";
		}
		for(org.smap.sdal.model.Dhis2ExportItem item : export.items) {
			if(item.data_element == null || item.data_element.trim().length() == 0) {
				return "Every mapped value needs a DHIS2 data element";
			}
			if(item.aggregation == null || item.aggregation.trim().length() == 0) {
				return "Every mapped value needs an aggregation";
			}
		}
		return null;
	}
}
