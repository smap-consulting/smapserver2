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
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.DSARManager;
import org.smap.sdal.managers.LogManager;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * GDPR B2.3.1 — Data Subject Access Request (DSAR) export.
 *
 * GET /rest/dsar?identifier=<value>
 *               [&field=<question_name>]   restrict search to one PII column
 *               [&partial=true]            substring match instead of exact
 *
 * Searches all PII-flagged columns across every survey the caller can access.
 * Returns a single XLSX workbook — one sheet per matching survey/form pair.
 * Each sheet prepends Project/Survey/Form columns and highlights PII columns
 * in yellow so the admin can verify identity and discard false positives.
 *
 * Requires ANALYST or VIEW_DATA role. The export is logged at organisation
 * level for audit purposes.
 */
@Path("/dsar")
public class DataSubjectAccess extends Application {

	Authorise a = null;

	private static Logger log = Logger.getLogger(DataSubjectAccess.class.getName());

	LogManager lm = new LogManager();

	public DataSubjectAccess() {
		ArrayList<String> authorisations = new ArrayList<>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
	}

	@GET
	public Response export(
			@Context HttpServletRequest request,
			@Context HttpServletResponse response,
			@QueryParam("identifier") String identifier,
			@QueryParam("field") String field,
			@QueryParam("partial") boolean partial) {

		String connectionName = "surveyKPI-DataSubjectAccess";
		Connection sd = SDDataSource.getConnection(connectionName);
		Connection cResults = null;

		Response responseVal;

		try {
			if (identifier == null || identifier.trim().isEmpty()) {
				return Response.status(Status.BAD_REQUEST)
						.entity("{\"error\":\"identifier parameter is required\"}")
						.build();
			}
			identifier = identifier.trim();

			a.isAuthorised(sd, request.getRemoteUser());

			boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			cResults = ResultsDataSource.getConnection(connectionName);

			// Log before streaming (stream may fail partway through)
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			String note = "DSAR export: identifier='" + identifier + "'"
					+ (field   != null ? ", field='" + field + "'" : "")
					+ (partial ? ", partial=true" : "");
			lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), LogManager.DSAR, note, 0);

			String xlsxFilename = "dsar_" + identifier.replaceAll("[^a-zA-Z0-9_\\-]", "_") + ".xlsx";
			GeneralUtilityMethods.setFilenameInResponse(xlsxFilename, response);
			response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

			DSARManager dm = new DSARManager();
			dm.export(sd, cResults, response.getOutputStream(),
					request.getRemoteUser(),
					identifier, field, partial, superUser,
					localisation);

			responseVal = Response.ok("").build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "DSAR export failed", e);
			response.setHeader("Content-type", "text/html; charset=UTF-8");
			responseVal = Response.status(Status.INTERNAL_SERVER_ERROR)
					.entity("Error: " + e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionName, sd);
			ResultsDataSource.closeConnection(connectionName, cResults);
		}

		return responseVal;
	}
}
