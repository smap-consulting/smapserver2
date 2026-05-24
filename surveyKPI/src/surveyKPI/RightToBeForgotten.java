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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.RTBFManager;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * GDPR B2.3.2 — Right to be Forgotten (RTBF).
 *
 * POST /rest/rtbf?identifier=<value>[&field=<question_name>][&partial=true]
 *
 * Replaces PII column values with "[REDACTED]" in all rows (including edit
 * history / soft-deleted rows) matching the identifier across all accessible
 * surveys. Cascades to child form rows.
 *
 * Returns JSON: {"affected":<n>, "identifier":"<value>"}
 *
 * Requires DPO role. All actions are logged at organisation level for audit.
 */
@Path("/rtbf")
@Produces(MediaType.APPLICATION_JSON)
public class RightToBeForgotten extends Application {

	Authorise a = null;

	private static Logger log = Logger.getLogger(RightToBeForgotten.class.getName());

	LogManager lm = new LogManager();

	public RightToBeForgotten() {
		ArrayList<String> authorisations = new ArrayList<>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
	}

	@POST
	public Response process(
			@Context HttpServletRequest request,
			@QueryParam("identifier") String identifier,
			@QueryParam("field")      String field,
			@QueryParam("partial")    boolean partial) {

		String connectionName = "surveyKPI-RightToBeForgotten";
		Connection sd       = SDDataSource.getConnection(connectionName);
		Connection cResults = null;

		try {
			if (identifier == null || identifier.trim().isEmpty()) {
				return Response.status(Status.BAD_REQUEST)
						.entity("{\"error\":\"identifier parameter is required\"}")
						.build();
			}
			identifier = identifier.trim();

			a.isAuthorised(sd, request.getRemoteUser());

			boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());

			Locale locale = new Locale(
					GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle(
					"org.smap.sdal.resources.SmapResources", locale);

			cResults = ResultsDataSource.getConnection(connectionName);

			RTBFManager rtbf = new RTBFManager();
			int affected = rtbf.process(sd, cResults,
					request.getRemoteUser(),
					identifier, field, partial, superUser,
					localisation);

			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			String note = "RTBF anonymise: identifier='" + identifier + "'"
					+ (field   != null ? ", field='" + field + "'" : "")
					+ (partial ? ", partial=true" : "")
					+ ", affected=" + affected;
			lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), LogManager.ERASE, note, affected);

			String json = "{\"affected\":" + affected
					+ ",\"identifier\":\"" + identifier.replace("\"", "\\\"") + "\"}";
			return Response.ok(json).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "RTBF failed", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR)
					.entity("{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}")
					.build();
		} finally {
			SDDataSource.closeConnection(connectionName, sd);
			ResultsDataSource.closeConnection(connectionName, cResults);
		}
	}
}
