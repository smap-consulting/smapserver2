package koboToolboxApi;
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

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.managers.SharedResourceManager;
import org.smap.sdal.managers.UsageManager;

/*
 * Provides access to end points that were previously directly accessed via 
 * surveyKPI.  Hence the path "misc".  Most of these could be moved to other locations
 * in the API
 */
@Path("/v1/misc")
public class Misc extends Application {

	public Misc() {
		
	}

	/*
	 * Get usage for a specific month and user
	 */
	@GET
	@Path("/adminreport/usage/{year}/{month}/{user}")
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@PathParam("year") int year,
			@PathParam("month") int month,
			@PathParam("user") String userIdent,
			@QueryParam("project") boolean byProject,
			@QueryParam("survey") boolean bySurvey,
			@QueryParam("device") boolean byDevice,
			@QueryParam("inc_temp") boolean includeTemporaryUsers,
			@QueryParam("inc_alltime") boolean includeAllTimeUsers,
			@QueryParam("o_id") int oId,
			@QueryParam("tz") String tz,
			@Context HttpServletResponse response) {

		UsageManager um = new UsageManager();
		return um.getUsageForMonth(request, response,
				oId, userIdent, year, month, 
				bySurvey, byProject, byDevice,
				tz);

	}
	
	/*
	 * Return available media files
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
		return srm.getSharedMedia(request, sId, getall);
	}
}

