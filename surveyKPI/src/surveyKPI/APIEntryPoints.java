package surveyKPI;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;

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

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.DataManager;

import java.sql.Date;
import java.util.ArrayList;

/*
 * Allow the GUI to get data from API functions while having a different entry point that can 
 * be authenticated differently from the API users
 */
@Path("/api")
public class APIEntryPoints extends Application {

	public APIEntryPoints() {

	}
	
	/*
	 * Get records for an individual survey in JSON format
	 */
	@GET
	@Produces("application/json")
	@Path("/data/{sIdent}")
	public Response getDataRecordsServiceSmap(@Context HttpServletRequest request,
			@Context HttpServletResponse response,
			@PathParam("sIdent") String sIdent,
			@QueryParam("start") int start,				// Primary key to start from
			@QueryParam("limit") int limit,				// Number of records to return
			@QueryParam("mgmt") boolean mgmt,
			@QueryParam("oversightSurvey") String oversightSurvey,	// Console
			@PathParam("view") int viewId,					// Console
			@QueryParam("schema") boolean schema,			// Console return schema with the data
			@QueryParam("group") boolean group,			// If set include a dummy group value in the response, used by duplicate query
			@QueryParam("sort") String sort,				// Column Human Name to sort on
			@QueryParam("dirn") String dirn,				// Sort direction, asc || desc
			@QueryParam("form") String formName,			// Form name (optional only specify for a child form)
			@QueryParam("start_parkey") int start_parkey,// Parent key to start from
			@QueryParam("parkey") int parkey,			// Parent key (optional, use to get records that correspond to a single parent record)
			@QueryParam("hrk") String hrk,				// Unique key (optional, use to restrict records to a specific hrk)
			@QueryParam("key") String key,				// Unique key (optional, use to restrict records to a specific key - same as hrk)
			@QueryParam("format") String format,			// dt for datatables otherwise assume kobo
			@QueryParam("bad") String include_bad,		// yes | only | none Include records marked as bad
			@QueryParam("completed") String include_completed,		// If yes return unassigned records that have the final status
			@QueryParam("audit") String audit_set,		// if yes return audit data
			@QueryParam("merge_select_multiple") String merge, 	// If set to yes then do not put choices from select multiple questions in separate objects
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("geojson") String geojson,		// if set to yes then format as geoJson
			@QueryParam("geom_question") String geomQuestion,
			@QueryParam("links") String links,
			@QueryParam("meta") String meta,
			@QueryParam("filter") String filter,
			@QueryParam("dd_filter") String dd_filter,		// Drill Down Filter when driling down to a child survey
			@QueryParam("prikey") int prikey,				// Return data for a specific primary key (Distinct from using start with limit 1 as this is for drill down and settings should not be stored)
			@QueryParam("dd_hrk") String dd_hrk,				// Return data matching key when drilling down to parent
			@QueryParam("dateName") String dateName,			// Name of question containing the date to filter by
			@QueryParam("startDate") Date startDate,
			@QueryParam("endDate") Date endDate,
			@QueryParam("instanceid") String instanceId,
			@QueryParam("getSettings") boolean getSettings			// if set true get the settings from the database
			) throws ApplicationException, Exception { 
			
		boolean incLinks = false;
		if(links != null && (links.equals("true") || links.equals("yes"))) {
			incLinks = true;
		}
		if(formName != null) {
			incLinks = false;		// Links can only be specified for the main form
		}
		
		if(key != null) {
			hrk = key;
		}
		
		boolean includeMeta = true;		// Default to true for get all records (Historical consistency reason)
		if(meta != null && (meta.equals("false") || meta.equals("no"))) {
			includeMeta = false;
		}
		
		// Authorisation, localisation and timezone are determined in getDataRecords
		DataManager dm = new DataManager(null, "UTC");	
		dm.getDataRecords(request, response, sIdent, start, limit, mgmt, oversightSurvey, viewId, 
				schema, group, sort, dirn, formName, start_parkey,
				parkey, hrk, format, include_bad, include_completed, audit_set, merge, geojson, geomQuestion,
				tz, incLinks, 
				filter, dd_filter, prikey, dd_hrk, dateName, startDate, endDate, getSettings, 
				instanceId, includeMeta);
		
		return Response.status(Status.OK).build();
	}

}

