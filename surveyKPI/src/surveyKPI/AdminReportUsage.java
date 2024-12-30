package surveyKPI;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.LogManager;


/*
 * Export a survey in XLSX format
 * This export follows the approach of CSV exports where a single sub form can be selected
 * Get access to a form for each user
 * Replaced by background reports
@Path("/adminreport/usage")
public class AdminReportUsage extends Application {

	Authorise a = null;
	Authorise aOrg = null;

	LogManager lm = new LogManager();		// Application log
	boolean includeTemporaryUsers;
	
	public AdminReportUsage() {
		
	}
	
	/*
	 * Get usage for a specific month and user
	 *
	@GET
	@Path("/{year}/{month}/{user}")
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

}
*/