package surveyKPI;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
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