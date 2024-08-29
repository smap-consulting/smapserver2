package surveyKPI;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.HourlyLogSummaryItem;

import utilities.XLSXLogHourlyReportsManager;

/*
 * Export a log report in xlsx
 */
@Path("/adminreport/logs")
public class AdminReportHourly extends Application {

	Authorise a = null;

	private static Logger log = Logger.getLogger(AdminReportHourly.class.getName());
	LogManager lm = new LogManager();		// Application log
	
	public AdminReportHourly() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get usage for a specific day by hour
	 */
	@GET
	@Path("/hourly/{year}/{month}/{day}")
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@PathParam("year") int year,
			@PathParam("month") int month,
			@PathParam("day") int day,
			@QueryParam("tz") String tz,
			
			@Context HttpServletResponse response) {

		Response responseVal = null;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Logs = Hourly";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			LogManager lm = new LogManager();
			ArrayList<HourlyLogSummaryItem> logs = lm.getSummaryLogEntriesForDay(sd, oId, year, month, day, tz);
			
			String filename = localisation.getString("ar_report_name") + "_" + year + "_" + month + "_" + day;
			
			// Get a sorted list of events
			ArrayList<String> events = new ArrayList<> ();
			HashMap<String, String> eventMap = new HashMap<> ();
			for(HourlyLogSummaryItem item : logs) {
				for(String event : item.events.keySet()) {
					eventMap.put(event, event);
				}
			}
			for(String event : eventMap.keySet()) {
				events.add(event);
			}
			events.sort(null);
			
			String orgName = GeneralUtilityMethods.getOrganisationName(sd, oId);  
			XLSXLogHourlyReportsManager rm = new XLSXLogHourlyReportsManager(localisation);
			responseVal = rm.getNewReport(sd, request, response, events, logs, filename, year, month, day, orgName);
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}


}
