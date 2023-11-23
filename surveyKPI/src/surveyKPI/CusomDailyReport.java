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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
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
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CustomConfigManager;
import org.smap.sdal.managers.CustomDailyReportsManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.CustomDailyReportsConfig;
import org.smap.sdal.model.CustomReportMultiColumn;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


@Path("/report/daily")
public class CusomDailyReport extends Application {

	Authorise a = new Authorise(null, Authorise.ORG);

	private static Logger log =
			Logger.getLogger(CusomDailyReport.class.getName());

	@GET
	@Path("/{id}/xls")
	@Produces("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	public Response getMonthly (@Context HttpServletRequest request,
			@PathParam("id") int id,
			@QueryParam("year") int year,
			@QueryParam("month") int month,
			@QueryParam("tz") String tz,
			@Context HttpServletResponse response) {

		String connectionString = "tdh - daily report - xls";
		Response responseVal = null;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);	
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidCustomReport(sd, request.getRemoteUser(), id);
		// End Authorisation 
		
		Connection cResults = null;
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			if(month < 1 || month > 12) {
				throw new ApplicationException("Month must be specified and be between 1 and 12");
			}
			if(year == 0) {
				throw new ApplicationException("Year must be specified");
			}
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			cResults = ResultsDataSource.getConnection(connectionString);	
			
			CustomConfigManager cm = new CustomConfigManager(localisation);
			String configString = cm.getConfig(sd, id);
			if(configString != null) {
			
				CustomDailyReportsConfig config = gson.fromJson(configString, CustomDailyReportsConfig.class);
				SurveyManager sm = new SurveyManager(localisation, tz);
				for (CustomReportMultiColumn rmc : config.bars) {
				
					rmc.columns =  getBarColumns(sd, sm, request.getRemoteUser(), rmc.name, config.sIdent);
				}
				
				String filename = GeneralUtilityMethods.getSurveyNameFromIdent(sd, config.sIdent);
				CustomDailyReportsManager drm = new CustomDailyReportsManager(localisation, tz);
				drm.getDailyReport(sd, cResults, response, filename, config, year, month);
				responseVal = Response.status(Status.OK).entity("").build();
			} else {
				responseVal = Response.status(Status.OK).entity("Error: Report not found").build();
			}
			
		}  catch (Exception e) {
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			log.log(Level.SEVERE, "Exception", e);
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);	
			ResultsDataSource.closeConnection(connectionString, cResults);	

		}
		return responseVal;
	}
	
	private ArrayList<String> getBarColumns(Connection sd, SurveyManager sm, String user, String qname, String sIdent) throws SQLException, Exception {
		ArrayList<String> columns = new ArrayList<>();
		
		int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
		Survey s = sm.getById(sd, 
				null, user, false, sId, true, null, null, 
				false, 	// get results
				false, 	// Create dummy values
				false,  // Get property type questions
				false,	// getSoftDeleted
				false,	// getHrk 
				"internal",	// getExternalOptions
				false,  // getChangeHistory
				false,   // getRoles
				false,  // superUser
				null, 	// geomFormat
				false,	// referenceSurveys
				false,	// onlyGetLaunched
				false	// mergeDefaultSetValue
				);
		
		int idx = qname.lastIndexOf('_');
		String base = null;
		if(idx > 0) {
			String suffix = qname.substring(idx + 1);
			try {
				Integer.parseInt(suffix);
				base = qname.substring(0, idx);
			} catch (Exception e) {
				
			}
		}
		if(base != null) {
			for(Form f : s.surveyData.forms) {
				for(Question q : f.questions) {
					if(q.published && (q.type.equals("int") || q.type.equals("decimal"))) {
						if(q.name.startsWith(base)) {
							idx = qname.lastIndexOf('_');
							if(idx > 0) {
								String suffix = qname.substring(idx + 1);
								try {
									Integer.parseInt(suffix);
									columns.add(q.columnName);
								} catch (Exception e) {
									
								}	
							}
						}
					}
				}
			}
		}
		
		return columns;
		
	}

}
