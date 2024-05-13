package org.smap.sdal.managers;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.AR;

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

public class UsageManager {
	
	private static Logger log =
			 Logger.getLogger(UsageManager.class.getName());
	

	public UsageManager() {
		
	}

	/*
	 * Get usage for a month
	 */
	public Response getUsageForMonth(HttpServletRequest request, 
			HttpServletResponse response,
			int oId,
			String userIdent,
			int year,
			int month,
			boolean bySurvey,
			boolean byProject,
			boolean byDevice,
			String tz) {
		
		Response responseVal = null;
		
		Authorise a = new Authorise(null, Authorise.ADMIN);
		Authorise aOrg = new Authorise(null, Authorise.ORG);
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Usage";
		Connection sd = SDDataSource.getConnection(connectionString);		
		// End Authorisation		
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {
		
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// start validation			
			if(oId > 0) {
				aOrg.isAuthorised(sd, request.getRemoteUser());
			} else {
				a.isAuthorised(sd, request.getRemoteUser());
			}
			String orgName = "";
			if(oId <= 0) {
				oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			} else {
				orgName = GeneralUtilityMethods.getOrganisationName(sd, oId);
			}
			int uId = GeneralUtilityMethods.getUserId(sd, userIdent);
			if(uId <= 0) {
				String msg = localisation.getString("unf");
				msg = msg.replace("%s1", userIdent);
				throw new Exception(msg);
			}
			a.isValidUser(sd, request.getRemoteUser(), uId);
						
			if(month < 1) {
				throw new ApplicationException(localisation.getString("ar_month_gt_0"));
			}
			// End Validation

			
			String filename = localisation.getString("ar_report_name") + (oId > 0 ? "_" + orgName : "") + year + "_" + month + "_" + userIdent + ".xlsx";
			
			ArrayList<AR> report = null;
			XLSXAdminReportsManager rm = new XLSXAdminReportsManager(localisation, tz);
			if(bySurvey) {
				report = rm.getAdminReportSurvey(sd, oId, month, year, userIdent);
			} else if(byProject) {
				report = rm.getAdminReportProject(sd, oId, month, year, userIdent);
			} else if(byDevice) {
				report = rm.getAdminReportDevice(sd, oId, month, year, userIdent);
			} else {
				report = rm.getAdminReport(sd, oId, month, year, userIdent);
			}
			
			ArrayList<String> header = new ArrayList<String> ();
			header.add(localisation.getString("ar_ident"));
			header.add(localisation.getString("ar_user_name"));
			header.add(localisation.getString("ar_user_created"));
			if(byProject || bySurvey) {
				header.add(localisation.getString("ar_project_id"));
				header.add(localisation.getString("ar_project"));
			}
			if(bySurvey) {
				header.add(localisation.getString("ar_survey_id"));
				header.add(localisation.getString("ar_survey"));
			}
			if(byDevice) {
				header.add(localisation.getString("a_device"));
			}
			header.add(localisation.getString("ar_usage_month"));
			header.add(localisation.getString("ar_usage_at"));
			
			// Get temp file
			String basePath = GeneralUtilityMethods.getBasePath(request);
			String filepath = basePath + "/temp/" + UUID.randomUUID();	// Use a random sequence to keep survey name unique
			File tempFile = new File(filepath);
			
			rm.getNewReport(sd, tempFile, header, report, byProject, bySurvey, byDevice, year, month,
					GeneralUtilityMethods.getOrganisationName(sd, oId));
			
			response.setHeader("Content-type",  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=UTF-8");
			
			FileManager fm = new FileManager();
			fm.getFile(response, filepath, filename);
			
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


