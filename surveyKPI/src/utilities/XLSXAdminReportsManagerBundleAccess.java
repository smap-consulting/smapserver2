package utilities;

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

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.GroupDetails;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSXAdminReportsManagerBundleAccess {

	private static Logger log =
			Logger.getLogger(XLSXAdminReportsManagerBundleAccess.class.getName());

	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation = null;

	CellStyle good = null;
	CellStyle bad = null;

	public XLSXAdminReportsManagerBundleAccess(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Create the new style XLSX report
	 */
	public Response getNewReport(
			Connection sd,
			HttpServletRequest request,
			HttpServletResponse response,
			String filename,
			int oId,
			String bundleIdent) {

		Response responseVal = null;

		String escapedFileName = null;
		try {
			escapedFileName = URLDecoder.decode(filename, "UTF-8");
			escapedFileName = URLEncoder.encode(escapedFileName, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}

		escapedFileName = escapedFileName.replace("+", " "); // Spaces ok for file name within quotes
		escapedFileName = escapedFileName.replace("%2C", ","); // Commas ok for file name within quotes

		Workbook wb = null;
		Sheet dataSheet = null;
		CellStyle errorStyle = null;
		int rowNumber = 0;
		int colNumber = 0;
		PreparedStatement pstmt = null;

		try {

			/*
			 * Create XLSX File
			 */
			GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
			wb = new SXSSFWorkbook(10);		// Serialised output
			dataSheet = wb.createSheet("data");

			Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
			CellStyle headerStyle = styles.get("header");
			errorStyle = styles.get("errorStyle");
			good = styles.get("good");
			bad = styles.get("bad");

			/*
			 * Add the headings 
			 */
			colNumber = 0;	
			Row row = dataSheet.createRow(rowNumber++);	

			Cell cell = row.createCell(colNumber++);	// Survey
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("a_name"));

			cell = row.createCell(colNumber++);			// Project
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("ar_project"));
			
			cell = row.createCell(colNumber++);			// Data Survey
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("br_ds"));
			
			cell = row.createCell(colNumber++);			// Oversight Survey
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("br_os"));
			
			cell = row.createCell(colNumber++);			// Read Only Survey
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("br_ro"));
			
			cell = row.createCell(colNumber++);			// Hide on device
			cell.setCellStyle(headerStyle);
			cell.setCellValue(localisation.getString("br_hod"));
			
			/*
			 * Get the surveys in the bundle
			 */
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			ArrayList<GroupDetails> surveys = sm.getSurveysInGroup(sd, bundleIdent);
			HashMap<String, Survey> surveyDetails = new HashMap<>();
			for(GroupDetails gd : surveys) {
				surveyDetails.put(gd.surveyIdent, sm.getById(
						sd, 
						null, 		// cResults
						request.getRemoteUser(), 
						false,
						gd.sId, 
						true,		// full details
						null, 		// basePath
						null, 		// instance id
						false, 		// get results
						false, 		// generate dummy values
						false, 		// get property type 
						true, 		// get soft deleted
						false, 		// get hrk
						"internal",	// get external options 
						false, 		// get changed history
						true, 		// get roles
						true, 		// Pretend to be super user
						null,		// geom format
						false,		// Child surveys
						false,		// launched only
						false		// Don't merge set values into default value
						));
			}
			

			/*
			 * Add the users who have access to the bundle
			 */
			UserManager um = new UserManager(localisation);
			HashMap<String, HashMap<String, String>> userSurveys = new HashMap<> ();
			ArrayList<User> users = um.getUserList(sd, oId, true, true, true, request.getRemoteUser());	
			for(User u : users) {
				
				for(GroupDetails gd : surveys) {
					boolean hasProject = false;
					boolean hasRoleAccess = false;
					boolean hasAuthority = false;
					/*
					 * Does the user have access to the survey project
					 */
					if(u.projects != null) {
						for(Project p : u.projects) {
							if(p.id == gd.pId) {
								hasProject = true;
								break;
							}
						}	
					}
					
					/*
					 * Determine role controlled access
					 */
					Survey survey = surveyDetails.get(gd.surveyIdent);
					if(survey.surveyData.roles.size() == 0) {
						hasRoleAccess = true;		// No roles to worry about - so has role access
					} else {
						for(String roleName : survey.surveyData.roles.keySet()) {	// Role
							for(Role r : u.roles) {
								if(r.name.equals(roleName)) {
									hasRoleAccess = true;
									break;
								}
							}
						}	
					}
					
					/*
					 * Determine user group access
					 */
					for(UserGroup ug : u.groups) {
						if(ug.id == Authorise.ADMIN_ID
								|| ug.id == Authorise.ANALYST_ID
								|| ug.id == Authorise.ENUM_ID) {
							hasAuthority = true;
						}
					}	
					
					/*
					 * Add survey to user survey map
					 */
					if(hasProject && hasRoleAccess && hasAuthority) {
						HashMap<String, String> sMap = userSurveys.get(u.ident);
						if(sMap == null) {
							sMap = new HashMap<> ();
							userSurveys.put(u.ident, sMap);
						}
						sMap.put(gd.surveyIdent, "yes");
					}
				}
				
				HashMap<String, String> sMap = userSurveys.get(u.ident);
				if(sMap != null && sMap.size() > 0) {
					cell = row.createCell(colNumber++);			// Add user name
					cell.setCellStyle(headerStyle);
					cell.setCellValue(u.name);
				}
			}

			/*
			 * Process the surveys
			 */
			HashMap<Integer, String> projects = new HashMap<> ();
			for(GroupDetails gd : surveys) {
				colNumber = 0;

				row = dataSheet.createRow(rowNumber++);	

				cell = row.createCell(colNumber++);	// Survey Name
				cell.setCellValue(gd.surveyName);
				
				cell = row.createCell(colNumber++);	// project
				String projectName = projects.get(gd.sId);
				if(projectName == null) {
					projectName = GeneralUtilityMethods.getProjectNameFromSurvey(sd, gd.sId);
					projects.put(gd.sId, projectName);
				}
				cell.setCellValue(projectName);

				cell = row.createCell(colNumber++);	// Data Survey
				if(gd.dataSurvey ? setCellGood(cell) : setCellBad(cell));
				
				cell = row.createCell(colNumber++);	// Oversight Survey
				if(gd.oversightSurvey ? setCellGood(cell) : setCellBad(cell));
				
				cell = row.createCell(colNumber++);	// Read Only Survey
				if(gd.readOnlySurvey ? setCellGood(cell) : setCellBad(cell));
				
				cell = row.createCell(colNumber++);	// Hide on Device
				if(gd.hideOnDevice ? setCellGood(cell) : setCellBad(cell));
				
				// Add the users
				for(User u : users) {
					HashMap<String, String> sMap = userSurveys.get(u.ident);
					if(sMap != null && sMap.size() > 0) {
						cell = row.createCell(colNumber++);			// Add user name
						String hasSurvey = sMap.get(gd.surveyIdent);
						if(hasSurvey != null ? setCellGood(cell) : setCellBad(cell));
					}
				}
				
			}


		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");

			String msg = e.getMessage();
			if(msg != null && msg.contains("does not exist")) {
				msg = localisation.getString("msg_no_data");
			}
			Row dataRow = dataSheet.createRow(rowNumber + 1);	
			Cell cell = dataRow.createCell(0);
			cell.setCellStyle(errorStyle);
			cell.setCellValue(msg);

			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {	

			try {
				if(pstmt != null ) {try{pstmt.close();} catch (Exception e) {}}
				OutputStream outputStream = response.getOutputStream();
				wb.write(outputStream);
				wb.close();
				outputStream.close();
				((SXSSFWorkbook) wb).dispose();		// Dispose of temporary files
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Error", ex);
			}
		}

		return responseVal;
	}

	private boolean setCellGood(Cell cell) {
		cell.setCellStyle(good);
		cell.setCellValue(localisation.getString("rep_yes"));
		return true;
	}

	private boolean setCellBad(Cell cell) {
		cell.setCellStyle(bad);
		cell.setCellValue(localisation.getString("rep_no"));
		return false;
	}
}
