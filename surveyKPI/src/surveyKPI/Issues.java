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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.IssuesManager;
import org.smap.sdal.model.OpsIssue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

/*
 * The operational issues report: problems in the user's projects that somebody needs to resolve,
 * each with the page that can resolve it
 */
@Path("/issues")
public class Issues extends Application {

	private static Logger log = Logger.getLogger(Issues.class.getName());

	Authorise a = null;

	public Issues() {
		ArrayList<String> authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}

	@GET
	@Produces("application/json")
	public Response getIssues(@Context HttpServletRequest request, @QueryParam("tz") String tz) {

		String connectionString = "surveyKPI-Issues";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request, request.getRemoteUser());

		Connection cResults = null;
		Response response;
		try {
			cResults = ResultsDataSource.getConnection(connectionString);
			IssuesManager im = new IssuesManager(localisation(sd, request), tz);
			List<OpsIssue> issues = im.getIssues(sd, cResults, request.getRemoteUser());
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(issues)).header("Cache-Control", "no-store").build();	// Always the current state
		} catch (Exception e) {
			log.log(Level.SEVERE, "Issues", e);
			response = Response.serverError().build();
		} finally {
			ResultsDataSource.closeConnection(connectionString, cResults);
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	/*
	 * The same issues as a spreadsheet
	 */
	@GET
	@Path("/xlsx")
	@Produces("application/x-download")
	public Response getIssuesXlsx(@Context HttpServletRequest request, @Context HttpServletResponse response,
			@QueryParam("tz") String tz) {

		String connectionString = "surveyKPI-Issues-xlsx";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request, request.getRemoteUser());

		Connection cResults = null;
		try {
			cResults = ResultsDataSource.getConnection(connectionString);
			ResourceBundle localisation = localisation(sd, request);
			IssuesManager im = new IssuesManager(localisation, tz);
			List<OpsIssue> issues = im.getIssues(sd, cResults, request.getRemoteUser());

			response.setHeader("Cache-Control", "no-store");		// Always the current state, not an earlier download
			GeneralUtilityMethods.setFilenameInResponse(text(localisation, "iss_title", "Operational issues") + ".xlsx", response);

			try (XSSFWorkbook wb = new XSSFWorkbook()) {
				Sheet sheet = wb.createSheet(text(localisation, "iss_sheet", "Issues"));
				CellStyle bold = wb.createCellStyle();
				Font font = wb.createFont();
				font.setBold(true);
				bold.setFont(font);

				String[] headings = {
						text(localisation, "c_type", "Type"),
						text(localisation, "c_project", "Project"),
						text(localisation, "c_survey", "Survey"),
						text(localisation, "iss_item", "Item"),
						text(localisation, "c_details", "Details"),
						text(localisation, "c_count", "Count"),
						text(localisation, "iss_first", "First"),
						text(localisation, "iss_last", "Last")
				};
				Row header = sheet.createRow(0);
				for (int c = 0; c < headings.length; c++) {
					Cell cell = header.createCell(c);
					cell.setCellValue(headings[c]);
					cell.setCellStyle(bold);
				}

				int r = 1;
				for (OpsIssue i : issues) {
					Row row = sheet.createRow(r++);
					// Every value is written as text, so nothing in an error message is read as a formula
					row.createCell(0).setCellValue(nz(i.typeLabel));
					row.createCell(1).setCellValue(nz(i.project));
					row.createCell(2).setCellValue(nz(i.survey));
					row.createCell(3).setCellValue(nz(i.item));
					row.createCell(4).setCellValue(nz(i.detail));
					row.createCell(5).setCellValue(i.count);
					row.createCell(6).setCellValue(nz(i.first));
					row.createCell(7).setCellValue(nz(i.last));
				}
				for (int c = 0; c < headings.length; c++) {
					if (c != 4) sheet.autoSizeColumn(c);
				}
				sheet.setColumnWidth(4, 80 * 256);
				wb.write(response.getOutputStream());
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Issues xlsx", e);
			return Response.serverError().build();
		} finally {
			ResultsDataSource.closeConnection(connectionString, cResults);
			SDDataSource.closeConnection(connectionString, sd);
		}
		return Response.ok("").build();
	}

	private ResourceBundle localisation(Connection sd, HttpServletRequest request) throws Exception {
		Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
		return ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
	}

	private String text(ResourceBundle l, String key, String dflt) {
		try {
			return l.getString(key);
		} catch (Exception e) {
			return dflt;
		}
	}

	private String nz(String s) {
		return s == null ? "" : s;
	}
}
