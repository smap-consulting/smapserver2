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

import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Logger;

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
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.DataEndPoint;
import managers.DataEntryPoints;

/*
 * Returns data for the passed in table name
 */
@Path("/v2/data.csv")
public class Data_CSV2 extends Application {

	Authorise a = null;
	
	private static String VERSION = "v2";

	boolean forDevice = true;	// Attachment URL prefixes for API should have the device/API format
	
	private static Logger log = Logger.getLogger(Data_CSV2.class.getName());

	LogManager lm = new LogManager(); // Application log
	
	// Tell class loader about the root classes. (needed as tomcat6 does not support
	// servlet 3)
	public Set<Class<?>> getClasses() {
		Set<Class<?>> s = new HashSet<Class<?>>();
		s.add(Data_CSV2.class);
		return s;
	}

	public Data_CSV2() {
		ArrayList<String> authorisations = new ArrayList<String>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}

	/*
	 * API version 2 /data.csv CSV
	 */
	@GET
	@Produces("text/csv")
	public Response getDataCsv(@Context HttpServletRequest request, @QueryParam("filename") String filename,
			@Context HttpServletResponse response) {

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("koboToolBoxApi-getDataCSV-2");
		String remoteUser = GeneralUtilityMethods.getApiKeyUser(sd, request);
		if(remoteUser == null) {
			return Response.status(Status.UNAUTHORIZED).build();
		}
		a.isAuthorised(sd, remoteUser);

		PrintWriter outWriter = null;

		if (filename == null) {
			filename = "forms.csv";
		}

		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, remoteUser));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
						
			DataManager dm = new DataManager(localisation, "UTC");
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			ArrayList<DataEndPoint> data = dm.getDataEndPoints(sd, remoteUser, true, urlprefix, VERSION);

			response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");

			outWriter = response.getWriter();

			if (data.size() > 0) {
				for (int i = 0; i < data.size(); i++) {
					DataEndPoint dep = data.get(i);
					if (i == 0) {
						outWriter.print(dep.getCSVColumns() + "\n");
					}
					outWriter.print(dep.getCSVData() + "\n");
					if(dep.subforms != null) {
						for(String formName : dep.subforms.keySet()) {
							outWriter.print(dep.getSubForm(formName, dep.subforms.get(formName)) + "\n");
						}
					}

				}
			} else {
				outWriter.print("No Data");
			}

			outWriter.flush();
			outWriter.close();

		} catch (Exception e) {
			e.printStackTrace();
			try {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			} catch (Exception ex) {
			}
			;
		} finally {
			SDDataSource.closeConnection("koboToolBoxApi-getDataCSV", sd);
		}
		
		return Response.ok("").build();

	}

	/*
	 * API version 2 /data
	 */
	@GET
	@Produces("text/csv")
	@Path("/{sIdent}")
	public Response getDataRecords(@Context HttpServletRequest request, @Context HttpServletResponse response,
			@PathParam("sIdent") String sIdent, 
			@QueryParam("start") int start, // Primary key to start from
			@QueryParam("limit") int limit, // Number of records to return
			@QueryParam("mgmt") boolean mgmt, 
			@QueryParam("group") boolean group, // If set include a dummy group value in the response, used by duplicate query
			@QueryParam("sort") String sort, // Column Human Name to sort on
			@QueryParam("dirn") String dirn, // Sort direction, asc || desc
			@QueryParam("form") String formName, // Form id (optional only specify for a child form)
			@QueryParam("start_parkey") int start_parkey, // Parent key to start from
			@QueryParam("parkey") int parkey, // Parent key (optional, use to get records that correspond to a single parent record)
			@QueryParam("hrk") String hrk, // Unique key (optional, use to restrict records to a specific hrk)
			@QueryParam("format") String format, // dt for datatables otherwise assume kobo
			@QueryParam("bad") String include_bad, // yes | only | none Include records marked as bad
			@QueryParam("filename") String filename, 
			@QueryParam("audit") String audit_set,
			@QueryParam("tz") String tz,			// Time Zone
			@QueryParam("filter") String filter,
			@QueryParam("merge_select_multiple") String merge 	// If set to yes then do not put choices from select multiple questions in separate columns
			) throws ApplicationException, Exception {

		String connectionString = "Api - get data records 2 csv";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		String remoteUser = GeneralUtilityMethods.getApiKeyUser(sd, request);
		
		DataEntryPoints dep = new DataEntryPoints();
		return dep.getCSVData(VERSION, sd, connectionString, request, response, remoteUser, 
				sIdent,
				formName,
				sort,
				dirn,
				audit_set,
				merge,
				include_bad,
				filename,
				tz,
				mgmt,
				hrk,
				group,
				start,
				parkey,
				start_parkey,
				filter,
				limit);
		
	}
	
}

