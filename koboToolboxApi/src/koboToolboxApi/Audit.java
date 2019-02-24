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

import managers.AuditManager;
import managers.DataManager;
import model.DataEndPoint;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CustomReportsManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.TableDataManager;
import org.smap.sdal.model.ReportConfig;
import org.smap.sdal.model.TableColumn;

/*
 * Provides access to audit views on the surveys
 */
@Path("/v1/audit")
public class Audit extends Application {

	Authorise a = null;
	Authorise aSuper = null;

	private static Logger log =
			Logger.getLogger(Audit.class.getName());

	LogManager lm = new LogManager();		// Application log

	// Tell class loader about the root classes.  (needed as tomcat6 does not support servlet 3)
	public Set<Class<?>> getClasses() {
		Set<Class<?>> s = new HashSet<Class<?>>();
		s.add(Audit.class);
		return s;
	}

	public Audit() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		a = new Authorise(authorisations, null);

		ArrayList<String> authorisationsSuper = new ArrayList<String> ();	
		authorisationsSuper.add(Authorise.ANALYST);
		authorisationsSuper.add(Authorise.VIEW_DATA);
		authorisationsSuper.add(Authorise.ADMIN);
		aSuper = new Authorise(authorisationsSuper, null);

	}

	/*
	 * KoboToolBox API version 1 /data
	 * Returns a list of data end points
	 */
	@GET
	@Produces("application/json")
	public Response getAudit(@Context HttpServletRequest request) { 

		Response response = null;
		String connectionString = "API - getAudit";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aSuper.isAuthorised(sd, request.getRemoteUser());
		

		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			AuditManager am = new AuditManager(localisation);
			
			ArrayList<DataEndPoint> data = am.getDataEndPoints(sd, request, false);

			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(data);
			response = Response.ok(resp).build();
		} catch (SQLException e) {
			e.printStackTrace();
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * API version 1 /audity
	 * Get audit records for an individual survey in GeoJSON format
	 */
	@GET
	@Produces("application/json")
	@Path("/{sIdent}")
	public void getDataRecordsNew(@Context HttpServletRequest request,
			@Context HttpServletResponse response,
			@PathParam("sIdent") String sIdent,
			@QueryParam("start") int start,				// Primary key to start from
			@QueryParam("limit") int limit,				// Number of records to return
			@QueryParam("mgmt") boolean mgmt,
			@QueryParam("group") boolean group,			// If set include a dummy group value in the response, used by duplicate query
			@QueryParam("sort") String sort,				// Column Human Name to sort on
			@QueryParam("dirn") String dirn,				// Sort direction, asc || desc
			@QueryParam("form") String formName,			// Form name (optional only specify for a child form)
			@QueryParam("start_parkey") int start_parkey,// Parent key to start from
			@QueryParam("parkey") int parkey,			// Parent key (optional, use to get records that correspond to a single parent record)
			@QueryParam("hrk") String hrk,				// Unique key (optional, use to restrict records to a specific hrk)
			@QueryParam("format") String format,			// dt for datatables otherwise assume kobo
			@QueryParam("bad") String include_bad,		// yes | only | none Include records marked as bad
			@QueryParam("audit") String audit_set,		// if yes return audit data
			@QueryParam("merge_select_multiple") String merge, 	// If set to yes then do not put choices from select multiple questions in separate objects
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("geojson") String geojson		// if set to yes then format as geoJson
			) throws ApplicationException, Exception { 
		
		getDataRecords(request, response, sIdent, start, limit, mgmt, group, sort, dirn, formName, start_parkey,
				parkey, hrk, format, include_bad, audit_set, merge, geojson, tz);
	}
	
	private void getDataRecords(HttpServletRequest request,
			HttpServletResponse response,
			String sIdent,
			int start,				// Primary key to start from
			int limit,				// Number of records to return
			boolean mgmt,
			boolean group,			// If set include a dummy group value in the response, used by duplicate query
			String sort,				// Column Human Name to sort on
			String dirn,				// Sort direction, asc || desc
			String formName,			
			int start_parkey,		// Parent key to start from
			int parkey,				// Parent key (optional, use to get records that correspond to a single parent record)
			String hrk,				// Unique key (optional, use to restrict records to a specific hrk)
			String format,			// dt for datatables otherwise assume kobo
			String include_bad,		// yes | only | none Include records marked as bad
			String audit_set,		// if yes return audit data
			String merge, 			// If set to yes then do not put choices from select multiple questions in separate objects
			String geojson,			// If set to yes then render as geoJson rather than the kobo toolbox structure
			String tz				// Timezone
			) throws ApplicationException, Exception { 

		String connectionString = "koboToolboxApi - get data records";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		int sId = 0;
		int fId = 0;
		try {
			sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);		// Ident - the correct way
			
			if(formName != null) {
				fId = GeneralUtilityMethods.getFormId(sd, sId, formName);
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		String language = "none";

		Connection cResults = ResultsDataSource.getConnection(connectionString);

		String sqlGetManagedId = "select managed_id from survey where s_id = ?";
		PreparedStatement pstmtGetManagedId = null;

		String sqlGetMainForm = "select f_id, table_name from form where s_id = ? and parentform = 0;";
		PreparedStatement pstmtGetMainForm = null;

		String sqlGetForm = "select parentform, table_name from form where s_id = ? and f_id = ?;";
		PreparedStatement pstmtGetForm = null;

		PreparedStatement pstmt = null;

		String table_name = null;
		int parentform = 0;
		int managedId = 0;
		boolean getParkey = false;
		ResultSet rs = null;

		if(sort != null && dirn == null) {
			dirn = "asc";
		}
		
		boolean audit=false;
		if(audit_set != null && audit_set.equals("yes")) {
			audit = true;
		}
		
		boolean isGeoJson=false;
		if(geojson != null && geojson.equals("yes")) {
			isGeoJson = true;
		}
		
		boolean mergeSelectMultiple = false;
		if(merge != null && merge.equals("yes")) {
			mergeSelectMultiple = true;
		}

		if(include_bad == null) {
			include_bad = "none";
		}

		boolean isDt = false;
		if(format != null && format.equals("dt")) {
			isDt = true;
		}
		
		tz = (tz == null) ? "UTC" : tz;

		PrintWriter outWriter = null;
		try {

			lm.writeLog(sd, sId, request.getRemoteUser(), "view", "Managed Forms or the API. " + (hrk == null ? "" : "Hrk: " + hrk));
			
			response.setContentType("text/html; charset=UTF-8");
			response.setCharacterEncoding("UTF-8");
			outWriter = response.getWriter();
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			if(!GeneralUtilityMethods.isApiEnabled(sd, request.getRemoteUser())) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}
			
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);

			// Get the managed Id
			if(mgmt) {
				pstmtGetManagedId = sd.prepareStatement(sqlGetManagedId);
				pstmtGetManagedId.setInt(1, sId);
				rs = pstmtGetManagedId.executeQuery();
				if(rs.next()) {
					managedId = rs.getInt(1);
				}
				if(rs != null) try {rs.close(); rs = null;} catch(Exception e) {}
			}

			if(fId == 0) {
				pstmtGetMainForm = sd.prepareStatement(sqlGetMainForm);
				pstmtGetMainForm.setInt(1,sId);

				log.info("Getting main form: " + pstmtGetMainForm.toString() );
				rs = pstmtGetMainForm.executeQuery();
				if(rs.next()) {
					fId = rs.getInt(1);
					table_name = rs.getString(2);
				}
				if(rs != null) try {rs.close(); rs = null;} catch(Exception e) {}
			} else {
				getParkey = true;
				pstmtGetForm = sd.prepareStatement(sqlGetForm);
				pstmtGetForm.setInt(1,sId);
				pstmtGetForm.setInt(2,fId);

				log.info("Getting specific form: " + pstmtGetForm.toString() );
				if(rs != null) try {rs.close(); rs = null;} catch(Exception e) {}
				rs = pstmtGetForm.executeQuery();
				if(rs.next()) {
					parentform = rs.getInt(1);
					table_name = rs.getString(2);
				}
				if(rs != null) try {rs.close(); rs = null;} catch(Exception e) {}
			}

			ArrayList<TableColumn> columns = GeneralUtilityMethods.getColumnsInForm(
					sd,
					cResults,
					localisation,
					language,
					sId,
					sIdent,
					request.getRemoteUser(),
					parentform,
					fId,
					table_name,
					true,		// Read Only
					getParkey,	// Include parent key if the form is not the top level form (fId is 0)
					(include_bad.equals("yes") || include_bad.equals("only")),
					true,		// include instance id
					true,		// include other meta data
					true,		// include preloads
					true,		// include instancename
					true,		// include survey duration
					superUser,
					false,		// TODO include HXL
					audit,
					tz
					);

			if(mgmt) {
				CustomReportsManager crm = new CustomReportsManager ();
				ReportConfig config = crm.get(sd, managedId, -1);
				columns.addAll(config.columns);
			}

			TableDataManager tdm = new TableDataManager(localisation, tz);

			pstmt = tdm.getPreparedStatement(
					sd, 
					cResults,
					columns,
					urlprefix,
					sId,
					table_name,
					parkey,
					hrk,
					request.getRemoteUser(),
					sort,
					dirn,
					mgmt,
					group,
					isDt,
					start,
					limit,
					getParkey,
					start_parkey,
					superUser,
					false,			// Return records greater than or equal to primary key
					include_bad,
					null	,			// no custom filter
					null,			// key filter
					tz
					);
			
			// Write array start
			if(isDt) {
				outWriter.print("{\"data\":");
			}
			if(isGeoJson) {
				outWriter.print("{\"type\":\"FeatureCollection\",");		// type
																		// TODO metadata
				outWriter.print("\"features\":");						// Features
			}
			
			// Add feature data
			outWriter.print("[");
			
			if(pstmt != null) {
				log.info("KoboAPI data: " + pstmt.toString());
				/*
				 * Get the data record by record so it can be streamed
				 */
				if(rs != null) try {rs.close(); rs = null;} catch(Exception e) {}
				rs = pstmt.executeQuery();
				JSONObject jo = new JSONObject();
				int index = 0;
				while(jo != null) {
					
					jo =  tdm.getNextRecord(
							rs,
							columns,
							urlprefix,
							group,
							isDt,
							limit,
							mergeSelectMultiple,
							isGeoJson
							);
					if(jo != null) {
						if(index > 0) {
							outWriter.print(",");
						}
						outWriter.print(jo.toString());
					}
					
					index++;
					if (limit > 0 && index >= limit) {
						break;
					}

				}
				
			}
			
			outWriter.print("]");
			if(isDt) {
				outWriter.print("}");
			}
			
			if(isGeoJson) {
										// TODO bbox
				outWriter.print("}");	// close
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			outWriter.print(e.getMessage());
			//response = Response.serverError().entity(e.getMessage()).build();
		} finally {

			outWriter.flush(); 
			outWriter.close();
			
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetMainForm != null) {pstmtGetMainForm.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetForm != null) {pstmtGetForm.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetManagedId != null) {pstmtGetManagedId.close();	}} catch (SQLException e) {	}

			ResultsDataSource.closeConnection(connectionString, cResults);			
			SDDataSource.closeConnection(connectionString, sd);
		}

		//return response;

	}

}

