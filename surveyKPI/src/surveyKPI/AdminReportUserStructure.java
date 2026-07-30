package surveyKPI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.UserContext;
import utilities.XLSXAdminReportsHashMap;

/*
 * Produce a report on the enterprise and organisation administrators
 */
@Path("/adminreport/userstructure")
public class AdminReportUserStructure extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportUserStructure.class.getName());

	LogManager lm = new LogManager();		// Application log

	public AdminReportUserStructure() {
		ArrayList<String> authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ENTERPRISE);	// All enterprises
		authorisations.add(Authorise.ORG);			// Their own enterprise only
		a = new Authorise(authorisations, null);
	}

	/*
	 * Get the users that have enterprise or organisation administration rights
	 */
	@GET
	public Response exportSurveyXlsx (@Context HttpServletRequest request,
			@Context HttpServletResponse response) {

		Response responseVal;

		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - User Structure";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request, request.getRemoteUser());
		// End Authorisation


		try {

			UserContext context = GeneralUtilityMethods.getUserContext(sd, request, request.getRemoteUser());

			Locale locale = new Locale(context.language);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Enterprise administrators see every enterprise, organisation administrators only their own
			boolean enterpriseLevel = context.inGroup(Authorise.ENTERPRISE_ID);

			String filename = localisation.getString("ar_report_name");

			ArrayList<HashMap<String, String>> report = getUserReportStructure(sd, localisation,
					enterpriseLevel, context.eId);

			// Add header
			ArrayList<String> header = new ArrayList<String> ();
			header.add(localisation.getString("ar_ident"));
			header.add(localisation.getString("ar_user_name"));
			header.add(localisation.getString("email"));
			header.add(localisation.getString("bill_ent"));
			header.add(localisation.getString("bill_org"));
			header.add(localisation.getString("rep_ent_admin"));
			header.add(localisation.getString("rep_org_admin"));

			// Add elements
			ArrayList<String> elements = new ArrayList<String> ();
			elements.add("ident");
			elements.add("name");
			elements.add("email");
			elements.add("e");
			elements.add("o");
			elements.add("ent_admin");
			elements.add("org_admin");

			XLSXAdminReportsHashMap rm = new XLSXAdminReportsHashMap(localisation);
			responseVal = rm.getNewReport(sd, request, response, header, elements, report, filename);

		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return responseVal;

	}

	/*
	 * Users that are in the enterprise or organisation administrator security group,
	 * with a column for each of those groups
	 */
	private ArrayList<HashMap<String, String>> getUserReportStructure(Connection sd,
			ResourceBundle localisation, boolean enterpriseLevel, int eId) throws SQLException {

		ArrayList<HashMap<String, String>> rows = new ArrayList<HashMap<String, String>> ();
		StringBuilder sql = new StringBuilder("select e.name as ent_name, o.name as org_name, "
				+ "u.ident as ident, u.name as user_name, u.email as email, "
				+ "bool_or(ug.g_id = ?) as ent_admin, "
				+ "bool_or(ug.g_id = ?) as org_admin "
				+ "from users u "
				+ "inner join organisation o "
				+ "on u.o_id = o.id "
				+ "inner join enterprise e "
				+ "on o.e_id = e.id "
				+ "inner join user_group ug "
				+ "on ug.u_id = u.id "
				+ "where ug.g_id in (?, ?) "
				+ "and not u.temporary ");

		if(!enterpriseLevel) {
			// Always filter, even if the requester has no enterprise, so nothing can leak
			sql.append("and e.id = ? ");
		}

		sql.append("group by e.name, o.name, u.ident, u.name, u.email ");
		sql.append("order by u.ident");
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			pstmt.setInt(idx++, Authorise.ENTERPRISE_ID);
			pstmt.setInt(idx++, Authorise.ORG_ID);
			pstmt.setInt(idx++, Authorise.ENTERPRISE_ID);
			pstmt.setInt(idx++, Authorise.ORG_ID);
			if(!enterpriseLevel) {
				pstmt.setInt(idx++, eId);
			}

			log.info("Admin user structure report: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();

			String yes = localisation.getString("rep_yes");
			String no = localisation.getString("rep_no");

			while(rs.next()) {
				HashMap<String, String> ar = new HashMap<String, String>();
				ar.put("e", rs.getString("ent_name"));
				ar.put("o", rs.getString("org_name"));
				ar.put("ident", rs.getString("ident"));
				ar.put("name", rs.getString("user_name"));
				ar.put("email", rs.getString("email"));
				ar.put("ent_admin", rs.getBoolean("ent_admin") ? yes : no);
				ar.put("org_admin", rs.getBoolean("org_admin") ? yes : no);
				rows.add(ar);
			}

		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return rows;
	}



}
