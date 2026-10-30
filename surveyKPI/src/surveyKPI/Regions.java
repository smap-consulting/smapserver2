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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.RegionManager;
import org.smap.sdal.model.RegionGrid;

import com.google.gson.Gson;

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

/*
 * The regions a map panel can group a survey's records by: hexagonal grids made for the survey
 * bundle and the polygons recorded by the survey's geoshape questions
 */
@Path("/regions")
public class Regions extends Application {

	Authorise aView = null;		// List the regions
	Authorise aEdit = null;		// Create and delete grids

	private static Logger log =
			 Logger.getLogger(Regions.class.getName());

	private class Shape {
		String question;
		String form;
		int sId;			// Survey holding the polygons, this survey or another in its bundle
		String sIdent;
		String survey;
	}

	private class RegionList {
		ArrayList<RegionGrid> grids;
		ArrayList<Shape> shapes = new ArrayList<> ();
	}

	private class NewGrid {
		int sId;
		String name;
		int cellSize;
		double [] extent;		// minLon, minLat, maxLon, maxLat
	}

	public Regions() {

		ArrayList<String> authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		aView = new Authorise(authorisations, null);

		authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		aEdit = new Authorise(authorisations, null);
	}

	/*
	 * Get the grids and polygon questions available to a survey
	 */
	@GET
	@Produces("application/json")
	public Response getRegions(@Context HttpServletRequest request,
			@QueryParam("sId") int sId) {

		String connectionString = "surveyKPI-Regions";
		Connection sd = SDDataSource.getConnection(connectionString);
		aView.isAuthorised(sd, request, request.getRemoteUser());
		aView.isValidSurvey(sd, request.getRemoteUser(), sId, false, isSuperUser(sd, request));

		Response response = null;
		PreparedStatement pstmt = null;
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			String groupIdent = RegionManager.getBundleIdent(sd, sId);

			RegionList list = new RegionList();
			list.grids = new RegionManager(getLocalisation(sd, request)).getGrids(sd, oId, groupIdent);

			// The geoshape questions of the surveys in the bundle that the user can access, this survey's first
			// The access check is the one Authorise.isValidSurvey makes, which closes the connection when it fails
			boolean superUser = isSuperUser(sd, request);
			StringBuilder sql = new StringBuilder("select q.qname, f.name, s.s_id, s.ident, s.display_name "
					+ "from question q, form f, survey s, users u, user_project up "
					+ "where q.f_id = f.f_id "
					+ "and f.s_id = s.s_id "
					+ "and coalesce(s.group_survey_ident, s.ident) = ? "
					+ "and not s.deleted "
					+ "and not s.hidden "
					+ "and q.qtype = 'geoshape' "
					+ "and not q.soft_deleted "
					+ "and s.p_id = up.p_id "
					+ "and up.u_id = u.id "
					+ "and u.ident = ? ");
			if(!superUser) {
				sql.append(GeneralUtilityMethods.getSurveyRBAC());
			}
			sql.append("order by (s.s_id = ?) desc, s.display_name, f.f_id, q.seq");
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			pstmt.setString(idx++, groupIdent);
			pstmt.setString(idx++, request.getRemoteUser());
			if(!superUser) {
				pstmt.setString(idx++, request.getRemoteUser());
			}
			pstmt.setInt(idx++, sId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				Shape shape = new Shape();
				shape.question = rs.getString(1);
				shape.form = rs.getString(2);
				shape.sId = rs.getInt(3);
				shape.sIdent = rs.getString(4);
				shape.survey = rs.getString(5);
				list.shapes.add(shape);
			}

			response = Response.ok(new Gson().toJson(list)).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Create a grid for the survey's bundle over an area of the map
	 */
	@POST
	@Produces("application/json")
	public Response createGrid(@Context HttpServletRequest request,
			@FormParam("grid") String gridString) {

		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();
		}

		NewGrid grid = new Gson().fromJson(gridString, NewGrid.class);
		if(grid == null) {
			return Response.status(Response.Status.BAD_REQUEST).build();
		}

		String connectionString = "surveyKPI-Regions";
		Connection sd = SDDataSource.getConnection(connectionString);
		aEdit.isAuthorised(sd, request, request.getRemoteUser());
		aEdit.isValidSurvey(sd, request.getRemoteUser(), grid.sId, false, isSuperUser(sd, request));

		Connection cResults = null;
		Response response = null;
		try {
			cResults = ResultsDataSource.getConnection(connectionString);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			String groupIdent = RegionManager.getBundleIdent(sd, grid.sId);

			int id = new RegionManager(getLocalisation(sd, request)).createGrid(sd, cResults, oId, groupIdent,
					grid.name, grid.cellSize, grid.extent, request.getRemoteUser());

			HashMap<String, Integer> created = new HashMap<> ();
			created.put("id", id);
			response = Response.ok(new Gson().toJson(created)).build();

		} catch (ApplicationException e) {
			response = Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;
	}

	/*
	 * Delete a grid of the survey's bundle
	 */
	@DELETE
	@Path("/{id}")
	public Response deleteGrid(@Context HttpServletRequest request,
			@PathParam("id") int id,
			@QueryParam("sId") int sId) {

		String connectionString = "surveyKPI-Regions";
		Connection sd = SDDataSource.getConnection(connectionString);
		aEdit.isAuthorised(sd, request, request.getRemoteUser());
		aEdit.isValidSurvey(sd, request.getRemoteUser(), sId, false, isSuperUser(sd, request));

		Connection cResults = null;
		Response response = null;
		try {
			cResults = ResultsDataSource.getConnection(connectionString);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			String groupIdent = RegionManager.getBundleIdent(sd, sId);

			if(new RegionManager(getLocalisation(sd, request)).deleteGrid(sd, cResults, oId, groupIdent, id)) {
				response = Response.ok().build();
			} else {
				response = Response.status(Response.Status.NOT_FOUND).build();
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;
	}

	private boolean isSuperUser(Connection sd, HttpServletRequest request) {
		try {
			return GeneralUtilityMethods.isSuperUser(sd, request, request.getRemoteUser());
		} catch (Exception e) {
			return false;
		}
	}

	private ResourceBundle getLocalisation(Connection sd, HttpServletRequest request) throws SQLException {
		Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
		return ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
	}
}
