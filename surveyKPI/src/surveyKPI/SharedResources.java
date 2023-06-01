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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import model.MapConfig;
import model.MapResource;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.SharedResourceManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/shared")
public class SharedResources extends Application {
	
	Authorise orgLevelAuth = null;
	Authorise orgLevelDelete = null;
	
	public SharedResources() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		orgLevelAuth = new Authorise(authorisations, null);	
		
		ArrayList<String> authorisationsDelete = new ArrayList<String> ();	
		authorisationsDelete.add(Authorise.ANALYST);
		authorisationsDelete.add(Authorise.ADMIN);
		orgLevelDelete = new Authorise(authorisationsDelete, null);	
	}
	
	private static Logger log =
			 Logger.getLogger(SharedResources.class.getName());

	/*
	 * Return available maps
	 */
	@GET
	@Produces("application/json")
	@Path("/maps")
	public Response getMaps(
			@Context HttpServletRequest request
			) throws IOException {
		
		Response response = null;
		String user = request.getRemoteUser();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-SharedResources");
		orgLevelAuth.isAuthorised(sd, request.getRemoteUser());	
		// End Authorisation		
		
		PreparedStatement pstmt = null;
		try {
			ArrayList<MapResource> maps = new ArrayList<MapResource> ();	
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			String sql = "select id, name, map_type, description, config, version " +
					" from map " + 
					" where o_id = ? " +
					" order by name asc;";	
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
		
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				MapResource m = new MapResource();
				m.id = rs.getInt(1);
				m.name = rs.getString(2);
				m.type = rs.getString(3);
				m.description = rs.getString(4);
				
				String configJson = rs.getString(5);
				m.config = new Gson().fromJson(configJson, MapConfig.class);
				
				m.version = rs.getInt(6);
				
				maps.add(m);
			}
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(maps);
			response = Response.ok(resp).build();		
			
		}  catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().build();
		} finally {
	
			if (pstmt != null) { try {pstmt.close();} catch (SQLException e) {}}

			SDDataSource.closeConnection("surveyKPI-SharedResources", sd);
		}
		
		return response;		
	}
	
	/*
	 * Add or update a map
	 */
	@POST
	@Path("/maps")
	@Consumes("application/json")
	public Response updateMap(@Context HttpServletRequest request, @FormParam("map") String mapString) { 
		
		Response response = null;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-SharedResources");
		orgLevelAuth.isAuthorised(sd, request.getRemoteUser());	
		// End Authorisation	
		
		
		MapResource map = new Gson().fromJson(mapString, MapResource.class);
		
		// Store the config object as json
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String configJson = gson.toJson(map.config);
		
		PreparedStatement pstmt = null;
		String sql = null;
		
		try {	
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			if(map.id < 1) {
						
				// New map		
				sql = "insert into map (o_id, name, map_type, description, config, version) " +
								" values (?, ?, ?, ?, ?, '1');";
						
				pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
				pstmt.setInt(1, o_id);
				pstmt.setString(2, map.name);
				pstmt.setString(3, map.type);
				pstmt.setString(4, map.description);
				pstmt.setString(5, configJson);
						
				log.info("Insert map: " + pstmt.toString());
				pstmt.executeUpdate();
						
			} else {
				// Existing map
						
				sql = "update map set " +
						" name = ?, " + 
						" map_type = ?, " + 
						" description = ?, " + 
						" config = ?, " + 
						" version = version + 1 " + 
						" where id = ? " +
						" and o_id = ? " +		// Security check
						" and version = ?";		// Integrity check
						
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, map.name);
				pstmt.setString(2, map.type);
				pstmt.setString(3, map.description);
				pstmt.setString(4, configJson);
				pstmt.setInt(5,  map.id);
				pstmt.setInt(6,  o_id);
				pstmt.setInt(7, map.version);
										
				log.info("update map: " + pstmt.toString());
				pstmt.executeUpdate();
			
				response = Response.ok().build();
			} 
				
		} catch (SQLException e) {
			String state = e.getSQLState();
			log.info("sql state:" + state);
			if(state.startsWith("23")) {
				response = Response.status(Status.CONFLICT).build();
			} else {
				response = Response.serverError().build();
				log.log(Level.SEVERE,"Error", e);
			}
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-SharedResources", sd);
		}
		
		return response;
	}

	/*
	 * Delete a map
	 */
	@Path("/maps/{id}")
	@DELETE
	public Response deleteMap(@Context HttpServletRequest request,
			@PathParam("id") int id) { 
		
		ResponseBuilder builder = Response.ok();
		Response response = null;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-DeleteMap");
		orgLevelAuth.isAuthorised(sd, request.getRemoteUser());	
		// End Authorisation
		
		PreparedStatement pstmt = null;
		
		
		try {
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			String sql = "delete from map where id = ? and o_id = ?; ";
			pstmt = sd.prepareStatement(sql);	 			

			pstmt.setInt(1, id);
			pstmt.setInt(2,o_id);
			log.info("Delete: " + pstmt.toString());
			pstmt.executeUpdate();
			
			response = Response.ok().build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE,"SQL Exception", e);
		    response = Response.serverError().entity("SQL Error").build();
		} catch (AuthorisationException e) {
			log.info("Authorisation Exception");
		    response = Response.serverError().entity("Not authorised").build();
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-DeleteMap", sd);
			
		}

		return response;

	}
	
	/*
	 * Delete a file
	 */
	@Path("/file/{name}")
	@DELETE
	public Response deleteFile(@Context HttpServletRequest request,
			@PathParam("name") String name) { 
		
		Response response = null;
		String tz = "UTC";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-DeleteMap");
		orgLevelDelete.isAuthorised(sd, request.getRemoteUser());	
		// End Authorisation		
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
		
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			SharedResourceManager srm = new SharedResourceManager(localisation, tz);			
			String basePath = GeneralUtilityMethods.getBasePath(request);			
			srm.delete(sd, null, oId, basePath, request.getRemoteUser(), name);
		
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {			
			SDDataSource.closeConnection("surveyKPI-DeleteMap", sd);			
		}

		return response;

	}

}


