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
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
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
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import model.MapConfig;
import model.MapResource;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.SDDataSource;
import org.apache.commons.fileupload2.core.DiskFileItemFactory;
import org.apache.commons.fileupload2.core.FileItem;
import org.apache.commons.fileupload2.jakarta.servlet6.JakartaServletFileUpload;
import org.smap.sdal.managers.CsvTableManager;
import org.smap.sdal.managers.FileManager;
import org.smap.sdal.managers.OfflineLayerManager;
import org.smap.sdal.managers.SharedResourceManager;
import org.smap.sdal.model.CsvTable;
import org.smap.sdal.model.OfflineLayer;
import org.smap.sdal.model.SharedHistoryItem;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
		String connectionString = "surveyKPI-SharedResources-getMaps";
		String user = request.getRemoteUser();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		orgLevelAuth.isAuthorised(sd, request, request.getRemoteUser());	
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
			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(maps);
			response = Response.ok(resp).build();		
			
		}  catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().build();
		} finally {
	
			if (pstmt != null) { try {pstmt.close();} catch (SQLException e) {}}

			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;		
	}
	
	/*
	 * Add or update a map
	 */
	@POST
	@Path("/maps")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateMap(@Context HttpServletRequest request, @FormParam("map") String mapString) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-SharedResources-UpdateMap";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		orgLevelAuth.isAuthorised(sd, request, request.getRemoteUser());	
		// End Authorisation	
		
		
		MapResource map = new Gson().fromJson(mapString, MapResource.class);
		
		// Store the config object as json
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		String configJson = gson.toJson(map.config);
		
		PreparedStatement pstmt = null;
		String sql = null;
		
		try {	
		
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

		
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			
			if(map.id < 1) {
						
				// New map		
				sql = "insert into map (o_id, name, map_type, description, config, version) " +
								" values (?, ?, ?, ?, ?, '1');";
						
				pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
				pstmt.setInt(1, o_id);
				pstmt.setString(2, HtmlSanitise.checkCleanName(map.name, localisation) );
				pstmt.setString(3, HtmlSanitise.checkCleanName(map.type,localisation));
				pstmt.setString(4, HtmlSanitise.checkCleanName(map.description, localisation));
				pstmt.setString(5, HtmlSanitise.checkCleanName(configJson, localisation));
						
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
				pstmt.setString(1, HtmlSanitise.checkCleanName(map.name, localisation));
				pstmt.setString(2, HtmlSanitise.checkCleanName(map.type, localisation));
				pstmt.setString(3, HtmlSanitise.checkCleanName(map.description, localisation));
				pstmt.setString(4, HtmlSanitise.checkCleanName(configJson, localisation));
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
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception adding map", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
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
		
		Response response = null;
		String connectionString = "surveyKPI-DeleteMap";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		orgLevelAuth.isAuthorised(sd, request, request.getRemoteUser());	
		// End Authorisation
		
		PreparedStatement pstmt = null;
		
		
		try {
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			
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
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	/*
	 * Delete a file
	 */
	@Path("/file/{name}")
	@DELETE
	public Response deleteFile(@Context HttpServletRequest request,
			@PathParam("name") String name,
			@QueryParam("survey_id") int sId) { 
		
		Response response = null;
		String tz = "UTC";
		String connectionString = "surveyKPI-DeleteFile";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		orgLevelDelete.isAuthorised(sd, request, request.getRemoteUser());	
		// End Authorisation		
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
		
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			String sIdent = null;
			if(sId > 0) {
				sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			}
			
			SharedResourceManager srm = new SharedResourceManager(localisation, tz);			
			String basePath = GeneralUtilityMethods.getBasePath(request);			
			srm.delete(sd, sIdent, sId, oId, basePath, request.getRemoteUser(), name);
		
			response = Response.ok().build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {			
			SDDataSource.closeConnection(connectionString, sd);			
		}

		return response;

	}
	
	/*
	 * Return history of updates to shared resources
	 */
	@GET
	@Produces("application/json")
	@Path("/media/{name}/history")
	public Response getMediaHistory(
			@Context HttpServletRequest request,
			@PathParam("name") String resource_name,
			@QueryParam("survey_id") int sId,
			@QueryParam("tz") String tz
			) throws IOException {
		
		Response response = null;
		String connectionString = "surveyKPI - getMediaHistory";
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		String user = request.getRemoteUser();
		
		if(tz == null) {
			tz = "UTC";
		}
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		orgLevelAuth.isAuthorised(sd, request, user);
		if(sId > 0) {
			orgLevelAuth.isValidSurvey(sd, user, sId, false, false);
		}
		// End Authorisation		
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
		
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			
			SharedResourceManager srm = new SharedResourceManager(localisation, tz);
			ArrayList<SharedHistoryItem> items = srm.getHistory(sd, sIdent, oId, user, resource_name, tz);
			response = Response.ok(gson.toJson(items)).build();		
			
		}  catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().build();
		} finally {

			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;		
	}
	
	/*
	 * Get a list of the available CSV files
	 */
	@GET
	@Path("/csv/files")
	@Produces("application/json")
	public String getServer(@Context HttpServletRequest request,
			@QueryParam("survey_id") int sId) throws Exception {

		String connectionString = "surveyKPI-Csv - Files";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		orgLevelAuth.isAuthorised(sd, request, request.getRemoteUser());
		// End Authorisation

		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();

		ArrayList<CsvTable> tables = null;
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			CsvTableManager tm = new CsvTableManager(sd, localisation);

			// Always include the organisation level CSV files
			tables = tm.getTables(oId, 0);

			// If a survey is specified also include CSV files uploaded for that survey
			if(sId > 0) {
				tables.addAll(tm.getTables(oId, sId));
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		if(tables == null) {
			tables = new ArrayList<CsvTable> ();
		}
		return gson.toJson(tables);
	}

	/*
	 * Get the latest shared resource file 
	 * The last file uploaded for this resource is returned
	 */
	@GET
	@Path("/latest/{resourceName}")
	@Produces("application/x-download")
	public Response getLatestSharedHistoryFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("resourceName") String resourceName,
			@QueryParam("sIdent") String sIdent) throws Exception {
		
		Response r = null;
		String connectionString = "SurveyKPI - Get Latest Shared History File";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request, request.getRemoteUser());
		} catch (Exception e) {
		}
		orgLevelAuth.isAuthorised(sd, request, request.getRemoteUser());
		if(sIdent != null) {
			orgLevelAuth.isValidSurveyIdent(sd, request.getRemoteUser(), sIdent, false, superUser);
		}
		// End Authorisation 
		
		try {
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			FileManager fm = new FileManager();
			r = fm.getLatestSharedHistoryFile(sd,  response, oId, resourceName, sIdent, 
					GeneralUtilityMethods.getBasePath(request)); 
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Error getting file", e);
			r = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, sd);	
		}
		
		return r;
	}

	/*
	 * Get the offline map layers for the organisation
	 */
	@GET
	@Produces("application/json")
	@Path("/offlinemaps")
	public Response getOfflineMaps(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-SharedResources-getOfflineMaps";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		orgLevelAuth.isAuthorised(sd, request, request.getRemoteUser());
		// End Authorisation

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			OfflineLayerManager olm = new OfflineLayerManager();

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(olm.getLayers(sd, oId))).build();

		} catch(Exception ex) {
			log.log(Level.SEVERE, ex.getMessage(), ex);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Add or update an offline map layer.  The file is optional, if it is not supplied
	 * only the name and description are updated.
	 */
	@POST
	@Path("/offlinemaps")
	public Response updateOfflineMap(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-SharedResources-updateOfflineMap";

		Connection sd = SDDataSource.getConnection(connectionString);

		// Authorisation - Access
		orgLevelDelete.isAuthorised(sd, request, request.getRemoteUser());
		// End Authorisation

		DiskFileItemFactory fileItemFactory = DiskFileItemFactory.builder().get();
		JakartaServletFileUpload uploadHandler = new JakartaServletFileUpload(fileItemFactory);

		int id = 0;
		String name = null;
		String description = null;
		String fileName = null;
		FileItem fileItem = null;
		ArrayList<Integer> projects = null;
		String user = request.getRemoteUser();

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, user));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);

			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();
			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();

				if(item.isFormField()) {
					String fieldName = item.getFieldName();
					if(fieldName.equals("id")) {
						try {
							id = Integer.parseInt(item.getString());
						} catch (Exception e) {
							id = 0;
						}
					} else if(fieldName.equals("name")) {
						name = item.getString(java.nio.charset.StandardCharsets.UTF_8).trim();
					} else if(fieldName.equals("description")) {
						description = item.getString(java.nio.charset.StandardCharsets.UTF_8).trim();
					} else if(fieldName.equals("projects")) {
						projects = new Gson().fromJson(item.getString(java.nio.charset.StandardCharsets.UTF_8),
								new com.google.gson.reflect.TypeToken<ArrayList<Integer>>() {}.getType());
					}
				} else if(item.getName() != null && item.getName().trim().length() > 0) {
					fileName = item.getName().trim();
					// Only keep the file name, browsers on some platforms send a path
					int sep = Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\'));
					if(sep >= 0) {
						fileName = fileName.substring(sep + 1);
					}
					fileItem = item;
				}
			}

			if(name == null || name.length() == 0) {
				return Response.status(Status.BAD_REQUEST).entity("A name is required").build();
			}
			if(id <= 0 && fileItem == null) {
				return Response.status(Status.BAD_REQUEST).entity("A file is required").build();
			}

			name = HtmlSanitise.checkCleanName(name, localisation);
			description = HtmlSanitise.checkCleanName(description, localisation);

			OfflineLayerManager olm = new OfflineLayerManager();
			OfflineLayer ol = olm.save(sd, oId, user, id, name, description, fileName,
					fileItem == null ? null : fileItem.getInputStream(),
					GeneralUtilityMethods.getBasePath(request));

			if(projects != null) {
				olm.setProjects(sd, oId, ol.id, projects);
			}

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(ol)).build();

		} catch (SQLException e) {
			String state = e.getSQLState();
			if(state != null && state.startsWith("23")) {
				// Duplicate layer name
				response = Response.status(Status.CONFLICT).build();
			} else {
				log.log(Level.SEVERE, "Error", e);
				response = Response.serverError().build();
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error saving offline layer", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}


	/*
	 * Delete an offline map layer
	 */
	@Path("/offlinemaps/{id}")
	@DELETE
	public Response deleteOfflineMap(@Context HttpServletRequest request,
			@PathParam("id") int id) {

		Response response = null;
		String connectionString = "surveyKPI-SharedResources-deleteOfflineMap";

		Connection sd = SDDataSource.getConnection(connectionString);

		// Authorisation - Access
		orgLevelDelete.isAuthorised(sd, request, request.getRemoteUser());
		// End Authorisation

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());

			OfflineLayerManager olm = new OfflineLayerManager();
			olm.delete(sd, oId, id);

			response = Response.ok().build();

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
}


