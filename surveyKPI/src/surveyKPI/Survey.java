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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.ConsoleColumn;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.SurveyLinkDetails;
import org.smap.server.utilities.GetXForm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

/*
 * Return meta information about a survey
 *  For each table in the survey
 *  	Table Name
 *  	Form Name
 *  	Number of rows
 *  	If the table has a geometry column
 *      If it does, the bounding box of the survey
 *  For the whole survey
 *      The survey id
 *  	For each date column
 *  		The question id
 *  		The question name
 */

@Path("/survey/{sId}")
public class Survey extends Application {

	Authorise a = null;
	Authorise aManage = null;

	private static Logger log =
			Logger.getLogger(Survey.class.getName());

	LogManager lm = new LogManager();		// Application log

	public Survey() {

		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.MANAGE_TASKS);
		aManage = new Authorise(authorisations, null);

	}

	@Path("/download")
	@GET
	public Response getSurveyDownload(@Context HttpServletRequest request,
			@QueryParam("type") String type,
			@QueryParam("language") String language,
			@PathParam("sId") int sId) { 

		ResponseBuilder builder = Response.ok();
		Response response = null;
		String connectionString = "surveyKPI-Survey-getSurveyDownload";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidDelSurvey(sd, request.getRemoteUser(), sId, superUser);
		// End Authorisation

		if(type == null) {
			type = "xml";
		}

		PreparedStatement pstmt = null;
		try {
			
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";	// Set default for timezone
			
			String sourceName = null;
			String display_name = null;
			String fileBasePath = null;		// File path excluding extensions
			String folderPath = null;
			String filename = null;
			String sourceExt = null;
			int projectId = 0;

			String sql = null;
			ResultSet resultSet = null;

			/*
			 * Get the survey name (file name)
			 */
			sql = "SELECT s.display_name, s.p_id " +
					"FROM survey s " + 
					"where s.s_id = ?;";

			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			log.info("Get survey details: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			if (resultSet.next()) {				
				display_name = resultSet.getString(1);
				projectId = resultSet.getInt(2);

				String basePath = GeneralUtilityMethods.getBasePath(request);
				String target_name = GeneralUtilityMethods.convertDisplayNameToFileName(display_name, false);

				fileBasePath = basePath + "/templates/" + projectId + "/" + target_name; 
				folderPath = basePath + "/templates/" + projectId;

				String ext;		
				if(type.equals("codebook")) {
					ext = "_gen.pdf";		// Codebooks are written as PDF files 
					sourceExt = "_gen.xml";		// input name is xml for a codebook file
				} else if(type.equals("xml")) {	
					ext = "_gen.xml";		// Generate xml
					sourceExt = "_gen.xml";
				} else {
					ext = "." + type;
					sourceExt = "." + type;
				}
				sourceName = fileBasePath + sourceExt;		

				log.info("Source name: " + sourceName + " type: " + type);
				/*
				 * The XML file for a code book or an XML download needs to be generated so that it contains the latest changes
				 */
				if(type.equals("codebook") || type.equals("xml")) {

					try {
						SurveyTemplate template = new SurveyTemplate(localisation);
						template.readDatabase(sId, false);
						GetXForm xForm = new GetXForm(localisation, request.getRemoteUser(), tz);

						boolean useNodesets = !type.equals("codebook");		// For codebooks do not create nodesets in the XML
						String xmlForm = xForm.get(template, false, useNodesets, false, request.getRemoteUser(), request);

						// 1. Create the project folder if it does not exist
						File folder = new File(folderPath);
						FileUtils.forceMkdir(folder);

						File f = new File(sourceName);

						// 2. Re-Create the file
						if(f.exists()) {
							f.delete();
						} 
						f.createNewFile();
						FileWriter fw = new FileWriter(f.getAbsoluteFile());
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(xmlForm);
						bw.close();

						log.info("Written xml file to: " + f.getAbsoluteFile());
					} catch (Exception e) {
						log.log(Level.SEVERE, "", e);
						throw e;
					}
				}

				// Check for the existence of the source file
				File outputFile = null;
				String filepath = fileBasePath + ext;
				outputFile = new File(filepath);

				filename = target_name + ext;
				try {  		
					int code = 0;
					if(type.equals("codebook")) {
						String scriptPath = basePath + "_bin" + File.separator + "gettemplate.sh";
						Process proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", scriptPath + " " + sourceName +
								" " + language
								+ " >> /var/log/subscribers/attachments.log 2>&1"});
						code = proc.waitFor();
						if(code > 0) {
							int len;
							if ((len = proc.getErrorStream().available()) > 0) {
								byte[] buf = new byte[len];
								proc.getErrorStream().read(buf);
								log.info("Error: generating codebook");
								log.info("Command error:\t\"" + new String(buf) + "\"");
							}
						} else {
							int len;
							if ((len = proc.getInputStream().available()) > 0) {
								byte[] buf = new byte[len];
								proc.getInputStream().read(buf);
								log.info("Completed codebook process:\t\"" + new String(buf) + "\"");
							}
						}
					}

					builder = Response.ok(outputFile);
					if(type.equals("codebook")) {
						builder.header("Content-type","application/pdf; charset=UTF-8");
					} else if(type.equals("xml")) {
						builder.header("Content-type","text/xml; charset=UTF-8");
					}
					builder.header("Content-Disposition", "attachment;Filename=" + GeneralUtilityMethods.urlEncode(filename));
					response = builder.build();

				} catch (Exception e) {
					log.log(Level.SEVERE, "", e);
					response = Response.serverError().entity("<h1>Error retrieving " + type + " file</h1><p>" + e.getMessage() + "</p>").build();
				}
			} else {
				response = Response.serverError().entity("Invalid survey name: " + sourceName).build();

			}


		} catch (SQLException e) {
			log.log(Level.SEVERE,"No data available", e);
			response = Response.serverError().entity("No data available").build();
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
	 * Get a public link to a webform for this survey
	 */
	@Path("/link")
	@GET
	@Produces("application/text")
	public Response getLink(@Context HttpServletRequest request,
			@PathParam("sId") int sId) { 

		Response response = null;
		String connectionString = "surveyKPI-Survey-getLink";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		String sql = "update survey set public_link = ? where s_id = ?";
		PreparedStatement pstmt = null;
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
						
			int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);
			int pId = GeneralUtilityMethods.getProjectId(sd, sId);
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			String tempUserId = GeneralUtilityMethods.createTempUser(
					sd,
					localisation,
					oId,
					null, 
					"", 
					pId,
					null);
			
			String link = GeneralUtilityMethods.getUrlPrefix(request) 
					+ "app/myWork/webForm/id/" 
					+ tempUserId 
					+ "/" + sIdent;
			
			// Store the link with the survey
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, link);
			pstmt.setInt(2, sId);
			pstmt.executeUpdate();
			
			response = Response.ok(link).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString
					, sd);
		}
		
		return response;
	}
	

	
	/*
	 * Delete a public link to a webform for this survey
	 */
	@Path("/deletelink/{ident}")
	@DELETE
	@Produces("application/text")
	public Response deleteLink(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("ident") String ident) { 

		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Survey-deleteLink");
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		

		String sql = "update survey set public_link = null where s_id = ?";
		PreparedStatement pstmt = null;;
		try {
			
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			/*
			 * Delete the temporary user
			 */
			int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);
			GeneralUtilityMethods.deleteTempUser(sd, localisation, oId, ident);
			
			// Delete the link from the survey
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			pstmt.executeUpdate();
			
			response = Response.ok("").build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection("surveyKPI-Survey-deleteLink", sd);
		}
		
		return response;
	}
	
	/*
	 * Get the Survey Meta data
	 */
	private class DateInfo {	// Temporary storage of the array of date questions
		int qId;
		String name;
		String columnName;
		int fId;
		Date first;
		Date last;
	}

	@Path("/getMeta")
	@GET
	@Produces("application/json")
	public Response getSurveyMeta(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("extended") boolean extended) { 

		Response response = null;
		String topTableName = null;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Survey-getSurveyMeta");
		aManage.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		JSONObject jo = new JSONObject();

		Connection connectionRel = null; 
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		PreparedStatement pstmt3 = null;
		PreparedStatement pstmtTables = null;
		PreparedStatement pstmtGeom = null;

		try {
			String sqlTables = "select "
					+ "f.table_name, f.name, f_id, f.parentform "
					+ "from form f "
					+ "where f.s_id = ? "
					+ "and f.reference = 'false' " 
					+ "order by f.table_name";		
			pstmtTables = sd.prepareStatement(sqlTables);	

			String sqlGeom = "select q.q_id, q.qname "
					+ "from form f, question q "
					+ "where f.f_id = q.f_id "
					+ "and (q.qtype='geopoint' "
					+ "or q.qtype='geopolygon' "
					+ "or q.qtype='geolinestring' "
					+ "or q.qtype='geoshape' "
					+ "or q.qtype='geotrace' "
					+ "or q.qtype='geocompound') "
					+ "and not q.soft_deleted "
					+ "and f.f_id = ? "
					+ "and f.s_id = ?";
			pstmtGeom = sd.prepareStatement(sqlTables);	

			// Get the preloads
			ArrayList<MetaItem> preloads = GeneralUtilityMethods.getPreloads(sd, sId);
			
			// Add the sId to the response so that it is available in the survey meta object
			jo.put("sId", sId);

			connectionRel = ResultsDataSource.getConnection("surveyKPI-Survey-getSurveyMeta");

			String sql = null;
			ResultSet resultSet = null;
			ResultSet resultSetTable = null;
			ArrayList<DateInfo> dateInfoList = new ArrayList<DateInfo> ();
			HashMap<String, SurveyLinkDetails> completeLinks = new HashMap<String, SurveyLinkDetails> ();

			JSONArray ja = new JSONArray();
			JSONArray jLinks = new JSONArray();
			JSONArray jSurveys = new JSONArray();

			float [] bbox = new float[4]; 
			bbox[0] = 180;
			bbox[1] = 90;
			bbox[2] = -180;
			bbox[3] = -90;

			/*
			 * Start with the passed in survey
			 * If extended mode is set then we will need to retrieve forms for other surveys
			 */
			HashMap<Integer, Integer> completedSurveys = new HashMap <Integer, Integer> ();
			Stack<Integer> surveys = new Stack<Integer>();
			surveys.push(Integer.valueOf(sId));
			completedSurveys.put(Integer.valueOf(sId), Integer.valueOf(sId));
			
			/*
			 * Get Forms and row counts the next survey
			 */
			while (!surveys.empty()) {

				int currentSurveyId = surveys.pop().intValue();

				// If extended get the surveys that link to this survey
				if(extended) {

					// Get the surveys that link to this one
					log.info("meta: get surveys that link to this one");
					ArrayList<SurveyLinkDetails> sList = GeneralUtilityMethods.getLinkingSurveys(sd, currentSurveyId);
					if(sList.size() > 0) {
						for(SurveyLinkDetails link : sList) {
							completeLinks.put(link.getId(), link);
							int s = link.fromSurveyId;
							if(completedSurveys.get(s) != null) {
								log.info("Have already got meta data for survey " + s);
							} else {
								completedSurveys.put(Integer.valueOf(s), Integer.valueOf(s));
								surveys.push(s);
							}
						}	
					}

					// Get the surveys that this survey links to
					log.info("meta: get surveys that his survey links to");
					sList = GeneralUtilityMethods.getLinkedSurveys(sd, currentSurveyId);
					if(sList.size() > 0) {
						for(SurveyLinkDetails link : sList) {
							completeLinks.put(link.getId(), link);
							int s = link.toSurveyId;
							if(completedSurveys.get(s) != null) {
								log.info("Have already got meta data for survey " + s);
							} else {
								completedSurveys.put(Integer.valueOf(s), Integer.valueOf(s));
								surveys.push(s);
							}
						}	
					}	
				}

				pstmtTables.setInt(1, currentSurveyId);
				log.info("meta: Get tables :" + pstmtTables.toString());
				resultSet = pstmtTables.executeQuery();

				while (resultSet.next()) {							

					String tableName = resultSet.getString(1);
					String formName = resultSet.getString(2);
					int fId = resultSet.getInt(3);
					int p_id = resultSet.getInt(4);
					int rowCount = 0;
					boolean has_geom = false;
					ArrayList<String> geomQuestions = new ArrayList<String> ();

					try {
						sql = "select count(*) from " + tableName;
						try {if (pstmt2 != null) {pstmt2.close();}} catch (SQLException e) {}
						pstmt2 = connectionRel.prepareStatement(sql);
						resultSetTable = pstmt2.executeQuery();
						if(resultSetTable.next()) {
							rowCount = resultSetTable.getInt(1);
						}

					} catch (Exception e) {
						// If the table has not been created yet set row count to the default=0
					}

					// Get any geometry questions for this table
					pstmtGeom = sd.prepareStatement(sqlGeom);
					pstmtGeom.setInt(1, fId);
					pstmtGeom.setInt(2, sId);
					resultSetTable = pstmtGeom.executeQuery();
					while(resultSetTable.next()) {
						geomQuestions.add(resultSetTable.getString(2));
						has_geom = true;
					}
					// Add any location meta data
					if(p_id == 0) {
						for(MetaItem mi : preloads) {
							if(mi.type.equals("geopoint")) {
								geomQuestions.add(mi.name);
								has_geom = true;
							}
						}
					}

					/*
					 * Get first last record of any date fields
					 */
					for(int i = 0; i < dateInfoList.size(); i++) {
						DateInfo di = dateInfoList.get(i);
						if(fId == di.fId) {
							try {
								String name = di.columnName;
								sql = ":q" + name + "), max(" + name + ") FROM " + tableName + ";";

								try {if (pstmt2 != null) {pstmt2.close();}} catch (SQLException e) {}
								pstmt2 = connectionRel.prepareStatement(sql);
								log.info("meta: get first/last record of date field: " + pstmt2.toString());
								resultSetTable = pstmt2.executeQuery();
								if(resultSetTable.next()) {
									di.first = resultSetTable.getDate(1);
									di.last = resultSetTable.getDate(2);
								}

							} catch (Exception e) {
								// Ignore errors, for example table not created
								log.log(Level.SEVERE, e.getMessage(), e);
							}
						}
					}


					JSONObject jp = new JSONObject();
					jp.put("name", tableName);
					jp.put("form", formName);
					jp.put("rows", rowCount);
					jp.put("geom", has_geom);
					jp.put("s_id", currentSurveyId);
					jp.put("f_id", fId);
					jp.put("p_id", p_id);
					if(p_id == 0) {
						topTableName = tableName;
						jo.put("top_table", tableName);
					}
					jp.put("geomQuestions", geomQuestions);
					ja.put(jp);

				} 	
			}
			jo.put("forms", ja);

			if(extended) {
				for(String linkId : completeLinks.keySet()) {
					SurveyLinkDetails link = completeLinks.get(linkId);
					JSONObject jl = new JSONObject();
					jl.put("fromSurveyId", link.fromSurveyId);
					jl.put("fromFormId", link.fromFormId);
					jl.put("fromQuestionId", link.fromQuestionId);
					jl.put("toQuestionId", link.toQuestionId);

					jl.put("toSurveyId", link.toSurveyId);

					jLinks.put(jl);
				}
				jo.put("links", jLinks);

				for(Integer surveyId : completedSurveys.keySet()) {
					JSONObject js = new JSONObject();
					String sName = GeneralUtilityMethods.getSurveyName(sd, surveyId);
					js.put("sId", surveyId);
					js.put("name", sName);
					jSurveys.put(js);
				}
				jo.put("surveys", jSurveys);
			}

			/*
			 * Add the date information
			 */
			/*
			 * Get Date columns available in this survey
			 * The maximum and minimum value for these dates will be added when 
			 * the results data for each table is checked
			 */
			sql = "select q.q_id, q.qname, f.f_id, q.column_name "
					+ "from form f, question q "
					+ "where f.f_id = q.f_id "
					+ "and (q.qtype='date' "
					+ "or q.qtype='dateTime') "
					+ "and f.s_id = ?"; 	


			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			resultSet = pstmt.executeQuery();

			while (resultSet.next()) {	
				DateInfo di = new DateInfo();
				di.qId = resultSet.getInt(1);
				di.name = resultSet.getString(2);
				di.fId = resultSet.getInt(3);
				di.columnName = resultSet.getString(4);
				dateInfoList.add(di);
			}	

			// Add upload time
			if(GeneralUtilityMethods.columnType(connectionRel, topTableName, SmapServerMeta.UPLOAD_TIME_NAME) != null) {
				DateInfo di = new DateInfo();

				di.columnName = SmapServerMeta.UPLOAD_TIME_NAME;
				di.name = "Upload Time";
				di.qId = SmapServerMeta.UPLOAD_TIME_ID;
				dateInfoList.add(di);
			}
			
			// Add scheduled start
			if(GeneralUtilityMethods.columnType(connectionRel, topTableName, SmapServerMeta.SCHEDULED_START_NAME) != null) {
				DateInfo di = new DateInfo();

				di.columnName = SmapServerMeta.SCHEDULED_START_NAME;
				di.name = "Scheduled Start";
				di.qId = SmapServerMeta.SCHEDULED_START_ID;
				dateInfoList.add(di);
			}
			
			// Add preloads
			int metaId = MetaItem.INITIAL_ID;		// Backward compatability to when meta items did not have an id
			for(MetaItem mi : preloads) {
				if(mi.type.equals("dateTime") || mi.type.equals("date")) {
					DateInfo di = new DateInfo();

					int id = (mi.id <= MetaItem.INITIAL_ID) ? mi.id : metaId--;
					di.columnName = mi.columnName;
					if(mi.display_name != null && mi.display_name.trim().length() > 0) {
						di.name = mi.display_name;
					} else {
						di.name = mi.name;
					}
					
					di.qId = id;
					dateInfoList.add(di);
				}
			}

			ja = new JSONArray();
			for (int i = 0; i < dateInfoList.size(); i++) {
				DateInfo di = dateInfoList.get(i);
				JSONObject jp = new JSONObject();
				jp.put("id", di.qId);
				jp.put("name", di.name);
				jp.put("col", di.columnName);
				jp.put("first", di.first);
				jp.put("last", di.last);
				ja.put(jp);
			}			
			jo.put("dates", ja);

			/*
			 * Add the bounding box
			 *  Don't set the bbox if there is no location data, that is the left is greater than right
			 *  If there was only one point then add a buffer around that point
			 */		
			if(bbox[0] <= bbox[2] && bbox[1] <= bbox[3]) {
				if(bbox[0] == bbox[2]) {	// Zero width
					bbox[0] -= 0.05;		// Size in degrees 
					bbox[2] += 0.05;
				}
				if(bbox[1] == bbox[3]) {	// Zero height
					bbox[1] -= 0.05;		// Size in degrees 
					bbox[3] += 0.05;
				}
				JSONArray bb = new JSONArray();
				for(int i = 0; i < bbox.length; i++) {
					bb.put(bbox[i]);
				}
				jo.put("bbox", bb);
			} 

			/*
			 * Get other survey details
			 */
			sql = "select "
					+ "s.display_name, s.deleted, s.p_id, s.ident, s.model, s.task_file "
					+ "from survey s "
					+ "where s.s_id = ?";

			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			log.info("meta: get other survey details: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			if (resultSet.next()) {				
				jo.put("name", resultSet.getString(1));
				jo.put("deleted", resultSet.getBoolean(2));
				jo.put("project", resultSet.getInt(3));
				jo.put("survey_ident", resultSet.getString(4));
				jo.put("model", resultSet.getString(5));
				jo.put("task_file", resultSet.getBoolean(6));
			}

			String resp = jo.toString();
			response = Response.ok(resp).build();


		} catch (SQLException e) {
			log.log(Level.SEVERE,"No data available", e);
			response = Response.serverError().entity("No data available").build();
		} catch (JSONException e) {
			log.log(Level.SEVERE,"", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmt2 != null) {pstmt2.close();}} catch (SQLException e) {}
			try {if (pstmt3 != null) {pstmt3.close();}} catch (SQLException e) {}
			try {if (pstmtTables != null) {pstmtTables.close();}} catch (SQLException e) {}
			try {if (pstmtGeom != null) {pstmtGeom.close();}} catch (SQLException e) {}

			SDDataSource.closeConnection("surveyKPI-Survey-getSurveyMeta", sd);
			ResultsDataSource.closeConnection("surveyKPI-Survey-getSurveyMeta", connectionRel);
		}
		
		return response;
	}

	/*
	 * Prevent any more submissions of the survey
	 */
	@Path("/block")
	@POST
	@Consumes("application/json")
	public Response block(
			@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("set") boolean set) { 

		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionRequest = "surveyKPI-Survey";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionRequest);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation

		PreparedStatement pstmt = null;
		try {

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			/*
			 * Get Forms and row counts in this survey
			 */
			String sql = "update survey set blocked = ? where s_id = ?;";		

			log.info(sql + " : " + set + " : " + sId);
			pstmt = sd.prepareStatement(sql);	
			pstmt.setBoolean(1, set);
			pstmt.setInt(2, sId);
			int count = pstmt.executeUpdate();

			if(count == 0) {
				log.info("Error: Failed to update blocked status");
			} else {
				lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.BLOCK, set ? " : block survey : " : " : unblock survey : ", 0, request.getServerName());
				log.info("userevent: " + request.getRemoteUser() + (set ? " : block survey : " : " : unblock survey : ") + sId);
			}

			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);

			response = Response.ok().build();



		} catch (SQLException e) {
			log.log(Level.SEVERE,"No data available", e);
			response = Response.serverError().entity("No data available").build();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}

			SDDataSource.closeConnection(connectionRequest, sd);
		}

		return response;
	}

	/*
	 * Remove media attachments
	 */
	@Path("/remove_media")
	@POST
	@Consumes("application/json")
	public Response remove_media(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@FormParam("qId") int qId,
			@FormParam("oId") int oId,
			@FormParam("text_id") String text_id) { 

		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Survey");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation

		PreparedStatement pstmt = null;
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String sql = null;
			if(text_id != null) {
				// Survey level media
				sql = "delete FROM translation t " +
						" where t.s_id = ? " +
						" and t.text_id = ? "; 
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, sId);
				pstmt.setString(2, text_id);
			} else 	if(oId == -1) {
				// Question level media
				sql = "delete FROM translation t " +
						" where t.s_id = ? " +
						" and (t.type = 'image' or t.type = 'video' or t.type = 'audio') " +
						" and t.text_id in (select q.qtext_id from question q " + 
						" where q.q_id = ?); "; 
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, sId);
				pstmt.setInt(2, qId);

			} else {
				// Option level media
				sql = "delete FROM translation t " +
						" where t.s_id = ? " +
						" and (t.type = 'image' or t.type = 'video' or t.type = 'audio') " +
						" and t.text_id in (select o.label_id from option o " + 
						" where o.o_id = ?); "; 
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, sId);
				pstmt.setInt(2, oId);
			}


			int count = pstmt.executeUpdate();

			if(count == 0) {
				log.info("Error: Failed to remove any media");
			} else {
				log.info("Info: Media removed");
			}

			// Record the message so that devices can be notified
			MessagingManager mm = new MessagingManager(localisation);
			mm.surveyChange(sd, sId, 0);

			response = Response.ok().build();

		} catch (SQLException e) {
			log.log(Level.SEVERE,"", e);
			response = Response.serverError().entity("Error").build();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}

			SDDataSource.closeConnection("surveyKPI-Survey", sd);

		}

		return response;
	}
	
	/*
	 * Save the console settings for this survey
	 */
	@Path("/console_settings/columns")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response save_console_columns(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@FormParam("columns") String sColumns) { 

		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey-save console columns";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);	// Validate that the user can access this survey
		// End Authorisation

		String sql = "update survey_settings "
				+ "set columns = ? "
				+ "where u_id = ? "
				+ "and s_ident = ? ";		
		PreparedStatement pstmt = null;
		
		String sqlInsert = "insert into survey_settings (u_id, s_ident, columns) "
				+ "values (?, ?, ?) ";		
		PreparedStatement pstmtInsert = null;
		try {

			if(sColumns != null) {
				Type type = new TypeToken<HashMap<String, ConsoleColumn>>(){}.getType();
				Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
				HashMap<String, ConsoleColumn> columns = gson.fromJson(sColumns, type);
				
				int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				String vColumns = gson.toJson(columns);
				
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, vColumns);
				pstmt.setInt(2, uId);
				pstmt.setString(3, sIdent);
				int count = pstmt.executeUpdate();
				
				if(count == 0) {
					pstmtInsert = sd.prepareStatement(sqlInsert);
					pstmtInsert.setInt(1, uId);
					pstmtInsert.setString(2, sIdent);
					pstmtInsert.setString(3, vColumns);
					pstmtInsert.executeUpdate();
				}
			} 

			response = Response.ok().build();

		} catch (SQLException e) {
			log.log(Level.SEVERE,"", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtInsert != null) {pstmtInsert.close();}} catch (SQLException e) {}

			SDDataSource.closeConnection(connectionString, sd);

		}

		return response;
	}

	/*
	 * Deletes a survey
	 *  @param tables if set to yes, data tables will also be deleted
	 *  @param hard if set to true then data and tables will be physically deleted otherwise they will only
	 *    be marked as deleted in the meta data tables but will remain in the database
	 *  @param delData if set to yes then the results tables will be deleted even if they have data
	 */
	@DELETE
	public Response deleteSurvey(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("tables") String tables,
			@QueryParam("hard") boolean hard,
			@QueryParam("undelete") boolean undelete,
			@QueryParam("delData") boolean delData) { 

		Response response = null;
		String connectionString ="surveyKPI-Survey-Delete";

		Connection cResults = null;
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		boolean surveyMustBeDeleted = undelete || hard;
		a.isValidSurvey(sd, request.getRemoteUser(), sId, surveyMustBeDeleted, superUser);  // Note if hard delete is set to true the survey should have already been soft deleted
		// End Authorisation

		if(sId != 0) {
			
			try {
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				SurveyManager mgr = new SurveyManager(localisation, "UTC");
				
				if(undelete) {				 
					mgr.restore(sd, sId, request.getRemoteUser());	// Restore the survey
				} else {
					cResults = ResultsDataSource.getConnection(connectionString);
					String basePath = GeneralUtilityMethods.getBasePath(request);
					
					mgr.delete(sd, 
							cResults, 
							sId, 
							hard, 
							delData, 
							request.getRemoteUser(), 
							basePath,
							tables,
							0);
				}
				
				// Record the message so that devices can be notified
				MessagingManager mm = new MessagingManager(localisation);
				mm.surveyChange(sd, sId, 0);

				response = Response.status(Status.OK).entity("").build();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Error", e);
				response = Response.serverError().entity(e.getMessage()).build();

			} finally {

				SDDataSource.closeConnection("surveyKPI-Survey", sd);
				ResultsDataSource.closeConnection("surveyKPI-Survey", cResults);
			}
		}

		return response; 
	}

}

