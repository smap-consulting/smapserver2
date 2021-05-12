package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.DataEndPoint;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.Line;
import org.smap.sdal.model.Point;
import org.smap.sdal.model.Polygon;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TableColumn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DataManager {
	
	private static Logger log =
			 Logger.getLogger(DataManager.class.getName());

	private ResourceBundle localisation;
	private String tz;
	
	public DataManager(ResourceBundle l, String timezone) {
		localisation = l;
		tz = timezone;
	}
	public ArrayList<DataEndPoint> getDataEndPoints(Connection sd, 
			HttpServletRequest request,
			boolean csv) throws SQLException {
		
		ArrayList<DataEndPoint> data = new ArrayList<DataEndPoint> ();
		
		/*
		 * Use existing survey manager call to get a list of surveys that the user can access
		 */
		ArrayList<Survey> surveys = null;	
		SurveyManager sm = new SurveyManager(localisation, "UTC");
		boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		surveys = sm.getSurveysAndForms(sd, request.getRemoteUser(), superUser);
		
		String urlprefix = request.getScheme() + "://" + request.getServerName();
		if(csv) {
			urlprefix += "/api/v1/data.csv/";	
		} else {
			urlprefix += "/api/v1/data/";
		}
		
		for(Survey s: surveys) {
			DataEndPoint dep = new DataEndPoint();
			dep.id = s.id;
			dep.id_string = s.ident;
			dep.title = s.displayName;
			dep.description = s.displayName;
			dep.url = urlprefix + dep.id_string + "?links=true";
			
			if(s.forms != null && s.forms.size() > 0) {
				dep.subforms = new HashMap<String, String> ();
				for(Form f : s.forms) {
					dep.subforms.put(f.name, urlprefix + dep.id_string + "?form=" + f.name);
				}
			}
			data.add(dep);
		}
		
		return data;
	}
	
	/*
	 * Get the instance data for a record or records in a survey
	 * Unlike the getInstances() function in SurveyManager, this request creates JSON objects that includes repeat information 
	 * in their correct location as defined by the survey definition
	 */
	public JSONArray getInstanceData(
			Connection sd,
			Connection cResults, 
			Survey s, 
			Form form, 
			int parkey,
			String hrk,				// Usually either hrk or instanceId would be used to identify the instance
			String instanceId,
			SurveyManager sm,
			boolean includeMeta
			) throws Exception {

		ArrayList<TableColumn> columns = null;
		JSONArray dataArray = new JSONArray();
		
		StringBuffer sql = new StringBuffer("");
		sql.append("select prikey ");
		
		PreparedStatement pstmt = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();

		try {
			
			TableDataManager tdm = new TableDataManager(localisation, tz);
			
			String serverName = GeneralUtilityMethods.getSubmissionServer(sd);
			String urlprefix = "https://" + serverName + "/";	
			
			if(!GeneralUtilityMethods.tableExists(cResults, form.tableName)) {
				throw new ApplicationException(localisation.getString("imp_no_file"));
			}
			columns = GeneralUtilityMethods.getColumnsInForm(
					sd,
					cResults,
					localisation,
					"none",
					s.id,
					s.ident,
					null,
					null,		// roles for column filtering
					0,			// parent form id
					form.id,
					form.tableName,
					true,		// Read Only
					false,		// Parent key
					false,
					true,			// include instance id
					true,			// include prikey
					includeMeta,	// include other meta data
					includeMeta,	// include preloads
					true,			// include instancename
					includeMeta,	// include survey duration
					false,
					false,			// include HXL
					false,
					tz,
					false,		// mgmt
					false,		// Accuracy and Altitude
					true		// Server calculates
					);

			/*
			 * Get the latest instanceid in case this record has been updated
			 */
			if(instanceId != null) {
				instanceId = GeneralUtilityMethods.getLatestInstanceId(cResults, form.tableName, instanceId);
			}
			
			/*
			 * Only return the last 20 minutes of submissions if the instance is not specified
			 * Assumes polling is at max 15 minutes
			 */
			String filter = null;
			if(instanceId == null && parkey == 0) {
				filter = "${_upload_time} > now() - {20_minutes}";
			}
			
			/*
			 * Get the data
			 */
			pstmt = tdm.getPreparedStatement(
					sd, 
					cResults,
					columns,
					urlprefix,
					s.id,
					0,			// SubForm Id - Not required
					form.tableName,
					parkey,
					hrk,
					null,
					null,		// roles for row filtering
					null,		// sort
					null,		// sort direction
					false,		// mgmt
					false,		// group
					false,		// prepare for data tables
					0,			// start
					false,		// get parkey
					0,			// start parkey
					false,		// super user
					false,		// Return records greater than or equal to primary key
					"none",		// include bad
					null,		// no custom filter
					null,		// key filter
					tz,
					instanceId,
					filter,			// advanced filter
					null,			// Date filter name
					null,			// Start date
					null				// End date
					);
			
			if(pstmt != null) {
				log.info("Data Manager Getting instance data: " + pstmt.toString());
				ResultSet rs = pstmt.executeQuery();
				
				while(rs.next()) {
					JSONObject data = new JSONObject();
					int prikey = 0;
					for (int i = 0; i < columns.size(); i++) {
						TableColumn c = columns.get(i);
						String name = null;

						
						name = c.displayName;
						if(name.equals("prikey")) {
							prikey = rs.getInt(i + 1);
							if(parkey == 0) {
								name = "id";	// for zapier
							}
							if(includeMeta) {
								data.put(name, prikey);
							}
						} else if (c.type.equals("geopoint")) {
							// Add Geometry (assume one geometry type per table)
							//instance.geometry = parser.parse(rs.getString(i + 1)).getAsJsonObject();
							Point point = gson.fromJson(rs.getString(i + 1), Point.class);
							Double lon = 0.0;
							Double lat = 0.0;
							if(point.coordinates != null && point.coordinates.size() > 0) {
								lon = point.coordinates.get(0);
								lat = point.coordinates.get(1);
							}
							data.put(name + "_lon", lon);
							data.put(name + "_lat", lat);
						} else if (c.type.equals("geoshape")) {
							// TODO
							//instance.polygon_geometry = gson.fromJson(rs.getString(i + 1), Polygon.class);
						} else if (c.type.equals("geotrace")) {
							//TODO
							//instance.line_geometry = gson.fromJson(rs.getString(i + 1), Line.class);
						} else if (c.type.equals("select1") && c.selectDisplayNames) {
							// Convert value to display name
							String value = rs.getString(i + 1);
							for(KeyValue kv: c.choices) {
								if(kv.k.equals(value)) {
									value = kv.v;
									break;
								}
							}
							data.put(name, value);
						} else if (c.type.equals("decimal")) {
							Double dValue = rs.getDouble(i + 1);
							dValue = Math.round(dValue * 10000.0) / 10000.0;
							data.put(name, dValue);
						} else if (c.type.equals("dateTime")) {
							String value = rs.getString(i + 1);
							if (value != null) {
								value = value.replaceAll("\\.[0-9]+", ""); // Remove milliseconds
							}
							data.put(name, value);
						} else if (c.type.equals("calculate")) {
							// This calculation may be a decimal - give it a go
							String v = rs.getString(i + 1);
							if (v != null && v.indexOf('.') > -1) {
								try {
									Double dValue = rs.getDouble(i + 1);
									dValue = Math.round(dValue * 10000.0) / 10000.0;
									data.put(name, dValue);
								} catch (Exception e) {
									data.put(name, rs.getString(i + 1)); // Assume text
								}
							} else {
								data.put(name, rs.getString(i + 1)); // Assume text
							}
	
						} else {
							data.put(name, rs.getString(i + 1));
						}
							
	
					}
				
					// Get the data for sub forms
					for(Form f : s.forms) {
						if(f.parentform == form.id) {
							int parentQuestion = f.parentQuestionIndex;
							Question q = form.questions.get(parentQuestion);
							String name = q.name;
							if(q.display_name != null) {
								name = q.display_name;
							}
							data.put(name, getInstanceData(
									sd,
									cResults,
									s,
									s.getSubFormQId(form, q.id),
									prikey,
									null,
									null,
									sm,
									false));
						}
					}	
					
					dataArray.put(data);	
	
				}

			}			
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
		}

		return dataArray;
	}
}
