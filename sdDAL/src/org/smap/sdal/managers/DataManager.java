package org.smap.sdal.managers;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.RateLimiter;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.ConsoleTotals;
import org.smap.sdal.model.DataEndPoint;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.FormLink;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.Point;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.ReportConfig;
import org.smap.sdal.model.SMSNumber;
import org.smap.sdal.model.SqlParam;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.SurveySettingsDefn;
import org.smap.sdal.model.SurveyViewDefn;
import org.smap.sdal.model.TableColumn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DataManager {
	
	private static Logger log = Logger.getLogger(DataManager.class.getName());
	LogManager lm = new LogManager();		// Application log
	
	private ResourceBundle localisation;
	private String tz;
	Authorise a = null;
	
	public DataManager(ResourceBundle l, String timezone) {
		localisation = l;
		tz = timezone;
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		a = new Authorise(authorisations, null);
	}
	public ArrayList<DataEndPoint> getDataEndPoints(Connection sd, 
			String remoteUser,
			boolean csv,
			String urlprefix,
			String version) throws SQLException {
		
		ArrayList<DataEndPoint> data = new ArrayList<DataEndPoint> ();
		
		/*
		 * Use existing survey manager call to get a list of surveys that the user can access
		 */
		ArrayList<Survey> surveys = null;	
		SurveyManager sm = new SurveyManager(localisation, "UTC");
		boolean superUser = GeneralUtilityMethods.isSuperUser(sd, remoteUser);
		surveys = sm.getSurveysAndForms(sd, remoteUser, superUser);
		
		urlprefix += "api/" + version;
		if(csv) {
			urlprefix += "/data.csv/";
		} else {
			urlprefix += "/data/";
		}
		
		for(Survey s: surveys) {
			DataEndPoint dep = new DataEndPoint();
			dep.id = s.surveyData.id;
			dep.id_string = s.surveyData.ident;
			dep.title = s.surveyData.displayName;
			dep.description = s.surveyData.displayName;
			dep.url = urlprefix + dep.id_string + "?links=true";
			
			if(s.surveyData.forms != null && s.surveyData.forms.size() > 0) {
				dep.subforms = new HashMap<String, String> ();
				for(Form f : s.surveyData.forms) {
					dep.subforms.put(f.name, urlprefix + dep.id_string + "?form=" + f.name);
				}
			}
			data.add(dep);
		}
		
		return data;
	}
	
	/*
	 * Get record(s) in JSON format
	 * Return as a hierarchy of forms and subforms rather than separating the sub form data out into arrays
	 */
	public Response getRecordHierarchy(
			Connection sd,
			Connection cResults,
			HttpServletRequest request,
			String sIdent,
			int sId,
			String uuid,
			String merge, 			// If set to yes then do not put choices from select multiple questions in separate objects
			ResourceBundle localisation,
			String tz,				// Timezone
			boolean includeMeta,
			String urlprefix,
			String attachmentPrefix
			) throws ApplicationException, Exception { 

		Response response;

		lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.API_SINGLE_VIEW, "Hierarchy view. ", 0, request.getServerName());

		DataManager dm = new DataManager(localisation, tz);

		SurveyManager sm = new SurveyManager(localisation, tz);

		Survey s = sm.getById(
				sd, 
				cResults, 
				request.getRemoteUser(),
				false,
				sId, 
				true, 		// full
				null, 		// basepath
				null, 		// instance id
				false, 		// get results
				false, 		// generate dummy values
				true, 		// get property questions
				false, 		// get soft deleted
				true, 		// get HRK
				"external", 	// get external options
				false, 		// get change history
				false, 		// get roles
				true,		// superuser 
				null, 		// geomformat
				false, 		// reference surveys
				false,		// only get launched
				false		// Don't merge set value into default values
				);

		JSONArray data = null;
		if(s != null) {
			data = dm.getInstanceData(
					sd,
					cResults,
					s,
					s.getFirstForm(),
					0,
					null,
					uuid,
					sm,
					includeMeta,
					urlprefix,
					attachmentPrefix);
		} else {
			throw new ApplicationException(localisation.getString("mf_snf"));
		}

		String resp = null;
		if(uuid == null) {
			resp = "[]";
		} else {
			resp = "{}";
		}
		if(data != null && data.length() > 0) {
			if(uuid == null) {
				resp = data.toString();
			} else {
				resp = data.getString(0);
			}
		}
		response = Response.ok(resp).build();


		return response;

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
			boolean includeMeta,
			String urlprefix,
			String attachmentPrefix
			) throws Exception {

		ArrayList<TableColumn> columns = null;
		JSONArray dataArray = new JSONArray();
		
		StringBuffer sql = new StringBuffer("");
		sql.append("select prikey ");
		
		PreparedStatement pstmt = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();

		try {
			
			TableDataManager tdm = new TableDataManager(localisation, tz);
			
			if(!GeneralUtilityMethods.tableExists(cResults, form.tableName)) {
				throw new ApplicationException(localisation.getString("imp_no_file"));
			}
			columns = GeneralUtilityMethods.getColumnsInForm(
					sd,
					cResults,
					localisation,
					"none",
					s.surveyData.id,
					s.surveyData.ident,
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
					includeMeta,	// Include HRK
					includeMeta,	// include other meta data
					includeMeta,	// include preloads
					true,			// include instancename
					includeMeta,	// include survey duration
					includeMeta,	// include case management
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
					attachmentPrefix,
					s.surveyData.id,
					s.surveyData.ident,
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
					"yes",			// return completed
					null,			// case management settings can be null
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
							if(point != null && point.coordinates != null && point.coordinates.size() > 0) {
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
					for(Form f : s.surveyData.forms) {
						if(f.parentform == form.id) {
							int parentQuestion = f.parentQuestionIndex;
							Question q = form.questions.get(parentQuestion);
							String name = q.name;
							if(q.display_name != null && q.display_name.trim().length() > 0) {
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
									false,
									urlprefix,
									attachmentPrefix));
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
	
	/*
	 * Get records for an individual survey in JSON format
	 */
	public void getDataRecords(Connection sd,
			String connectionString,
			HttpServletRequest request,
			String remoteUser,
			HttpServletResponse response,
			String sIdent,
			int start,				// Primary key to start from
			int limit,				// Number of records to return
			boolean mgmt,
			String oversightSurvey,		// Console
			int viewId,				// Console
			boolean schema,			// Console - return schema
			boolean group,			// If set include a dummy group value in the response, used by duplicate query
			String sort,				// Column Human Name to sort on
			String dirn,				// Sort direction, asc || desc
			String formName,			
			int start_parkey,		// Parent key to start from
			int parkey,				// Parent key (optional, use to get records that correspond to a single parent record)
			String hrk,				// Unique key (optional, use to restrict records to a specific hrk)
			String format,			// dt for datatables otherwise assume kobo
			String include_bad,		// yes | only | none Include records marked as bad
			String include_completed,
			String audit_set,		// if yes return audit data
			String merge, 			// If set to yes then do not put choices from select multiple questions in separate objects
			String geojson,			// If set to yes then render as geoJson rather than the kobo toolbox structure
			String geomQuestion,		// Set to the name of the question with the geometry
			String tz,				// Timezone
			boolean incLinks,
			String advanced_filter,
			String dd_filter,		// Console calls only
			int prikey,
			String dd_hrk,
			String dateName,
			Date startDate,
			Date endDate,
			boolean getSettings,		// Set true if the settings are stored in the database, otherwise they are passed with the request
			String instanceId,
			boolean includeMeta,
			String urlprefix,
			String attachmentPrefix,
			String selectedInstanceId,
			int pageLen							// Set by the console and remembered on the server - not used
			) throws ApplicationException, Exception { 


		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, remoteUser);
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(),e);
		}
		int sId = 0;
		int fId = 0;
		boolean errorMsgAddClosingArray = false;
		int errorMsgAddClosingBracket = 0;
		try {
			/*
			 * Hack - some older clients still pass the survey id rather than the ident
			 * Until these are fixed handle either
			 */
			log.info("Get data records for survey ident: " + sIdent);
			if(sIdent.startsWith("s")) {
				sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);		// Ident - the correct way
			} else {
				sId = Integer.parseInt(sIdent);							// Id the old way
				sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			}
			if(formName != null) {
				fId = GeneralUtilityMethods.getFormId(sd, sId, formName);
			}
		} catch (SQLException e1) {
			log.log(Level.SEVERE, e1.getMessage(), e1);
		}
		
		String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
		
		a.isAuthorised(sd, remoteUser);
		a.isValidSurvey(sd, remoteUser, sId, false, superUser);
		if(viewId > 0) {
			a.isValidView(sd, remoteUser, viewId, false);
		}
		if(oversightSurvey != null) {
			a.isValidOversightSurvey(sd, remoteUser, sId, oversightSurvey);
		}		
		// End Authorisation

		String language = "none";

		Connection cResults = ResultsDataSource.getConnection(connectionString);

		String sqlGetMainForm = "select f_id, table_name from form where s_id = ? and parentform = 0;";
		PreparedStatement pstmtGetMainForm = null;

		String sqlGetForm = "select parentform, table_name from form where s_id = ? and f_id = ?;";
		PreparedStatement pstmtGetForm = null;

		PreparedStatement pstmt = null;

		String table_name = null;
		int parentform = 0;
		boolean getParkey = false;
		ResultSet rs = null;

		if(sort != null && dirn == null) {
			dirn = "asc";
		}
		
		boolean audit=false;
		if(audit_set != null && (audit_set.equals("yes") || audit_set.equals("true"))) {
			audit = true;
		}
		
		boolean isGeoJson=false;
		if(geojson != null && (geojson.equals("yes") || geojson.equals("true"))) {
			isGeoJson = true;
		}
		
		boolean mergeSelectMultiple = false;
		if(merge != null && (merge.equals("yes") || merge.equals("true"))) {
			mergeSelectMultiple = true;
		}

		if(include_bad == null) {
			include_bad = "none";
		}
		
		if(include_completed == null) {
			include_completed = "yes";
		}

		boolean isDt = false;
		if(format != null && format.equals("dt")) {
			isDt = true;
		}
		
		if(tz == null) {
			tz = GeneralUtilityMethods.getOrganisationTZ(sd, 
					GeneralUtilityMethods.getOrganisationId(sd, remoteUser));
		}
		tz = GeneralUtilityMethods.validTimezone(tz);

		PrintWriter outWriter = null;
		ResourceBundle localisation = null;
		SurveySettingsManager ssm = null;
		SurveySettingsDefn ssd = null;
		int uId = 0;
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, remoteUser);
			
			lm.writeLog(sd, sId, remoteUser, LogManager.API_VIEW, "Managed Forms or the API. " + (hrk == null ? "" : "Hrk: " + hrk), 0, request.getServerName());
			
			response.setContentType("application/json; charset=UTF-8");
			response.setCharacterEncoding("UTF-8");
			outWriter = response.getWriter();
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, remoteUser));
			localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			/*
			 * Check rate Limiter and whether or not the api is disabled
			 */
			if(!GeneralUtilityMethods.isApiEnabled(sd, remoteUser)) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}	
			RateLimiter.isPermitted(sd, oId, response, localisation);

			/*
			 * Get the survey view
			 */
			SurveyViewManager svm = new SurveyViewManager(localisation, tz);
			ssm = new SurveySettingsManager(localisation, tz);
			uId = GeneralUtilityMethods.getUserId(sd, remoteUser);
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			
			SurveyViewDefn sv = null;
			ArrayList<TableColumn> columns = null;
			
			/*
			 * If the data is destined for the console then get the columns using the survey view class
			 * For normal API calls this is not used primarily to reduce the chance that anything gets changed due
			 * to console specific coding
			 */
			if(schema) {
				ssd = ssm.getSurveySettings(sd, uId, sIdent);
				if(!getSettings) {
					// Update the settings with the values passed in the request
					ssd.limit = limit;
					ssd.filter = advanced_filter;
					ssd.dateName = dateName;
					ssd.fromDate = startDate;
					ssd.toDate = endDate;
					ssd.include_bad = include_bad;
					ssd.include_completed = include_completed;
					ssd.overridenDefaultLimit = "yes";
					ssd.pageLen = pageLen;
					
					ssm.setSurveySettings(sd, uId, sIdent, ssd);
				} else {
					// If the limit has not previously been set then default it to 1,000
					if(ssd.overridenDefaultLimit == null) {
						ssd.limit = 1000;
					}
					if(ssd.include_bad == null) {
						ssd.include_bad = "none";
					}
				}
				
				// Add the drill down advanced filter - this is not to be saved
				// This drill down filter overrides the parent filter
				if(dd_filter != null && dd_filter.trim().length() > 0) {
					ssd.filter = dd_filter;
				}
				
				// Add the filter for drill down to parent - this too is not to be saved
				if(dd_hrk != null) {
					
					StringBuffer parentFilter = new StringBuffer("");

					KeyManager km = new KeyManager(localisation);
					String hrkExpression = km.get(sd, groupSurveyIdent).key;

					if(hrkExpression != null) {
						parentFilter.append("(${_hrk} = '").append(dd_hrk).append("')");
					} else {
						int pKey = 0;
						try {
							pKey = Integer.valueOf(dd_hrk);
							parentFilter.append("(${prikey} = ").append(pKey).append(")");
						} catch (Exception e) {
							
						}
						
					}
					if(parentFilter.length() > 0) {
						ssd.filter = parentFilter.toString();
					} else {
						ssd.filter = null;
					}
					
				}
				
				sv = svm.getSurveyView(sd, 
						cResults, 
						uId, 
						ssd, 
						sId,
						fId,
						formName,
						remoteUser, 
						oId, 
						superUser,
						oversightSurvey,
						ssd.include_bad.equals("yes") || ssd.include_bad.equals("only"));	
				columns = sv.columns;
				table_name = sv.tableName;			
				
			} else {
			
				ssd = new SurveySettingsDefn();
				ssd.limit = limit;
				ssd.filter = advanced_filter;
				ssd.dateName = dateName;
				ssd.fromDate = startDate;
				ssd.toDate = endDate;
				ssd.include_bad = include_bad;
				ssd.include_completed = include_completed;
				ssd.overridenDefaultLimit = "yes";
				
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
					rs = pstmtGetForm.executeQuery();
					if(rs.next()) {
						parentform = rs.getInt(1);
						table_name = rs.getString(2);
					}
					if(rs != null) try {rs.close(); rs = null;} catch(Exception e) {}
				}
				
				columns = GeneralUtilityMethods.getColumnsInForm(
						sd,
						cResults,
						localisation,
						language,
						sId,
						sIdent,
						remoteUser,
						null,
						parentform,
						fId,
						table_name,
						true,				// Read Only
						getParkey,			// Include parent key if the form is not the top level form (fId is 0)
						(ssd.include_bad.equals("yes") || ssd.include_bad.equals("only")),
						includeMeta,		// include instance id
						includeMeta,		// Include prikey
						includeMeta,		// Include HRK
						includeMeta,		// include other meta data
						includeMeta,		// include preloads
						true,				// include instancename
						includeMeta,		// include survey duration
						includeMeta,		// include case management
						superUser,
						false,				// TODO include HXL
						audit,
						tz,
						mgmt,				// If this is a management request then include the assigned user after prikey
						false,				// Accuracy and Altitude
						true		// Server calculates
						);
	
			}
			
			/*
			 * Get Case Management Settings
			 */
			CaseManager cm = new CaseManager(localisation);				
			CMS cms = cm.getCaseManagementSettings(sd, groupSurveyIdent);
					
			/*
			 * Get SMS Settings
			 */
			SMSManager mgr = new SMSManager(localisation, tz);
			SMSNumber smsNumber = mgr.getDetailsForSurvey(sd, groupSurveyIdent);
			
			// Only set the filter if parkey is not set. Otherwise, if set, it is a drill down and the filter does not apply
			String filter = null;
			if(parkey == 0) {
				filter = ssd.filter;
			}
			
			/*
			 * Get the prepared statement
			 */
			TableDataManager tdm = new TableDataManager(localisation, tz);
			pstmt = tdm.getPreparedStatement(
					sd, 
					cResults,
					columns,
					urlprefix,
					attachmentPrefix,
					sId,
					sIdent,
					fId,
					table_name,
					parkey,
					hrk,
					remoteUser,
					null,	// roles
					sort,
					dirn,
					mgmt,
					group,
					isDt,
					start,
					getParkey,
					start_parkey,
					superUser,
					false,			// Return records greater than or equal to primary key
					ssd.include_bad,
					ssd.include_completed,
					cms,
					null,			// no custom filter
					null,			// key filter
					tz,
					instanceId,			// instanceId
					filter,
					ssd.dateName,
					ssd.fromDate,
					ssd.toDate
					);
			
			ConsoleTotals totals = new ConsoleTotals();

			if(isDt) {
				outWriter.print("{\"data\":");
				errorMsgAddClosingBracket++;
			}
			if(isGeoJson) {
				outWriter.print("{\"type\":\"FeatureCollection\",");		// type
																		// TODO metadata
				outWriter.print("\"features\":");						// Features
				errorMsgAddClosingBracket++;
			}
			
			outWriter.print("[");
			errorMsgAddClosingArray = true;
			
			if(geomQuestion == null) {
				geomQuestion = GeneralUtilityMethods.getFirstGeometryQuestionName(columns);
			}
			
			if(pstmt != null) {
				log.info("DataAPI data: " + pstmt.toString());
				/*
				 * Get the data record by record so it can be streamed
				 */
				
				// page the results to reduce memory usage
				log.info("---------------------- paging results to postgres");
				cResults.setAutoCommit(false);		
				pstmt.setFetchSize(100);	
				
				rs = pstmt.executeQuery();
				JSONObject jo = new JSONObject();
				int index = 0;
				boolean viewOwnDataOnly = GeneralUtilityMethods.isOnlyViewOwnData(sd, remoteUser);
				//boolean viewLinks = GeneralUtilityMethods.hasSecurityGroup(sd, remoteUser, Authorise.LINKS_ID);
				while(jo != null) {
					
					jo =  tdm.getNextRecord(
							sd,
							rs,
							columns,
							urlprefix,
							group,
							isDt,
							limit,
							mergeSelectMultiple,
							isGeoJson,
							geomQuestion,
							incLinks,
							sIdent,
							viewOwnDataOnly,
							false				// View links no longer available
							);
					
					if(jo != null) {
						if(index > 0) {
							outWriter.print(",");
						}
						outWriter.print(jo.toString());
					}
					
					index++;
					if (ssd.limit > 0 && index >= ssd.limit) {
						totals.reached_limit = true;
						break;
					}

				}
				
				cResults.setAutoCommit(true);		// page the results to reduce memory
				
			} else {
				log.info("Error:  prepared statement is null");
			}
			
			outWriter.print("]");
			errorMsgAddClosingArray = false;
			
			if(isDt) {
				if(schema) {
					/*
					 * Return the schema with the data 
					 * 1. remove data not needed by the client for performance and security reasons
					 */
					for(TableColumn tc : columns) {
						tc.actions = null;
						tc.calculation = null;
					}
					sv.tableName = null;
					sv.mainColumnNames = null;
					sv.mainColumnsRemoved = null;
					
					// 2. Add the schema to the results
					outWriter.print(",\"schema\":");
					outWriter.print(gson.toJson(sv));		// Add the survey view
					
					// 3. Add the survey settings to the results
					outWriter.print(",\"settings\":");
					/*
					 * Setting values get applied to the schema with the exception of a few parameters
					 * Remove the settings not used by the client
					 */
					ssd.columnSettings = null;
					ssd.layers = null;
					outWriter.print(gson.toJson(ssd));
					
					// 4. Add totals to the results
					outWriter.print(",\"totals\":");
					outWriter.print(gson.toJson(totals));
					
					// 5. Add forms to the results
					outWriter.print(",\"forms\":");
					ArrayList<FormLink> forms = GeneralUtilityMethods.getFormLinks(sd, sId);
					outWriter.print(gson.toJson(forms));
					
					// 5. Add case settings
					outWriter.print(",\"case\":");
					outWriter.print(gson.toJson(cms));
					
					// 6. Add SMS settings
					if(smsNumber != null) {
						outWriter.print(",\"sms\":");
						smsNumber.identifier = null;	// Don't need these
						smsNumber.surveyIdent = null;
						outWriter.print(gson.toJson(smsNumber));
					}
					// 7. Add currently selected instanceid
					if(selectedInstanceId != null) {
						outWriter.print(",\"selectedRow\":");
						outWriter.print(gson.toJson(GeneralUtilityMethods.getLatestInstanceId(cResults, table_name, selectedInstanceId)));
					}
				}
				
				outWriter.print("}");
				errorMsgAddClosingBracket--;
			}
			
			if(isGeoJson) {				// TODO bbox										
				outWriter.print("}");	// close
				errorMsgAddClosingBracket--;
			}

		} catch(ApplicationException ae) {
			response.setContentType("text/plain");
			response.setStatus(429);
			response.getWriter().append(ae.getMessage());
			log.info(ae.getMessage());
		} catch (Exception e) {
			try {cResults.setAutoCommit(true);} catch(Exception ex) {};
			
			String status;
			String msg = e.getMessage();
			if(msg == null) {
				status = "error";
				msg = localisation.getString("c_error");
				log.log(Level.SEVERE, "Exception", e);
			} else if(msg.indexOf("does not exist", 0) > 0 && msg.startsWith("ERROR: relation")) {
				status = "ok";
				log.info(msg);
			} else {
				status = "error";
				log.log(Level.SEVERE, "Exception", e);
			}		
		
			if(msg != null) {
				msg = msg.replace("\"", "\\\"");
				msg = msg.replace('\n', ',');
			}
			outWriter.print("{\"status\": \"" + status + "\"");
			outWriter.print(",\"msg\": \"");
			outWriter.print(msg);
			outWriter.print("\"}");
			
			if(errorMsgAddClosingArray) {
				outWriter.print("]");
			}
			if(errorMsgAddClosingBracket > 0) {
				for(int i = 0; i < errorMsgAddClosingBracket; i++) {
					outWriter.print("}");
				}
			}
			
			/*
			 * Clear stored values for advanced filter as this is a common source of error
			 */
			ssd.filter = null;
			ssm.setSurveySettings(sd, uId, sIdent, ssd);
			
		} finally {

			outWriter.flush(); 
			outWriter.close();
			
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetMainForm != null) {pstmtGetMainForm.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetForm != null) {pstmtGetForm.close();	}} catch (SQLException e) {	}

			ResultsDataSource.closeConnection(connectionString, cResults);			
			SDDataSource.closeConnection(connectionString, sd);
		}

	}
	
	/*
	 * Get similar records for an individual survey in JSON format
	 */
	public Response getSimilarDataRecords(HttpServletRequest request,
			String select,
			String format,
			int sId,
			int fId,
			boolean mgmt,
			int start,
			int limit,
			String urlPrefix,
			String attachmentPrefix) {
		Response response = null;

		ArrayList<String> authorisationsSuper = new ArrayList<String> ();	
		authorisationsSuper.add(Authorise.ANALYST);
		authorisationsSuper.add(Authorise.VIEW_DATA);
		authorisationsSuper.add(Authorise.VIEW_OWN_DATA);
		authorisationsSuper.add(Authorise.ADMIN);
		Authorise aSuper = new Authorise(authorisationsSuper, null);
		
		String connectionString = "Get similar data records";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aSuper.isAuthorised(sd, request.getRemoteUser());
		aSuper.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		String language = "none";
		Connection cResults = ResultsDataSource.getConnection(connectionString);

		String sqlGetMainForm = "select f_id, table_name from form where s_id = ? and parentform = 0;";
		PreparedStatement pstmtGetMainForm = null;

		String sqlGetForm = "select parentform, table_name from form where s_id = ? and f_id = ?;";
		PreparedStatement pstmtGetForm = null;

		String sqlGetManagedId = "select managed_id from survey where s_id = ?";
		PreparedStatement pstmtGetManagedId = null;

		PreparedStatement pstmtGetSimilar = null;
		PreparedStatement pstmtGetData = null;

		String tz = "UTC";

		StringBuffer columnSelect = new StringBuffer();
		StringBuffer groupSelect = new StringBuffer();
		StringBuffer similarWhere = new StringBuffer();
		ArrayList<String> groupTypes = new ArrayList<String> ();
		int groupColumns = 0;
		String table_name = null;
		int parentform = 0;
		int managedId = 0;
		ResultSet rs = null;
		JSONArray ja = new JSONArray();

		boolean isDt = false;
		if(format != null && format.equals("dt")) {
			isDt = true;
		}

		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			if(!GeneralUtilityMethods.isApiEnabled(sd, request.getRemoteUser())) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}

			// Get the managed Id
			if(mgmt) {
				pstmtGetManagedId = sd.prepareStatement(sqlGetManagedId);
				pstmtGetManagedId.setInt(1, sId);
				rs = pstmtGetManagedId.executeQuery();
				if(rs.next()) {
					managedId = rs.getInt(1);
				}
				rs.close();
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
				rs.close();
			} else {
				pstmtGetForm = sd.prepareStatement(sqlGetForm);
				pstmtGetForm.setInt(1,sId);
				pstmtGetForm.setInt(2,fId);

				log.info("Getting specific form: " + pstmtGetForm.toString() );
				rs = pstmtGetForm.executeQuery();
				if(rs.next()) {
					parentform = rs.getInt(1);
					table_name = rs.getString(2);
				}
				rs.close();
			}

			String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			ArrayList<TableColumn> columns = GeneralUtilityMethods.getColumnsInForm(
					sd,
					cResults,
					localisation,
					language,
					sId,
					surveyIdent,
					request.getRemoteUser(),
					null,
					parentform,
					fId,
					table_name,
					true,		// Read Only
					false,		// Don't include parent key
					false,		// Don't include "bad" columns
					true,		// include instance id
					true,		// Include prikey
					true,		// Include HRK
					true,		// Include other meta data
					true,		// Include preloads
					true,		// Include instance name
					false,		// Include survey duration
					true,		// Include case management
					superUser,
					false,		// Only include HXL with CSV and Excel output
					false,
					tz,
					false,		// mgmt
					false,		// Accuracy and Altitude
					true		// Server calculates
					);

			if(mgmt) {
				CustomReportsManager crm = new CustomReportsManager ();
				ReportConfig config = crm.get(sd, managedId, -1);
				columns.addAll(config.columns);
			}

			ArrayList<SqlParam> params = new ArrayList<> ();
			
			if(GeneralUtilityMethods.tableExists(cResults, table_name)) {

				/*
				 * 1. Prepare the data query minus the where clause that is created to select similar rows
				 */
				for(int i = 0; i < columns.size(); i++) {
					TableColumn c = columns.get(i);
					if(i > 0) {
						columnSelect.append(",");
					}
					columnSelect.append(c.getSqlSelect(attachmentPrefix, tz, params));
				}


				String sqlGetData = "select " + columnSelect.toString() + " from " + table_name
						+ " where prikey >= ? "
						+ "and _bad = 'false' ";
				String sqlSelect = "";

				String sqlGetDataOrder = null;

				// Set default sort order
				if(mgmt) {
					sqlGetDataOrder = " order by prikey desc limit 10000";
				} else {
					sqlGetDataOrder = " order by prikey asc;";
				}

				/*
				 * 1. Find the groups of similar records
				 */
				columnSelect = new StringBuffer();
				String [] selectPairs = select.split(",");
				for(int i = 0; i < selectPairs.length; i++) {
					String [] aSelect = selectPairs[i].split("::");
					if(aSelect.length > 1) {
						for(int j = 0; j < columns.size(); j++) {
							if(columns.get(j).column_name.equals(aSelect[0])) {
								TableColumn c = columns.get(j);
								boolean stringFnApplies = false;

								if(c.type.equals("string") || c.type.equals("select1")
										|| c.type.equals("barcode")) {
									stringFnApplies = true;
								}

								if( groupColumns > 0) {
									columnSelect.append(",");
									groupSelect.append(",");
								}
								similarWhere.append(" and ");

								if(stringFnApplies 
										&& (aSelect[1].equals("lower") 
												|| aSelect[1].equals("soundex"))) {
									String s = aSelect[1] +"(" + c.getSqlSelect(attachmentPrefix, tz, params) + ")";
									columnSelect.append(s);
									groupSelect.append(c.column_name);
									similarWhere.append(s + " = ?");
								} else {
									String s = c.getSqlSelect(attachmentPrefix, tz, params);
									columnSelect.append(s);
									groupSelect.append(c.column_name);
									similarWhere.append(s + " = ?");
								}
								groupColumns++;
								groupTypes.add(c.type);
								break;
							}
						}
					}
				}

				if(columnSelect.length() == 0) {
					throw new Exception("No Matching Columns");
				}

				String sqlGetSimilar = "select count(*), " + columnSelect.toString()
				+ " from " + table_name
				+ " where prikey >= ? "
				+ "and _bad = 'false'";
				String sqlGroup = " group by " + groupSelect.toString();
				String sqlHaving = " having count(*) > 1 ";

				pstmtGetSimilar = cResults.prepareStatement(sqlGetSimilar + sqlGroup + sqlHaving);
				
				// Set parameters
				int paramCount = 1;			
				pstmtGetSimilar.setInt(paramCount++, start);
				
				log.info("Get similar: " + pstmtGetSimilar.toString());
				rs = pstmtGetSimilar.executeQuery();				

				/*
				 * For each grouping of similar records get the individual records
				 */
				while(rs.next()) {

					/*
					 * 3. Get the data that make up these similar records
					 */
					String groupKey = "";
					pstmtGetData = cResults.prepareStatement(sqlGetData + sqlSelect + similarWhere.toString() + sqlGetDataOrder);
					
					paramCount = 1;
					paramCount = GeneralUtilityMethods.addSqlParams(pstmtGetSimilar, paramCount, params);	// Parameters in select clause
					
					pstmtGetData.setInt(paramCount++, start);
					for(int i = 0; i < groupColumns; i++) {
						String gType = groupTypes.get(i);
						log.info("Adding group type: " + gType);
						if(gType.equals("int")) {
							pstmtGetData.setInt(paramCount++, rs.getInt(i + 2));	
						} else { 
							pstmtGetData.setString(paramCount++, rs.getString(i + 2));
						}
						if(i > 0) {
							groupKey += "::";
						}
						groupKey += rs.getString(i + 2);
					}
					log.info("Get data: " + pstmtGetData.toString());
					ResultSet rsD = pstmtGetData.executeQuery();

					int index = 0;
					while (rsD.next()) {

						if(limit > 0 && index >= limit) {
							break;
						}
						index++;

						JSONObject jr = new JSONObject();
						jr.put("_group", groupKey);
						for(int i = 0; i < columns.size(); i++) {	

							TableColumn c = columns.get(i);
							String name = null;
							String value = null;

							if(GeneralUtilityMethods.isGeometry(c.type)) {							
								// Add Geometry (assume one geometry type per table)
								String geomValue = rsD.getString(i + 1);	
								
								name = "_geolocation";
								JSONArray coords = null;
								if(geomValue != null) {
									JSONObject jg = new JSONObject(geomValue);									
									coords = jg.getJSONArray("coordinates");
								} else {
									coords = new JSONArray();
								}
								jr.put(name, coords);

							} else {

								name = c.column_name;
								value = rsD.getString(i + 1);	

								if(value == null) {
									value = "";
								} else if(c.type.equals("dateTime")) {
									value = value.replaceAll("\\.[0-9]{3}", "");
								}

								if(name != null ) {
									if(!isDt) {
										name = GeneralUtilityMethods.translateToKobo(name);
									}
									jr.put(name, value);
								}
							}

						}

						ja.put(jr);
					}
					rsD.close();
				}

				rs.close();
			}

			if(isDt) {
				JSONObject dt  = new JSONObject();
				dt.put("data", ja);
				response = Response.ok(dt.toString()).build();
			} else {
				response = Response.ok(ja.toString()).build();
			}


		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {

			try {if (pstmtGetMainForm != null) {pstmtGetMainForm.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetForm != null) {pstmtGetForm.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetData != null) {pstmtGetData.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetSimilar != null) {pstmtGetSimilar.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetManagedId != null) {pstmtGetManagedId.close();	}} catch (SQLException e) {	}

			ResultsDataSource.closeConnection(connectionString, cResults);			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;

	}
	
	/*
	 * KoboToolBox API version 1 /data
	 * Get a single record in JSON format
	 */
	public Response getSingleRecord(
			Connection sd,
			Connection cResults,
			HttpServletRequest request,
			String sIdent,
			int sId,
			String uuid,
			String merge, 			// If set to yes then do not put choices from select multiple questions in separate objects
			ResourceBundle localisation,
			String tz,				// Timezone
			boolean includeMeta,
			String urlprefix,
			String attachmentPrefix
			) throws ApplicationException, Exception { 

		Response response;
			
			lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.API_SINGLE_VIEW, "Managed Forms or the API. ", 0, request.getServerName());
			
			
			if(!GeneralUtilityMethods.isApiEnabled(sd, request.getRemoteUser())) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}

			SurveyManager sm = new SurveyManager(localisation, tz);
			
			Survey s = sm.getById(
					sd, 
					cResults, 
					request.getRemoteUser(),
					false,
					sId, 
					true, 		// full
					null, 		// basepath
					null, 		// instance id
					false, 		// get results
					false, 		// generate dummy values
					true, 		// get property questions
					false, 		// get soft deleted
					true, 		// get HRK
					"external", 	// get external options
					false, 		// get change history
					false, 		// get roles
					true,		// superuser 
					null, 		// geomformat
					false, 		// reference surveys
					false,		// only get launched
					false		// Don't merge set value into default values
					);
			
			ArrayList<Instance> instances = sm.getInstances(
					sd,
					cResults,
					s,
					s.getFirstForm(),
					0,
					null,
					uuid,
					sm,
					includeMeta,
					urlprefix,
					attachmentPrefix
					);
		
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			// Just return the first instance - at the top level there should only ever be one
			if(instances.size() > 0) {
				response = Response.ok(gson.toJson(instances.get(0))).build();
			} else {
				log.log(Level.SEVERE, "Instance not found for " + s.surveyData.displayName + " : " + uuid);
				response = Response.serverError().status(Status.NOT_FOUND).build();
			}

		return response;

	}
}
