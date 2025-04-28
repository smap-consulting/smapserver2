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
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.Response.Status;

import model.Filter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.RateLimiter;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.Tables;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.managers.SubmissionsManager;
import org.smap.sdal.managers.UserLocationManager;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SqlParam;
import org.smap.sdal.model.TableColumn;

import utilities.QuestionInfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/items")
public class Items extends Application {
	
	Authorise a = null;
	Authorise aUpdate = null;
	
	boolean forDevice = false;	// Attachment URL prefixes should be in the client format
	
	private static Logger log =
			 Logger.getLogger(Items.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public Items() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		aUpdate = new Authorise(authorisations, null);
	}
	
	/*
	 * JSON
	 * Gets data for the supplied form id
	 * Usage /surveyKPI/items/{formId}?geom=yes|no&feats=yes|no&mustHaveGeom=yes|no
	 *   geom=yes  then location information will be returned
	 *   feats=yes then features associated with geometries will be returned
	 *   mustHaveGeom=yes then only items that have a location will be returned
	 *   The default is:
	 *   	to return all records unless rec_limit is set > 0
	 *   	Not return "bad" records unless get_bad is set to true
	 */
	@GET
	@Path("/{form}")
	@Produces("application/json")
	public String getTable(@Context HttpServletRequest request,
			@Context HttpServletResponse response,
			@PathParam("form") int fId, 
			@QueryParam("geom") String geom,			
			@QueryParam("mustHaveGeom") String mustHaveGeom,
			@QueryParam("start_key") int start_key,
			@QueryParam("get_bad") boolean bBad,		// Get bad records
			@QueryParam("rec_limit") int rec_limit,
			@QueryParam("dateId") int dateId,		// Id of question containing the date to filter by
			@QueryParam("startDate") Date startDate,
			@QueryParam("endDate") Date endDate,
			@QueryParam("filter") String sFilter,
			@QueryParam("advanced_filter") String advanced_filter,
			@QueryParam("inc_ro") boolean inc_ro,
			@QueryParam("geom_questions") String geomQuestions,
			@QueryParam("tz") String tz) { 
		
		JSONObject jo = new JSONObject();
		boolean bGeom = true;
		boolean bMustHaveGeom = true;
		int maxRec = 0;
		int recCount = 0;
		String language = "none";
		ArrayList<SqlFrag> columnSqlFrags = new ArrayList<SqlFrag>();
		ArrayList<String> colNames = new ArrayList<String> ();
		HashMap<String, String> surveyNames = new HashMap<String, String> ();
		String connectionString = "surveyKPI-Items";
		DecimalFormat decimalFormat = new DecimalFormat("0.00");
		
		String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);

		if(geom != null && geom.equals("no")) {
			bGeom = false;
		}

		if(mustHaveGeom != null && mustHaveGeom.equals("no")) {
			bMustHaveGeom = false;
		}
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		int sId = 0;
		boolean superUser = false;
		try {
			sId = GeneralUtilityMethods.getSurveyIdForm(sd, fId);
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
			
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
			
		tz = (tz == null) ? "UTC" : tz;
		
		HashMap<String, String> geomQuestionMap = null;
		if(geomQuestions != null) {
			String [] a = geomQuestions.split(",");
			if(a.length > 0) {
				geomQuestionMap = new HashMap<> ();
				for(int i = 0; i < a.length; i++) {
					geomQuestionMap.put(a[i].trim(), "yes");
				}
			}
		}
		
		Tables tables = new Tables(sId);
		boolean hasRbacRowFilter = false;
		StringBuffer message = new StringBuffer("");
		
		if(fId > 0) {
			
			Connection cResults = null;
			PreparedStatement pstmt = null;
			PreparedStatement pstmtFDetails = null;
			
			int parent = 0;
			String tName = null;
			String formName = null;
			int totalCount = 0;
			 
			try {
				
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				
				/*
				 * Check the rate limiter
				 */
				RateLimiter.isPermitted(sd, oId, response, localisation);				
				
				// Connect to the results database
				cResults = ResultsDataSource.getConnection(connectionString);	
				
				// Prepare the statement to get the form details
				String sqlFDetails = "select table_name, parentform, name from form " +
					" where f_id = ? "
					+ "and s_id = ?";	// Authorisation check
				pstmtFDetails = sd.prepareStatement(sqlFDetails);
				
				// Get the table details
				// Get the question type
				pstmtFDetails.setInt(1, fId);
				pstmtFDetails.setInt(2, sId);
				ResultSet rsDetails = pstmtFDetails.executeQuery();
				if(rsDetails.next()) {
					
					tName = rsDetails.getString(1);
					parent = rsDetails.getInt(2);
					formName = rsDetails.getString(3);
					
					tables.add(tName, fId, parent);
				} else { 
					throw new Exception("Form " + fId + " not found");
				}
				
				GeneralUtilityMethods.ensureTableCurrent(cResults, tName, parent == 0);
				
				int geomIdx = -1;
				
				JSONObject jTotals = new JSONObject();
				jo.put("totals", jTotals);
				jTotals.put("start_key", start_key);
				
				// Get the number of records
				String sql = "SELECT count(*) FROM " + tName;
				log.info("Get the number of records: " + sql);	
				pstmt = cResults.prepareStatement(sql);	 			
				ResultSet resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					totalCount = resultSet.getInt(1);
					jTotals.put("total_count", totalCount);
				}
				
				// Get the number of bad records
				sql = "SELECT count(*) FROM " + tName + " where _bad = 'true'";
				log.info("Get the number of bad records: " + sql);
				if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
				pstmt = cResults.prepareStatement(sql);	 			
				resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					jTotals.put("bad_count", resultSet.getInt(1));
				}
				
				String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				ArrayList<SqlParam> params = new ArrayList<SqlParam>();
				ArrayList<TableColumn> columnList = GeneralUtilityMethods.getColumnsInForm(
						sd,
						cResults,
						localisation,
						language,
						sId,
						sIdent,
						request.getRemoteUser(),
						null,	// Roles to apply
						parent,
						fId,
						tName,
						inc_ro,	// Read only
						true,	// Include parent key
						true,	// Include "bad"
						true,	// Include instanceId
						true,	// Include prikey
						false,	// Include other meta data
						true,	// Include preloads
						true,	// Include instance names
						false,	// Include survey duration
						true,	// Include case management
						superUser,
						false,		// HXL only include with XLS exports
						false,		// Don't include audit data
						tz,
						false,		// mgmt
						false,		// Accuracy and Altitude
						true		// Server calculates
						);		
				
				// Construct a new query that retrieves a geometry object as geoJson
				StringBuffer cols = new StringBuffer("");
				int newColIdx = 0;
				JSONArray columns = new JSONArray();
				JSONArray types = new JSONArray();
				boolean hasGeometry = false;
				String geomColumn = null;
				
				for(TableColumn c : columnList) {
					if(newColIdx > 0) {
						cols.append(",");
					}
					if(bGeom && GeneralUtilityMethods.isGeometry(c.type)) {
						
						cols.append("ST_AsText(" + tName + "." + c.column_name + ") " + " as " +  c.column_name);
						if(geomQuestionMap != null && geomQuestionMap.size() > 0) {
							if(geomQuestionMap.get(c.question_name) != null) {		// requested this one
								geomColumn = c.column_name;
								hasGeometry = true;
							}						
						}
					
					} else if(GeneralUtilityMethods.isAttachmentType(c.type)) {
							cols.append("'" + attachmentPrefix + "' || " + tName + "." + c.column_name + " as " + c.column_name);
				
					} else if(c.column_name.equals("prikey") || c.column_name.equals("parkey") 
							|| c.column_name.equals("_bad") || c.column_name.equals("_bad_reason")) {
						cols.append(tName + "." + c.column_name);
					
					} else if(c.type != null && c.type.equals("dateTime")) {
						cols.append("timezone(?, ").append(tName).append(".").append(c.column_name).append(") as " +  c.column_name);
						params.add(new SqlParam("string", tz));
						
					}  else if(c.type != null && c.type.equals("date")) {
						cols.append(tName).append(".").append(c.column_name);
						
					} else if(c.type != null && c.type.equals("server_calculate")) {
						if (c.calculation != null) {
							cols.append(c.calculation.sql.toString()).append(" as ").append(c.column_name);
						
							// record any parameters for server side calculations
							if (c.calculation != null && c.calculation.params != null) {
								columnSqlFrags.add(c.calculation);
							}
						} else {
							cols.append("'' as ").append(c.column_name);	// No value
						}
						
					} else {
						cols.append(tName + "." + c.column_name);
						
					}
					
					colNames.add(c.column_name);
					if(c.column_name.equals("prikey")) {
						columns.put(c.column_name);		// For backward compatability (temporary)
					} else {
						columns.put(c.displayName);
					}
					types.put(c.type);
					newColIdx++;
				}
				if(hasGeometry) {	// Put the geojson select of the geometry at the end
					geomIdx = newColIdx;
					cols.append(",ST_AsGeoJSON(" + tName + "." + geomColumn + ") " + " as geomvalue");
				}
				
				boolean hasImportSourceColumn = GeneralUtilityMethods.hasColumn(cResults, tName, "_import_source");
				if(hasImportSourceColumn) {
					// add a hidden import source column
					if(newColIdx > 0) {
						cols.append(",");
					}
					cols.append(tName + "._import_source");
				}
				
				
				String stepFilter = "";
				String sqlFilter = "";
				if(start_key > 0) {
					stepFilter += tName + ".prikey < " +  start_key;			
				} 
				
				// Add filter for deleted records
				if(!bBad) {
					sqlFilter += tName + "._bad = 'false'";
				}
				
				 /*
				  * Get the where clause passed by the client
				  *  This may reference columns in a different table
				  */
				Filter filter = null;
				if(sFilter != null) {
					Type type = new TypeToken<Filter>(){}.getType();
					Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
					filter = gson.fromJson(sFilter, type);
					
					if(filter.value != null) {
						filter.value = filter.value.replace("'", "''");	// Escape apostrophes
					}
					
					QuestionInfo fQ = new QuestionInfo(sId, filter.qId, sd);	
					tables.add(fQ.getTableName(), fQ.getFId(), fQ.getParentFId());
					log.info("Filter expression: " + fQ.getFilterExpression(filter.value, null));
					
					if(sqlFilter.length() > 0) {
						sqlFilter += " and " + fQ.getFilterExpression(filter.value, null);
					} else {
						sqlFilter = fQ.getFilterExpression(filter.value, null);
					}
				}
				
				/*
				 * Validate the advanced filter and convert to an SQL Fragment
				 */
				SqlFrag advancedFilterFrag = null;
				if(advanced_filter != null && advanced_filter.length() > 0) {

					advancedFilterFrag = new SqlFrag();
					advancedFilterFrag.addSqlFragment(advanced_filter, false, localisation, 0);

					for(String filterCol : advancedFilterFrag.humanNames) {
						if(GeneralUtilityMethods.getColumnName(sd, sId, filterCol) == null) {
							String msg = localisation.getString("inv_qn_misc");
							msg = msg.replace("%s1", filterCol);
							throw new ApplicationException(msg);
						}
					}
				}
				// Add the advanced filter fragment
				if(advancedFilterFrag != null) {
					if(sqlFilter.length() > 0) {
						sqlFilter += " and " + "(" + advancedFilterFrag.sql + ")";
					} else {
						sqlFilter = "(" + advancedFilterFrag.sql + ")";
					}	
					
					for(int i = 0; i < advancedFilterFrag.columns.size(); i++) {
						int rqId = GeneralUtilityMethods.getQuestionIdFromName(sd, sId, advancedFilterFrag.humanNames.get(i));
						if(rqId > 0) {
							QuestionInfo qaf = new QuestionInfo(sId, rqId, sd);
							tables.add(qaf.getTableName(), qaf.getFId(), qaf.getParentFId());
						} else {
							// assume meta and hence include main table
							Form tlf = GeneralUtilityMethods.getTopLevelForm(sd, sId);
							tables.add(tlf.tableName, tlf.id, tlf.parentform);
						}
					}
				}
				
				/*
				 * Add row filtering performed by RBAC
				 */
				RoleManager rm = new RoleManager(localisation);
				ArrayList<SqlFrag> rfArray = null;
				if(!superUser) {
					rfArray = rm.getSurveyRowFilter(sd, sIdent, request.getRemoteUser());
					String rfString = "";
					if(rfArray.size() > 0) {
						for(SqlFrag rf : rfArray) {
							if(rf.columns.size() > 0) {
								for(int i = 0; i < rf.columns.size(); i++) {
									int rqId = GeneralUtilityMethods.getQuestionIdFromName(sd, sId, rf.humanNames.get(i));
									if(rqId > 0) {
										QuestionInfo fRbac = new QuestionInfo(sId, rqId, sd);
										tables.add(fRbac.getTableName(), fRbac.getFId(), fRbac.getParentFId());
									} else {
										// Assume meta and get top level table
										Form tlf = GeneralUtilityMethods.getTopLevelForm(sd, sId);
										tables.add(tlf.tableName, tlf.id, tlf.parentform);
									}
								}
								if(rfString.length() > 0) {
									rfString += " or";
								}
								rfString += " (" + rf.sql.toString() + ")";
								hasRbacRowFilter = true;
							}
						}
						if(rfString.trim().length() > 0) {
							if(sqlFilter.length() > 0) {
								sqlFilter += " and " + "(" + rfString + ")";
							} else {
								sqlFilter = "(" + rfString + ")";
							}
						}
					}
				}
				
				/*
				 * Get the date question used to set start and end date
				 */
				// Get date column information
				QuestionInfo date = null;
				if((dateId != 0) && (startDate != null || endDate != null)) {
					date = new QuestionInfo(localisation, tz, sId, dateId, sd, cResults, request.getRemoteUser(), false, "", attachmentPrefix, oId);	// Not interested in label any language will do
					tables.add(date.getTableName(), date.getFId(), date.getParentFId());
					log.info("Date name: " + date.getColumnName() + " Date Table: " + date.getTableName());
				}
				
				// Add any tables required to complete the join
				tables.addIntermediateTables(sd);
				
				jTotals.put("rec_limit", rec_limit);
				String sqlLimit = "";
				if(rec_limit > 0) {
					sqlLimit = "limit " + rec_limit;
				}
				StringBuffer sql2 = new StringBuffer("select ");		// Add distinct as filter by values in a subform would otherwise result in duplicate tables
				StringBuffer sqlFC = new StringBuffer("select count(*) ");	
				sql2.append(cols);
				sql2.append(" from ");
				sqlFC.append(" from ");
				sql2.append(tables.getTablesSQL());
				sqlFC.append(tables.getTablesSQL());
				
				String sqlTableJoin = tables.getTableJoinSQL();
				boolean doneWhere = false;
				
				if(date != null) {
					String sqlRestrictToDateRange = GeneralUtilityMethods.getDateRange(startDate, endDate, date.getColumnName());
					if(sqlRestrictToDateRange.trim().length() > 0) {
						if(sqlFilter.trim().length() > 0) {
							sqlFilter += " and ";
						} 
						sqlFilter += sqlRestrictToDateRange;
					}
				}
				String whereClause = "";
				String countWhereClause = "";
				if(sqlTableJoin.trim().length() > 0) {
					whereClause += " where " + sqlTableJoin;
					countWhereClause = " where " + sqlTableJoin;
					doneWhere = true;
				} 
				if(sqlFilter.trim().length() > 0) {
					if(doneWhere) {
						whereClause += " and ";
						countWhereClause += " and ";
					} else {
						whereClause += " where ";
						countWhereClause += " where ";
						doneWhere = true;
					}
					whereClause += sqlFilter;
					countWhereClause += sqlFilter;
				}
				
				// Only apply the step filter to the sql that retrieves the data and not the sql that retrieves the count of data
				if(stepFilter.trim().length() > 0) {
					if(doneWhere) {
						whereClause += " and ";
					} else {
						whereClause += " where ";
						doneWhere = true;
					}
					whereClause += stepFilter;						}
				
				
				sql2.append(whereClause);
				sqlFC.append(countWhereClause);
				
				// Add order by and limit
				sql2.append(" order by ");
				if(parent > 0) {
					sql2.append(tName).append(".parkey desc, ");
				}
				sql2.append(tName).append(".prikey desc ").append(sqlLimit);
				
				// Get the number of filtered records			
				if(sqlFC.length() > 0) {
					sql = sqlFC.toString();
					
					if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
					pstmt = cResults.prepareStatement(sql);
					
					int attribIdx = 1;	
					
					if(advancedFilterFrag != null) {
						attribIdx = GeneralUtilityMethods.setFragParams(pstmt, advancedFilterFrag, attribIdx, tz);
					}		
					// RBAC row filter
					if(hasRbacRowFilter) {
						attribIdx = GeneralUtilityMethods.setArrayFragParams(pstmt, rfArray, attribIdx, tz);
					}				
					// dates
					if(dateId != 0) {
						if(startDate != null) {
							pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.startOfDay(startDate, tz));
						}
						if(endDate != null) {
							pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.endOfDay(endDate, tz));
						}
					}
					log.info("Get the number of filtered records: " + pstmt.toString());
					resultSet = pstmt.executeQuery();
					if(resultSet.next()) {
						jTotals.put("filtered_count", resultSet.getInt(1));
					} else {
						jTotals.put("filtered_count", 0);
					}
				} else {
					jTotals.put("filtered_count", totalCount);
				}

				
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = cResults.prepareStatement(sql2.toString());
				
				/*
				 * Set prepared statement values
				 */
				int attribIdx = 1;
				
				// Add any parameters in the select
				attribIdx = GeneralUtilityMethods.addSqlParams(pstmt, attribIdx, params);
				
				if (columnSqlFrags.size() > 0) {
					attribIdx = GeneralUtilityMethods.setArrayFragParams(pstmt, columnSqlFrags, attribIdx, tz);
				}
				
				if(advancedFilterFrag != null) {
					attribIdx = GeneralUtilityMethods.setFragParams(pstmt, advancedFilterFrag, attribIdx, tz);
				}
				
				// RBAC row filter
				if(hasRbacRowFilter) {
					attribIdx = GeneralUtilityMethods.setArrayFragParams(pstmt, rfArray, attribIdx, tz);
				}
				
				// dates
				if(dateId != 0) {
					if(startDate != null) {
						pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.startOfDay(startDate, tz));
					}
					if(endDate != null) {
						pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.endOfDay(endDate, tz));
					}
				}
				
				// Request the data
				log.info("Get Item Data: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
				
				JSONArray ja = new JSONArray();
				while (resultSet.next()) {
					JSONObject jr = new JSONObject();
					JSONObject jp = new JSONObject();
					JSONObject jg = null;
					
					jr.put("type", "Feature");

					if(geomIdx > 0) {
						// Add Geometry
						String geomValue = resultSet.getString(geomIdx + 1);	
						if(geomValue != null) {	
							jg = new JSONObject(geomValue);
						}
					}
					for(int i = 0; i < colNames.size(); i++) {	
	
						String name = colNames.get(i);
						String headerName = columns.getString(i);
						String value = resultSet.getString(i + 1);	
						if(value == null) {
							value = "";
						}
						
						/*
						 * Truncate number of decimal places
						 */
						if(types.get(i).equals("decimal") && value.trim().length() > 0) {
							try {
								double dv = Double.parseDouble(value);
								value = decimalFormat.format(dv);
							} catch (Exception e) {
								log.log(Level.SEVERE, e.getMessage(), e);
							}
						}

						if(name.equals("prikey")) {
							JSONArray prikeys = new JSONArray();
							jp.put("prikeys", prikeys);
							prikeys.put(resultSet.getString("prikey"));
							maxRec = resultSet.getInt("prikey");
						} else if(name.equals(SmapServerMeta.SURVEY_ID_NAME)) {
							// Get the display name
							String displayName = "";
							if(value.length() > 0 && !value.equals("0")) {
								displayName = surveyNames.get(value);
								if(displayName == null) {
									displayName = GeneralUtilityMethods.getSurveyName(sd, Integer.parseInt(value));
									surveyNames.put(value, displayName);
								}
							} else {
								if(hasImportSourceColumn) {
									displayName = resultSet.getString("_import_source");
									if(displayName == null) {
										displayName = "";
									}
								} 
							}
							jp.put(headerName, displayName);
						} else {
							jp.put(headerName, value);
						}
					} 

					if(!bMustHaveGeom || jg != null) {
						jr.put("geometry", jg);
						jr.put("properties", jp);
						ja.put(jr);
					}
					
					recCount++;
				 }
				
				String maxRecordWhere = "";
				if(whereClause.equals("")) {
					maxRecordWhere = " where " + tName + ".prikey < " + maxRec;
				} else {
					maxRecordWhere = whereClause + " and " + tName + ".prikey < " + maxRec;
				}
				// Determine if there are more records to be returned
				sql = "SELECT count(*) FROM " + tables.getTablesSQL() + maxRecordWhere + ";";
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = cResults.prepareStatement(sql);	
				
				// Apply the parameters again
				attribIdx = 1;
				
				if(advancedFilterFrag != null) {
					attribIdx = GeneralUtilityMethods.setFragParams(pstmt, advancedFilterFrag, attribIdx, tz);
				}
				
				// RBAC row filter
				if(hasRbacRowFilter) {
					attribIdx = GeneralUtilityMethods.setArrayFragParams(pstmt, rfArray, attribIdx, tz);
				}
				if(dateId != 0) {
					if(startDate != null) {
						pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.startOfDay(startDate, tz));
					}
					if(endDate != null) {
						pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.endOfDay(endDate, tz));
					}
				}
				
				log.info("Check for more records: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					jTotals.put("more_recs", resultSet.getInt(1));
				}
				 jTotals.put("max_rec", maxRec);
				 jTotals.put("returned_count", recCount);
				 
				 jo.put("type", "FeatureCollection");
				 jo.put("features", ja);
				 jo.put("cols", columns);
				 jo.put("types", types);
				 jo.put("formName", formName);
				
				 /*
				  * Write to log
				  */
				 String msg = localisation.getString("rep_view_form");
				 msg = msg.replace("%s1", GeneralUtilityMethods.getSurveyName(sd, sId));
				 msg = msg.replace("%s2", formName);
				 lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.DASHBOARD_VIEW, msg, 0, request.getServerName());

			} catch (SQLException e) {
			    
				String msg = e.getMessage();
				if(msg.contains("does not exist") && !msg.contains("column") && !msg.contains("operator")) {	// Don't do a stack dump if the table did not exist that just means no one has submitted results yet
					// Don't do a stack dump if the table did not exist that just means no one has submitted results yet
				} else {
					message.append(msg);
					log.log(Level.SEVERE, message.toString(), e);
				}
				
			} catch(ApplicationException ae) {
				
				message.append(ae.getMessage());
				log.info(ae.getMessage());
				
			} catch (Exception e) {
				log.log(Level.SEVERE, message.toString(), e);
				message.append(e.getMessage());
			} finally {
				
				try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
				try {if (pstmtFDetails != null) {pstmtFDetails.close();	}} catch (SQLException e) {	}
				
				SDDataSource.closeConnection(connectionString, sd);
				ResultsDataSource.closeConnection(connectionString, cResults);	
			}
		}

		try {
			jo.put("message", message.toString());
		} catch (Exception e) {
		}
		return jo.toString();
	}
	
	/*
	 * Get the user activity
	 */
	@GET
	@Path("/user/{user}")
	@Produces("application/json")
	public String getUserActivity(@Context HttpServletRequest request,
			@PathParam("user") int uId, 
			@QueryParam("start_key") int start_key,
			@QueryParam("rec_limit") int rec_limit,
			@QueryParam("startDate") Date startDate,
			@QueryParam("dateId") int dateId,
			@QueryParam("endDate") Date endDate,
			@QueryParam("tz") String tz) { 
		
		String connectionString = "surveyKPI-Items-Users";
		JSONObject jo = new JSONObject();
		JSONArray columns = new JSONArray();
		JSONArray types = new JSONArray();
		
		int maxRec = 0;
		int recCount = 0;	
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);		
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidUser(sd, request.getRemoteUser(), uId);
		// End Authorisation
		
		lm.writeLog(sd, 0, request.getRemoteUser(), LogManager.USER_ACTIVITY_VIEW, "User Activity for " + uId, 0, request.getServerName());
	
		tz = (tz == null) ? "UTC" : tz;
		
		StringBuffer message = new StringBuffer("");
		
		if(uId > 0) {
			
			PreparedStatement pstmt = null;
			
			int totalCount = 0;
			 
			try {
				
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				
				/*
				 * View data users can only see usage data on themselves
				 */
				if(GeneralUtilityMethods.isOnlyViewData(sd, request.getRemoteUser())) {
					uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
				}
				
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				String user = GeneralUtilityMethods.getUserIdent(sd, uId);	
				
				JSONObject jTotals = new JSONObject();
				jo.put("totals", jTotals);
				jTotals.put("start_key", start_key);
				
				// Get the number of records
				String sql = "select count(*) from upload_event where user_name = ?";
				
				pstmt = sd.prepareStatement(sql);	
				pstmt.setString(1, user);
				log.info("Get the number of records: " + pstmt.toString());	
				ResultSet resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					totalCount = resultSet.getInt(1);
					jTotals.put("total_count", totalCount);
				}
				jTotals.put("rec_limit", rec_limit);
				

				SubmissionsManager subMgr = new SubmissionsManager(localisation, tz);
				String whereClause = subMgr.getWhereClause(user, oId, dateId, startDate, endDate, 0, null);			
		
				// Get count of available records
				StringBuffer sqlFC = new StringBuffer("select count(*) from upload_event ue ");				
				sqlFC.append(whereClause);			
				
				// Get the number of filtered records			
				if(sqlFC.length() > 0) {
				
					if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
					pstmt = sd.prepareStatement(sqlFC.toString());
					
					int attribIdx = 1;	
					
					// Add user and organisation
					pstmt.setInt(attribIdx++, oId);
					pstmt.setString(attribIdx++, user);

					pstmt.setString(attribIdx++, request.getRemoteUser());		// For RBAC
						
					// dates
					if(dateId > 0 && dateId < 5) {
						if(startDate != null) {
							pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.startOfDay(startDate, tz));
						}
						if(endDate != null) {
							pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.endOfDay(endDate, tz));
						}
					}

					log.info("Get the number of filtered records for user activity: " + pstmt.toString());
					resultSet = pstmt.executeQuery();
					if(resultSet.next()) {
						jTotals.put("filtered_count", resultSet.getInt(1));
					} else {
						jTotals.put("filtered_count", 0);
					}
				} else {
					jTotals.put("filtered_count", totalCount);
				}

				// Get the prepared statement to retrieve data
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = subMgr.getSubmissionsStatement(sd, rec_limit, start_key, 
						whereClause,
						user,
						oId,
						request.getRemoteUser(),
						dateId,
						startDate,
						endDate,
						0,
						null);
				
				// Request the data
				log.info("Get Usage Data: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
	
				JSONArray ja = new JSONArray();
				while (resultSet.next()) {
					
					JSONObject jr = subMgr.getRecord(resultSet, true, true, false, false, null);
					maxRec = resultSet.getInt("ue_id");	
					ja.put(jr);
					recCount++;

				 }
				
				/*
				 * Add columns and types
				 */
				columns.put("prikey");
				columns.put(localisation.getString("a_name"));
				columns.put(localisation.getString("a_device"));
				columns.put(localisation.getString("ar_project"));
				columns.put(localisation.getString("a_ut"));
				columns.put(localisation.getString("a_l"));
				columns.put(localisation.getString("a_sn"));
				columns.put(localisation.getString("a_in"));
				columns.put(localisation.getString("a_st"));
				columns.put(localisation.getString("a_et"));
				columns.put(localisation.getString("a_sched"));
					
				types.put("integer");
				types.put("string");
				types.put("string");
				types.put("string");
				types.put("dateTime");
				types.put("geometry");	
				types.put("string");	
				types.put("string");	
				types.put("dateTime");
				types.put("dateTime");
				types.put("dateTime");
				
				String maxRecordWhere = "";
				if(whereClause.length() == 0) {
					maxRecordWhere = " where ue_id < " + maxRec;
				} else {
					maxRecordWhere = whereClause + " and ue_id < " + maxRec;
				}
				
				// Determine if there are more records to be returned
				sql = "select count(*) from upload_event ue " + maxRecordWhere + ";";
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);	
				
				// Apply the parameters again
				int attribIdx = 1;
				
				// Add user and organisation
				pstmt.setInt(attribIdx++, oId);
				pstmt.setString(attribIdx++, user);
				pstmt.setString(attribIdx++, request.getRemoteUser());		// For RBAC
			
				// dates
				if(dateId > 0 && dateId < 5) {
					if(startDate != null) {
						pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.startOfDay(startDate, tz));
					}
					if(endDate != null) {
						pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.endOfDay(endDate, tz));
					}
				}
				
				log.info("Check for more records: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					jTotals.put("more_recs", resultSet.getInt(1));
				}
				 jTotals.put("max_rec", maxRec);
				 jTotals.put("returned_count", recCount);
				 
				 jo.put("type", "FeatureCollection");
				 jo.put("features", ja);
				 jo.put("cols", columns);
				 jo.put("types", types);
				 jo.put("formName", "Usage");
				
			} catch (SQLException e) {
			    
				String msg = e.getMessage();
				if(msg.contains("does not exist") && !msg.contains("column")) {	// Don't do a stack dump if the table did not exist that just means no one has submitted results yet
					// Don't do a stack dump if the table did not exist that just means no one has submitted results yet
				} else {
					message.append(msg);
					log.log(Level.SEVERE, message.toString(), e);
				}
				
			} catch (Exception e) {
				log.log(Level.SEVERE, message.toString(), e);
				message.append(e.getMessage());
			} finally {
				
				try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
				
				SDDataSource.closeConnection(connectionString, sd);
			}
		}

		try {
			jo.put("message", message);
		} catch (Exception e) {
		}
		return jo.toString();
	}
	
	/*
	 * Get the user locations
	 */
	@GET
	@Path("/user_locations/{p_id}")
	@Produces("application/json")
	public Response getUserLocations(@Context HttpServletRequest request,
			@PathParam("p_id") int pId, 
			@QueryParam("start_key") int start_key,
			@QueryParam("rec_limit") int rec_limit,
			@QueryParam("tz") String tz) { 
		
		Response response = null;
		
		String connectionString = "surveyKPI-Items-User Locations";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);		
		a.isAuthorised(sd, request.getRemoteUser());
		a.projectInUsersOrganisation(sd, request.getRemoteUser(), pId);
		// End Authorisation
		
		lm.writeLog(sd, 0, request.getRemoteUser(), LogManager.USER_LOCATION_VIEW, "User Locations " + pId, 0, request.getServerName());
	
		tz = (tz == null) ? "UTC" : tz;
		
		// Get the users locale
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
	
			UserLocationManager ulm = new UserLocationManager(localisation, tz);
			response = Response.ok(ulm.getUserLocations(sd, 
					pId,
					start_key,
					rec_limit,
					request.getRemoteUser(),
					true
					)).build();
		} catch (Exception e) {
			response = Response.serverError().build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;

	}
	
	/*
	 * Update the bad record status in a form
	 */
	@POST
	@Path("/{form}/bad/{key}")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response toggleBad(@Context HttpServletRequest request,
			@PathParam("form") int fId,
			@PathParam("key") int key,
			@FormParam("value") boolean value,
			@FormParam("sId") int sId,
			@FormParam("reason") String reason
			) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-Items-bad";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		Connection cRel = null; 
		PreparedStatement pstmt = null;
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			
			log.info("New toggle bad");
			cRel = ResultsDataSource.getConnection("surveyKPI-Items");
			
			// Get the table name
			String sql = "select distinct table_name, parentform from form f " +
					" where f.s_id = ?" + 
					" and f.f_id = ?;";
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			pstmt.setInt(2, fId);
			log.info("Get table name: " + pstmt.toString());
			
			ResultSet tableSet = pstmt.executeQuery();
			if(tableSet.next()) {
				String tName = tableSet.getString(1);
				int pId = tableSet.getInt(2);
				boolean isChild = pId > 0;
				UtilityMethodsEmail.markRecord(cRel, sd, localisation, tName, value, 
						reason, key, sId, fId, false, isChild, request.getRemoteUser(), true, tz, true);
				
				String msg = localisation.getString("msg_del_rec_form");
				msg = msg.replace("%s1", String.valueOf(fId));
				msg = msg.replace("%s2", String.valueOf(key));
				msg = msg.replace("%s3", reason == null ? "" : reason);
				lm.writeLog(sd, sId, request.getRemoteUser(), value ? LogManager.DELETE : LogManager.RESTORE, msg, 0, request.getServerName());

			} else {
				throw new Exception("Could not get form id");
			}

			response = Response.ok().build();
				
		} catch (Exception e) {
			String msg = e.getMessage();
			if(msg.equals("Failed to update record")) {
				response = Response.status(Status.OK).entity("Record cannot be modified").build();
			} else {
				response = Response.serverError().build();
				log.log(Level.SEVERE,"Error", e);
			}
		} finally {
			
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection("surveyKPI-Items", cRel);
		}
		
		return response;
	}
	
	/*
	 * Update the bad record status in a survey
	 */
	@POST
	@Path("/{survey}/survey/bad/{instanceId}")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response toggleBadSurvey(@Context HttpServletRequest request,
			@PathParam("survey") int sId,
			@PathParam("instanceId") String instanceId,
			@FormParam("value") boolean value,
			@FormParam("reason") String reason
			) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-Items-bad Survey Level";
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		Connection cRel = null; 
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			
			log.info("New toggle bad");
			cRel = ResultsDataSource.getConnection("surveyKPI-Items");
		
			Form form = GeneralUtilityMethods.getTopLevelForm(sd, sId);
			int key = GeneralUtilityMethods.getPrikey(cRel, form.tableName, instanceId);
			boolean isChild = false;
			UtilityMethodsEmail.markRecord(cRel, sd, localisation, form.tableName, value, 
						reason, key, sId, form.id, false, isChild, 
						request.getRemoteUser(), true, tz, true);
			String msg = localisation.getString("msg_del_rec");
			msg = msg.replace("%s1", String.valueOf(key));
			msg = msg.replace("%s2", reason == null ? "" : reason);
			lm.writeLog(sd, sId, request.getRemoteUser(), value ? LogManager.DELETE : LogManager.RESTORE, msg, 0, request.getServerName());

			response = Response.ok().build();
				
		} catch (Exception e) {
			String msg = e.getMessage();
			if(msg.equals("Failed to update record")) {
				response = Response.status(Status.OK).entity("Record cannot be modified").build();
			} else {
				response = Response.serverError().build();
				log.log(Level.SEVERE,"Error", e);
			}
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection("surveyKPI-Items", cRel);
		}
		
		return response;
	}


}

