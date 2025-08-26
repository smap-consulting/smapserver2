/*****************************************************************************

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

 ******************************************************************************/

package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.constants.SmapExportTypes;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.model.ColDesc;
import org.smap.sdal.model.OptionDesc;
import org.smap.sdal.model.QueryForm;
import org.smap.sdal.model.SqlDesc;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SqlFragParam;
import org.smap.sdal.model.SqlParam;
import org.smap.sdal.model.TableColumn;

/*
 * Create a query to retrieve results data
 */
public class QueryGenerator {
	
	private static Logger log =
			 Logger.getLogger(QueryGenerator.class.getName());

	
	public static SqlDesc gen(
			Connection sd, 
			Connection connectionResults, 
			ResourceBundle localisation,
			int sId,
			String sIdent,
			int fId, 
			String language, 
			String format,
			String urlprefix,
			String attachmentPrefix,
			boolean wantUrl,
			boolean exp_ro,
			boolean excludeParents,
			HashMap<ArrayList<OptionDesc>, String> labelListMap,
			boolean add_record_uuid,
			boolean add_record_suid,
			String hostname,
			ArrayList<String> requiredColumns,
			ArrayList<String> namedQuestions,
			String user,
			Date startDate,
			Date endDate,
			int dateId,
			boolean superUser,
			QueryForm form,
			String filter,
			boolean meta,
			boolean includeKeys,
			String tz,
			String geomQuestion,
			boolean outerJoin,
			boolean get_acc_alt) throws Exception {
		
		SqlDesc sqlDesc = new SqlDesc();
		ArrayList<String> tables = new ArrayList<String> ();

		PreparedStatement pstmtCols = null;
		PreparedStatement pstmtGeom = null;
		PreparedStatement pstmtQType = null;
	
		PreparedStatement pstmtQLabel = null;
		PreparedStatement pstmtListLabels = null;
		
		PreparedStatement pstmtConvert = null;
		try {			
			
			String topLevelTable = form.table;
			
			/*
			 * Prepare the statement to get the list labels
			 */
			String sqlListLabels = "SELECT o.ovalue, t.value " +
					"FROM option o, translation t, question q " +  		
					"WHERE o.label_id = t.text_id " +
					"AND t.s_id =  ? " + 
					"AND q.q_id = ? " +
					"AND q.l_id = o.l_id " +
					"AND t.language = ? " +
					"ORDER BY o.seq;";		
			pstmtListLabels = sd.prepareStatement(sqlListLabels);
			
			/*
			 *  Prepare the statement to get the question type and read only status
			 */
			String sqlQType = "select q.qtype, q.readonly, q.qtext_id, q.q_id, l.name from question q " +
					" left outer join listname l " +
					" on q.l_id = l.l_id " +
					" join form f " +
					" on q.f_id = f.f_id " +
					" where f.f_id = ? " +
					" and q.column_name = ?;";
			pstmtQType = sd.prepareStatement(sqlQType);
			
			/*
			 * Prepare the statement to get the question label
			 */
			String sqlQLabel = "select value from translation " +
					" where s_id = ? " +
					" and text_id = ? " +
					" and language = ? " +
					" and type = 'none'";
			pstmtQLabel = sd.prepareStatement(sqlQLabel);
			
			/*
			 * Create an object describing the SQL query recursively from the target table
			 */
			getSqlDesc(
					localisation,
					sqlDesc, 
					sId,
					0, 
					language,
					format,
					pstmtCols,
					pstmtGeom,
					pstmtQType,
					pstmtQLabel,
					pstmtListLabels,
					urlprefix,
					attachmentPrefix,
					wantUrl,
					exp_ro,
					labelListMap,
					sd, 
					connectionResults,
					requiredColumns,
					namedQuestions,
					user,
					startDate,
					endDate,
					dateId,
					superUser,
					form,
					tables,
					true,
					meta,
					includeKeys,
					tz,
					get_acc_alt,
					geomQuestion
					);
		
			/*
			 * Validate the filter and convert to an SQL Fragment
			 */
			SqlFrag filterFrag = null;
			if(filter != null && filter.length() > 0) {
	
				filterFrag = new SqlFrag();
				filterFrag.addSqlFragment(filter, false, localisation, 0);
	
	
				for(String filterCol : filterFrag.columns) {
					boolean valid = false;
					for(String q : sqlDesc.column_names) {
						if(filterCol.equals(q)) {
							valid = true;
							break;
						}
					}
					if(!valid) {
						/*
						 * Allow filtering on meta data even if the meta items are not included in the export
						 * Otherwise throw an error message
						 */
						if(!filterCol.startsWith("_") || !GeneralUtilityMethods.hasColumn(connectionResults, topLevelTable, filterCol)) {
							
							String msg = localisation.getString("inv_qn_misc");
							msg = msg.replace("%s1", filterCol);
							throw new Exception(msg);
						}
					}
				}
			}
			
			StringBuffer shpSqlBuf = new StringBuffer();
			shpSqlBuf.append("select ");
			if(add_record_uuid) {
				shpSqlBuf.append("uuid_generate_v4() as _record_uuid, ");
			}
			if(add_record_suid) {		// Smap unique id, globally unique (hopefully) but same value each time query is run
				shpSqlBuf.append("'" + hostname + "_" + topLevelTable + "_' || " + topLevelTable + ".prikey as _record_suid, ");
			}
			shpSqlBuf.append(sqlDesc.cols);
			shpSqlBuf.append(" from ");
	
			for(int i = 0; i < tables.size(); i++) {
				
				if(i > 0) {
					if(outerJoin) {
						shpSqlBuf.append(" left outer join ");
					} else {
						shpSqlBuf.append(",");
					}
				}
				shpSqlBuf.append(tables.get(i));
			}

			/*
			 * Add the joins
			 */
			if(outerJoin) {
				if(form.childForms != null && form.childForms.size() > 0) {
					shpSqlBuf.append(" on ");
					shpSqlBuf.append(getJoins(sd, localisation, form.childForms, form, false));
				}
				shpSqlBuf.append(" where ");
			} else {	
				shpSqlBuf.append(" where ");
				if(form.childForms != null && form.childForms.size() > 0) {
					shpSqlBuf.append(getJoins(sd, localisation, form.childForms, form, false));
					shpSqlBuf.append(" and ");
				}
			}
			
			/*
			 * Exclude "bad" records
			 */
			for(int i = 0; i < tables.size(); i++) {
				if(i > 0) {
					shpSqlBuf.append(" and ");
				}
				if(outerJoin && i > 0) {
					shpSqlBuf.append("(");
				}
				shpSqlBuf.append(tables.get(i));
				shpSqlBuf.append("._bad='false'");
				if(outerJoin && i > 0) {
					shpSqlBuf.append(" or ");
					shpSqlBuf.append(tables.get(i));
					shpSqlBuf.append(" is null)");
				}
			}
			
			
			if(format.equals("shape") && sqlDesc.geometry_type != null && geomQuestion != null) {
				shpSqlBuf.append(" and " + sqlDesc.geometry_table + "." + sqlDesc.geometry_column + " is not null");
			}
			
			
			String sqlRestrictToDateRange = null;
			if(dateId != 0) {
				String dateName = GeneralUtilityMethods.getColumnNameFromId(sd, sId, dateId);
				sqlRestrictToDateRange = GeneralUtilityMethods.getDateRange(startDate, endDate, dateName);
				if(sqlRestrictToDateRange.trim().length() > 0) {
					shpSqlBuf.append(" and ");
					shpSqlBuf.append(sqlRestrictToDateRange);
				}
			}
			
			// Add the advanced filter fragment
			if(filterFrag != null) {
				shpSqlBuf.append( " and (");
				shpSqlBuf.append(filterFrag.sql);
				shpSqlBuf.append(") ");
			}
			
			// Add RBAC/Role Row Filter
			boolean hasRbacFilter = false;
			ArrayList<SqlFrag> rfArray = null;
			RoleManager rm = new RoleManager(localisation);
			if(!superUser) {
				rfArray = rm.getSurveyRowFilter(sd, sIdent, user);
				if(rfArray.size() > 0) {
					String rFilter = rm.convertSqlFragsToSql(rfArray);
					if(rFilter.length() > 0) {
						shpSqlBuf.append(" and ");
						shpSqlBuf.append(rFilter);
						hasRbacFilter = true;
					}
				}
			}
			
			/*
			 * Sort by primary key ascending
			 */
			shpSqlBuf.append(" order by ");
			shpSqlBuf.append(form.table);
			shpSqlBuf.append(".prikey asc");
			
			/*
			 * Prepare the sql string
			 * Add the parameters to the prepared statement
			 * Convert back to sql
			 */
			pstmtConvert = connectionResults.prepareStatement(shpSqlBuf.toString());
			int paramCount = 1;
			
			// Add any parameters in the select
			paramCount = GeneralUtilityMethods.addSqlParams(pstmtConvert, paramCount, sqlDesc.params);
			
			// if date filter is set then add it
			if(sqlRestrictToDateRange != null && sqlRestrictToDateRange.trim().length() > 0) {
				if(startDate != null) {
					pstmtConvert.setTimestamp(paramCount++, GeneralUtilityMethods.startOfDay(startDate, tz));
				}
				if(endDate != null) {
					pstmtConvert.setTimestamp(paramCount++, GeneralUtilityMethods.endOfDay(endDate, tz));
				}
			}
			
			if(filterFrag != null) {
				paramCount = GeneralUtilityMethods.setFragParams(pstmtConvert, filterFrag, paramCount, tz);
			}
			
			if(hasRbacFilter) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmtConvert, rfArray, paramCount, tz);
			}
			
			sqlDesc.sql = GeneralUtilityMethods.getSql(pstmtConvert);
		}  finally {
			try {if (pstmtCols != null) {pstmtCols.close();}} catch (SQLException e) {}
			try {if (pstmtGeom != null) {pstmtGeom.close();}} catch (SQLException e) {}
			try {if (pstmtQType != null) {pstmtQType.close();}} catch (SQLException e) {}
			try {if (pstmtQLabel != null) {pstmtQLabel.close();}} catch (SQLException e) {}
			try {if (pstmtListLabels != null) {pstmtListLabels.close();}} catch (SQLException e) {}
			try {if (pstmtConvert != null) {pstmtConvert.close();}} catch (SQLException e) {}
		}

		return sqlDesc;
	}
	
	public static StringBuffer getJoins(Connection sd, ResourceBundle localisation, ArrayList<QueryForm> forms, 
			QueryForm prevForm,
			boolean startWithAnd) throws SQLException {
		StringBuffer join = new StringBuffer("");
		
		for(int i = 0; i < forms.size(); i++) {
			
			QueryForm form = forms.get(i);
			if(startWithAnd || i > 0) {
				join.append(" and ");
			}
			
			if(i > 0) {
				prevForm = forms.get(i - 1);
			}

			join.append(prevForm.table);
			if(form.fromQuestionId > 0) {
				if(form.toQuestionId > 0) {
					join.append(".");
					join.append(GeneralUtilityMethods.getColumnNameFromId(sd, prevForm.survey, form.toQuestionId));
					join.append(" = ");
				} else {
					join.append("._hrk = ");
				}
			} else {
				join.append(".prikey = ");
			}
			join.append(form.table);
			if(form.fromQuestionId > 0) {
				join.append(".");
				join.append(GeneralUtilityMethods.getColumnNameFromId(sd, form.survey, form.fromQuestionId));
			} else {
				join.append(".parkey");
			}
		}
		
		// Continue down the tree
		for(int i = 0; i < forms.size(); i++) {
			QueryForm form = forms.get(i);
			if(form.childForms != null && form.childForms.size() > 0) {
				join.append(getJoins(sd, localisation, form.childForms, form, true));
			}
		}
		return join;
	}
	
	/*
	 * Returns SQL to retrieve the selected form and all of its parents for inclusion in a shape file
	 *  A geometry is only returned for the level 0 form.
	 *  The order of fields is from highest form to lowest form
	 * 
	 */

	private  static void getSqlDesc(
			ResourceBundle localisation,
			SqlDesc sqlDesc, 
			int sId, 
			int level, 
			String language,
			String format,
			PreparedStatement pstmtCols,
			PreparedStatement pstmtGeom,
			PreparedStatement pstmtQType,
			PreparedStatement pstmtQLabel,
			PreparedStatement pstmtListLabels,
			String urlprefix,
			String attachmentPrefix,
			boolean wantUrl,
			boolean exp_ro,
			HashMap<ArrayList<OptionDesc>, String> labelListMap,
			Connection sd,
			Connection cResults,
			ArrayList<String> requiredColumns,
			ArrayList<String> namedQuestions,
			String user,
			Date startDate,
			Date endDate,
			int dateId,
			boolean superUser,
			QueryForm form,
			ArrayList<String> tables,
			boolean first,
			boolean meta,
			boolean includeKeys,
			String tz,
			boolean get_acc_alt,
			String geomQuestion
			) throws Exception {
		
		int colLimit = 10000;
		if(format.equals("shape")) {	// Shape files limited to 244 columns plus the geometry column
			colLimit = 244;
		}
		
		tables.add(form.table);

		String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
		ArrayList<TableColumn> cols = GeneralUtilityMethods.getColumnsInForm(
				sd,
				cResults,
				localisation,
				language,
				sId,
				surveyIdent,
				user,
				null,				// roles to apply
				form.parent,
				form.form,
				form.table,
				exp_ro,
				false,				// Don't include parent key
				false,				// Don't include "bad" columns
				false,				// Don't include instance id
				first && meta,		// Include prikey if meta set
				first && meta,		// Include HRK if meta set
				first && meta,		// Include other meta data if meta set
				first && meta,		// Include preloads if meta set
				first && meta,		// Include Instance Name in first form if meta set
				false,				// Survey duration
				first && meta,		// Include Case Management in first form if meta set
				superUser,
				false,				// HXL only include with XLS exports
				false,				// Don't include audit data
				tz,
				false,				// mgmt
				get_acc_alt,
				true		// Server calculates
				);
			
		StringBuffer colBuf = new StringBuffer();
		int idx = 0;
		
		if(includeKeys && level == 0) {
				
			TableColumn c = new TableColumn();
				
			c.column_name = "instanceid";
			c.question_name = "instanceid";
			c.type = "";
			cols.add(c);			
			sqlDesc.availableColumns.add("instanceid");
			sqlDesc.numberFields++;
			
			sqlDesc.cols = "instanceid";
			sqlDesc.colNameLookup.put(c.column_name, "instanceid");
		}
		
		for(TableColumn col : cols) {
			
			sqlDesc.colNameLookup.put(col.question_name, col.column_name);
			
			String column_name = null;
			String questionType = null;
			String type = null;
			String label = null;
			String text_id = null;
			String list_name = null;
			boolean needsReplace = false;
			ArrayList<OptionDesc> optionListLabels = null;
			
			int qId = col.qId;			
			column_name = col.column_name;
			type = col.type;
			questionType = col.type;

			if(GeneralUtilityMethods.isGeometry(type)) {
				type = "geometry";
			}
			
			if(column_name.equals("parkey") ||	column_name.equals("_bad") ||	column_name.equals("_bad_reason")
					||	column_name.equals("_task_key") ||	column_name.equals("_task_replace") ||	column_name.equals("_modified")
					||	column_name.equals("_instanceid") ||	column_name.equals("instanceid")) {
				continue;
			}
			
			// Ignore geometries in parent forms for shape, VRT, KML, Stata exports (needs to be a unique geometry)
			// Also if geomQuestion is set then ignore geometries other than that geometry question
			if(type.equals("geometry") && (
					format.equals(SmapExportTypes.SHAPE) 
					|| format.equals(SmapExportTypes.STATA)
					|| format.equals(SmapExportTypes.VRT)
					|| format.equals(SmapExportTypes.KML)
					|| format.equals(SmapExportTypes.SPSS))) {	
				if(geomQuestion != null && !geomQuestion.equals(col.question_name)) {
					continue;
				}
			}
			
			if(type.equals("geometry")) {
				// This is a geometry question, which for shape exports, has the same name as the specified geometry question
				String sqlGeom = "SELECT GeometryType(" + column_name + ") FROM " + form.table + ";";

				pstmtGeom = cResults.prepareStatement(sqlGeom);
				log.info("Get geometry type: " + pstmtGeom.toString());
				ResultSet rsGeom = pstmtGeom.executeQuery();
				
				/*
				 *  The GeometryType function will return null if a valid geometry was not created
				 *  Hence continue to the first valid geometry
				 */
				while (rsGeom.next()) {
					String geomName = rsGeom.getString(1);
					if(geomName == null) {
						continue;
					} else {
						if(geomName.startsWith("POLYGON")) {
							sqlDesc.geometry_type = "wkbPolygon";
						} else if(geomName.startsWith("LINESTRING")) {
							sqlDesc.geometry_type = "wkbLineString";
						} else if(geomName.startsWith("POINT")) {
							sqlDesc.geometry_type = "wkbPoint";
						} else {
							log.info("Error unknown geometry:  " + geomName);
						}
						sqlDesc.geometry_column = column_name;
						sqlDesc.geometry_table = form.table;
						break;
					}
				}
			} 
			
			if(requiredColumns != null) {
				boolean wantThisOne = false;
				for(int j = 0; j < requiredColumns.size(); j++) {
					if(requiredColumns.get(j).equals(column_name)) {
						wantThisOne = true;
						for(String namedQ : namedQuestions) {
							if(namedQ.equals(column_name)) {
								sqlDesc.availableColumns.add(column_name);
							}
						}
						break;
					} else if(column_name.equals("prikey") && requiredColumns.get(j).equals("_prikey_lowest")
							&& sqlDesc.gotPriKey == false && level == 0) {
						wantThisOne = true;	// Don't include in the available columns list as prikey as processed separately
						break;
					}
				}
				if(!wantThisOne) {
					continue;
				}
			} else if(column_name.equals("prikey") && (!first || !meta)) {	// Only return the primary key of the top level form
				continue;
			}
			
			if(!exp_ro && col.readonly && col.calculation == null) {
				continue;			// Drop non server calculation read only columns if they are not selected to be exported				
			}
			
			if(sqlDesc.numberFields <= colLimit || type.equals("geometry")) {
							
				// Set flag if this question has an attachment
				boolean isAttachment = GeneralUtilityMethods.isAttachmentType(type);
				
				// Get the question type
				pstmtQType.setInt(1, form.form);
				if(type.equals("select") && !col.compressed) {
					// Select multiple question
					String [] mNames = column_name.split("__");
					pstmtQType.setString(2, mNames[0]);
				} else {
					pstmtQType.setString(2, column_name);
				}
					
				ResultSet rsType = pstmtQType.executeQuery();
					
				if(rsType.next()) {
					text_id = rsType.getString(3);
					list_name = rsType.getString(5);
					if(list_name == null) {
						list_name = column_name;		// Default list name to question name if it has not been set
					}
					list_name = validStataName(list_name);	// Make sure the list name is a valid stata name
				}

				
				/*
				 * Get the labels if language has been specified
				 */
				if(!language.equals("none")) {
					label = getQuestionLabel(pstmtQLabel, sId, text_id, language);
					// Get the list labels if this is a select question
					if(type != null && type.startsWith("select")) {
						optionListLabels = new ArrayList<OptionDesc> ();
						needsReplace = getListLabels(pstmtListLabels, sId, qId, language, optionListLabels);
					}
				}
				
				if(idx > 0) {
					colBuf.append(",");
				}
				
				/*
				 * Specify SQL functions
				 */
				if(sqlDesc.geometry_type != null && type.equals("geometry") && (format.equals("vrt") || format.equals("csv") || format.equals("stata") 
						 || format.equals("xlsx"))) {
					if(sqlDesc.geometry_type.equals("wkbPoint") && (format.equals("csv") || format.equals("stata") || format.equals("spss")) ) {		// Split location into Lon, Lat
						colBuf.append("ST_Y(" + form.table + "." + column_name + ") as lat, ST_X(" + form.table + "." + column_name + ") as lon");
						sqlDesc.column_details.add(new ColDesc("lat", type, type, label, null, false, col.question_name, null, false, 
								col.displayName, col.selectDisplayNames, questionType, false));
						sqlDesc.column_details.add(new ColDesc("lon", type, type, label, null, false, col.question_name, null, false, 
								col.displayName, col.selectDisplayNames, questionType, false));
					} else {																								// Use well known text
						colBuf.append("ST_AsText(" + form.table + "." + column_name + ") as " + column_name);
						sqlDesc.column_details.add(new ColDesc(column_name, type, type, label, null, false, col.question_name, null, false, 
								col.displayName, col.selectDisplayNames, questionType, false));
					}
				} else {
				
					if(type != null && type.equals("dateTime")) {
						colBuf.append("to_char(timezone(?, ");
						sqlDesc.params.add(new SqlParam("string", tz));
					} else if(type != null && type.equals("date")) {
						colBuf.append("to_char(timezone(?, ");
						sqlDesc.params.add(new SqlParam("string", "UTC"));	// Date timezone always UTC
					} else if(type.equals("timestamptz")) {
						colBuf.append("timezone(?, "); 
						sqlDesc.params.add(new SqlParam("string", tz));
					} else if(type.equals("server_calculate")) {
						if(col.calculation != null) {
							colBuf.append(col.calculation.sql.toString());
							
							// record any parameters for server side calculations
							if (col.calculation.params != null) {
								for(SqlFragParam p : col.calculation.params) {
									sqlDesc.params.add(new SqlParam("string", p.sValue));
								}
							}
							
						} else {
							colBuf.append("''");
						}
					}
				
					if(isAttachment && wantUrl) {	// Add the url prefix to the file
						colBuf.append("'" + attachmentPrefix + "' || " + form.table + "." + column_name);
					} else if(!type.equals("server_calculate")) {
						colBuf.append(form.table + "." + column_name);
					}
				
					if(type != null && type.equals("date")) {
						colBuf.append("), 'YYYY-MM-DD')");
					} else if(type != null && type.equals("dateTime")) {
						colBuf.append("), 'YYYY-MM-DD HH24:MI:SS')");
					} else if(type.equals("timestamptz")) { 
						colBuf.append(")");
					}

					/*
					 * Specify the name of the returned variable
					 */
					colBuf.append(" as ");
					
					// Now any modifications to name will only apply to the output name not the column name
					if(format.equals("shape") && column_name.startsWith("_")) {
						column_name = column_name.replaceFirst("_", "x");	// Shape files must start with alpha's
					}
					colBuf.append(column_name);
					if(form.surveyLevel > 0) {
						colBuf.append("_");
						colBuf.append(form.surveyLevel);	// Differentiate questions from different surveys
					}
					
					sqlDesc.column_details.add(new ColDesc(column_name, type, type, label, 
							optionListLabels, needsReplace, col.question_name,
							col.choices, col.compressed, col.displayName, col.selectDisplayNames, questionType, 
							col.appearance != null && col.appearance.contains("literacy")));
					sqlDesc.column_names.add(col.column_name);
				}
				
				// Add the option labels to a hashmap to ensure they are unique
				if(optionListLabels != null) {
					labelListMap.put(optionListLabels, list_name);
				}
				
				idx++;
				sqlDesc.numberFields++;
			} else {
				log.info("Warning: Field dropped during shapefile generation: " + form.table + "." + column_name);
			}
		}
		
		if(sqlDesc.cols == null) {
			sqlDesc.cols = colBuf.toString();
		} else {
			if(colBuf.toString().trim().length() != 0) {	// Ignore tables with no columns
				sqlDesc.cols += "," + colBuf.toString();
			}
		}
		
		// Get the columns for child forms
		if(form.childForms != null && form.childForms.size() > 0) {
			for(int i = 0; i < form.childForms.size(); i++) {
				getSqlDesc(
						localisation,
						sqlDesc, 
						sId, 
						level + 1,
						language,
						format,
						pstmtCols, 
						pstmtGeom,
						pstmtQType,
						pstmtQLabel,
						pstmtListLabels,
						urlprefix,
						attachmentPrefix,
						wantUrl,
						exp_ro,
						labelListMap,
						sd,
						cResults,
						requiredColumns,
						namedQuestions,
						user,
						startDate,
						endDate,
						dateId,
						superUser,
						form.childForms.get(i),
						tables,
						false,
						meta,
						includeKeys,
						tz,
						get_acc_alt,
						geomQuestion
						);
			}
		}
	}
	
	/*
	 * Make sure the name is valid for Stata export
	 */
	private static String validStataName(String in) {
		String out = in;
		// Verify that the name does not start with a number or underscore
		if(Character.isDigit(out.charAt(0)) || out.charAt(0) == '_' ) {
			out = "a" + out;
		}
		return out;
	}
	
	/*
	 * Get the label for a question
	 */
	private static String getQuestionLabel(PreparedStatement pstmt,int sId, String text_id, String language) throws SQLException {
		String label = null;

		pstmt.setInt(1, sId);
		pstmt.setString(2, text_id);
		pstmt.setString(3, language);
		ResultSet rs = pstmt.executeQuery();
		if(rs.next()) {
			label = rs.getString(1);
		}

		return label;
	}
	
	/*
	 * For Stata get the labels that apply to a list 
	 */
	private static boolean getListLabels(PreparedStatement pstmt, int sId, int qId, 
			String language, ArrayList<OptionDesc> optionLabels) throws SQLException {
		
		pstmt.setInt(1, sId);
		pstmt.setInt(2, qId);
		pstmt.setString(3, language);
		
		boolean replacementsRequired = false;
		int maxValue = Integer.MIN_VALUE;
		
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			OptionDesc od = new OptionDesc();
			od.value = rs.getString(1);
			od.label = rs.getString(2);
			od.replace = false;
			
			// Attempt to set a numeric value for the option
			try {
				od.num_value = Integer.parseInt(od.value);
				if(maxValue < od.num_value) {
					maxValue = od.num_value;
				}
			} catch (NumberFormatException e) {
				replacementsRequired = true;
				od.replace = true;
			}
			
			optionLabels.add(od);
		}
		
		/*
		 * If we need to replace some non integer values lets start from the maximum existing integer value
		 */
		if(replacementsRequired) {
			if(maxValue < 0) {
				maxValue = 0;	// Start from a minimum of 1
			}
			for(int i = 0; i  < optionLabels.size(); i++) {
				OptionDesc od = optionLabels.get(i);
				if(od.replace) {
					od.num_value = ++maxValue;
				}
			}
		}
		
		return replacementsRequired;
	}
}
