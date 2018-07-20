package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.KeyFilter;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SqlFragParam;
import org.smap.sdal.model.TableColumn;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * 
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 * 
 ******************************************************************************/

/*
 * Manage access to a single tables data as used by managed forms, browse
 * results and the kobo api
 */
public class TableDataManager {

	private static Logger log = Logger.getLogger(TableDataManager.class.getName());
	private static ResourceBundle localisation;
	
	public TableDataManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Get the current columns
	 */
	public PreparedStatement getPreparedStatement(
			Connection sd, 
			Connection cResults, 
			ArrayList<TableColumn> columns,
			String urlprefix, 
			int sId, 
			String table_name, 
			int parkey, 
			String hrk, 
			String uIdent, 
			String sort,
			String dirn, 
			boolean mgmt, 
			boolean group, 
			boolean isDt, 
			int start, 
			int limit, 
			boolean getParkey,
			int start_parkey, 
			boolean superUser, 
			boolean specificPrikey, 
			String include_bad,
			String customFilter,
			ArrayList<KeyFilter> keyFilters)
			throws SQLException, Exception {

		StringBuffer columnSelect = new StringBuffer();
		boolean hasRbacFilter = false;
		ArrayList<SqlFrag> columnSqlFrags = new ArrayList<SqlFrag>();

		PreparedStatement pstmt = null;

		for (int i = 0; i < columns.size(); i++) {
			TableColumn c = columns.get(i);
			if (i > 0) {
				columnSelect.append(",");
			}
			columnSelect.append(c.getSqlSelect(urlprefix));
			if (c.calculation != null && c.calculation.params != null) {
				columnSqlFrags.add(c.calculation);
			}
		}

		if (GeneralUtilityMethods.tableExists(cResults, table_name)) {
			StringBuffer sqlGetData = new StringBuffer("");
			sqlGetData.append("select ");
			sqlGetData.append(columnSelect);
			sqlGetData.append(" from ");
			sqlGetData.append(table_name);
			if (specificPrikey) {
				sqlGetData.append(" where prikey = ? ");
			} else {
				sqlGetData.append(" where prikey >= ? ");
			}
			if (getParkey) {
				sqlGetData.append(" and parkey >= ?");
			}

			if (include_bad.equals("none")) {
				sqlGetData.append(" and _bad = 'false' ");
			} else if (include_bad.equals("only")) {
				sqlGetData.append(" and _bad = 'true' ");
			}

			// Add row selection clause
			StringBuffer sqlSelect = new StringBuffer("");
			if (parkey > 0) {
				sqlSelect.append(" and parkey = ?");
			}
			if (hrk != null) {
				sqlSelect.append(" and _hrk = ?");
			}

			// RBAC filter
			RoleManager rm = new RoleManager(localisation);
			ArrayList<SqlFrag> rfArray = null;
			if (!superUser) {
				rfArray = rm.getSurveyRowFilter(sd, sId, uIdent);
				if (rfArray.size() > 0) {
					String rFilter = rm.convertSqlFragsToSql(rfArray);
					if (rFilter.length() > 0) {
						sqlSelect.append(" and ");
						sqlSelect.append(rFilter);
						hasRbacFilter = true;
					}
				}
			}
			
			// Add custom filter
			if(customFilter != null) {
				sqlSelect.append(" and ").append(customFilter);
			}
			
			// Add key filters
			if(keyFilters != null) {
				for(KeyFilter kf : keyFilters) {
					kf.type = getColumnType(columns, kf.name);
					if(kf.type != null) {	// If type is null the column was not found
						sqlSelect.append(" and ").append(kf.name + " = ?");
					}
				}
			}

			StringBuffer sqlGetDataOrder = new StringBuffer("");
			if (sort != null) {
				// User has requested a specific sort order
				sqlGetDataOrder.append(" order by ");
				sqlGetDataOrder.append(getSortColumn(columns, sort));
				sqlGetDataOrder.append(" ");
				sqlGetDataOrder.append(dirn);
			} else {
				// Set default sort order
				if (mgmt) {
					sqlGetDataOrder.append(" order by prikey desc limit 10000");
				} else {
					sqlGetDataOrder.append(" order by prikey asc;");
				}
			}

			// Prepare statement
			StringBuffer sql = sqlGetData;
			sql.append(sqlSelect);
			sql.append(sqlGetDataOrder);
			pstmt = cResults.prepareStatement(sql.toString());

			// Set parameters
			int paramCount = 1;

			// Add parameters in table column selections
			if (columnSqlFrags.size() > 0) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, columnSqlFrags, paramCount);
			}
			pstmt.setInt(paramCount++, start);
			if (getParkey) {
				pstmt.setInt(paramCount++, start_parkey);
			}
			if (parkey > 0) {
				pstmt.setInt(paramCount++, parkey);
			}
			if (hrk != null) {
				pstmt.setString(paramCount++, hrk);
			}
			if (hasRbacFilter) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, rfArray, paramCount);
			}
			
			// Add key filter parameters
			if(keyFilters != null) {
				for(KeyFilter kf : keyFilters) {
					if(kf.type != null) {
						kf.setFilter(pstmt, paramCount++);
					}
				}
			}

			log.info("Get data: " + pstmt.toString());
		} else {
			throw new ApplicationException("Table does not exist");
		}
		return pstmt;

	}

	/*
	 * Get the data
	 */
	public JSONArray getData(
			PreparedStatement pstmt,
			ArrayList<TableColumn> columns, 
			String urlprefix, 
			boolean group, 
			boolean isDt, 
			int limit)
			throws SQLException, Exception {

		ResultSet rs = null;
		JSONArray ja = new JSONArray();

		rs = pstmt.executeQuery();
		int index = 0;
		while (rs.next()) {

			if (limit > 0 && index >= limit) {
				break;
			}
			index++;

			JSONObject jr = new JSONObject();
			if (group) {
				jr.put("_group", ""); // _group for duplicate queries
			}
			for (int i = 0; i < columns.size(); i++) {

				TableColumn c = columns.get(i);
				String name = null;
				String value = null;

				if (c.isGeometry()) {
					// Add Geometry (assume one geometry type per table)
					String geomValue = rs.getString(i + 1);
					if (geomValue == null) {
						geomValue = "{}";
					}
					name = "_geolocation";
					/*
					 * JSONArray coords = null; if(geomValue != null) { JSONObject jg = new
					 * JSONObject(geomValue); coords = jg.getJSONArray("coordinates"); } else {
					 * coords = new JSONArray(); }
					 */

					jr.put(name, new JSONObject(geomValue));

				} else {

					// String name = rsMetaData.getColumnName(i);
					// name = c.humanName;
					name = c.humanName;

					if (c.type != null && c.type.equals("decimal")) {
						Double dValue = rs.getDouble(i + 1);
						dValue = Math.round(dValue * 10000.0) / 10000.0;
						value = String.valueOf(dValue);
					} else if (c.type.equals("dateTime")) {
						value = rs.getString(i + 1);
						if (value != null) {
							value = value.replaceAll("\\.[0-9]+", ""); // Remove milliseconds
						}
					} else if (c.type != null && c.type.equals("calculate")) {
						// This calculation may be a decimal - give it a go
						String v = rs.getString(i + 1);
						if (v != null && v.indexOf('.') > -1) {
							try {
								Double dValue = rs.getDouble(i + 1);
								dValue = Math.round(dValue * 10000.0) / 10000.0;
								value = String.valueOf(dValue);
							} catch (Exception e) {
								value = rs.getString(i + 1); // Assume text
							}
						} else {
							value = rs.getString(i + 1); // Assume text
						}

					} else {
						value = rs.getString(i + 1);
					}

					if (value == null) {
						value = "";
					}

					if (name != null) {
						if (!isDt) {
							name = GeneralUtilityMethods.translateToKobo(name);
						}
						jr.put(name, value);
					}
				}

			}

			ja.put(jr);
		}

		return ja;

	}
	
	/*
	 * Get the next record of data
	 */
	public JSONObject getNextRecord(
			ResultSet rs,
			ArrayList<TableColumn> columns, 
			String urlprefix, 
			boolean group, 
			boolean isDt, 
			int limit,
			boolean mergeSelectMultiple,
			boolean isGeoJson)
			throws SQLException, Exception {

		JSONObject jr = null;
		JSONObject jp = null;
		JSONObject jf = null;
		JSONObject jGeom = null;
		String id = null;
		
		if (rs.next()) {

			jr = new JSONObject();
			if (group) {
				jr.put("_group", ""); // _group for duplicate queries
			}
			if(isGeoJson) {
				jr.put("type", "Feature");
				jp = new JSONObject();
				jr.put("properties", jp);
				jf = jp;
			} else {
				jf = jr;
			}
			for (int i = 0; i < columns.size(); i++) {

				TableColumn c = columns.get(i);
				String name = null;
				String value = null;

				if (c.isGeometry()) {
					// Add Geometry (assume one geometry type per table)
					String geomValue = rs.getString(i + 1);
					if (geomValue == null) {
						geomValue = "{}";
					}
					if(isGeoJson) {
						jGeom = new JSONObject(geomValue);
					} else {
						name = "_geolocation";
						jf.put(name, new JSONObject(geomValue));
					}

				} else if(c.type != null && c.type.equals("select") && c.compressed && !mergeSelectMultiple) {
					// Split the select multiple into its choices
					name = c.humanName;
					value = rs.getString(i + 1);
					if (value == null) {
						value = "";
					}
					String[] selected = {""};
					selected = value.split(" ");
					for(KeyValue kv: c.choices) {
						String choiceName = name + " - " + kv.k;
						boolean addChoice = false;
						for(String selValue : selected) {
							if(selValue.equals(kv.v)) {
								addChoice = true;
								break;
							}	
						}
						String choiceValue = addChoice ? "1" : "0";
						jf.put(choiceName, choiceValue);
					}
				} else {

					name = c.humanName;

					if (c.type != null && c.type.equals("decimal")) {
						Double dValue = rs.getDouble(i + 1);
						dValue = Math.round(dValue * 10000.0) / 10000.0;
						value = String.valueOf(dValue);
					} else if (c.type.equals("dateTime")) {
						value = rs.getString(i + 1);
						if (value != null) {
							value = value.replaceAll("\\.[0-9]+", ""); // Remove milliseconds
						}
					} else if (c.type != null && c.type.equals("calculate")) {
						// This calculation may be a decimal - give it a go
						String v = rs.getString(i + 1);
						if (v != null && v.indexOf('.') > -1) {
							try {
								Double dValue = rs.getDouble(i + 1);
								dValue = Math.round(dValue * 10000.0) / 10000.0;
								value = String.valueOf(dValue);
							} catch (Exception e) {
								value = rs.getString(i + 1); // Assume text
							}
						} else {
							value = rs.getString(i + 1); // Assume text
						}

					} else {
						value = rs.getString(i + 1);
					}

					if (value == null) {
						value = "";
					}

					if (name != null) {
						if (!isDt) {
							name = GeneralUtilityMethods.translateToKobo(name);
						}
						if(isGeoJson && name.equals("prikey")) {
							id = value;
						} else {
							jf.put(name, value);
						}
					}
				}

			}

		}

		if(jGeom != null) {
			jr.put("geometry", jGeom);
		}
		if(id != null) {
			jr.put("id", id);
		}
		return jr;

	}

	/*
	 * Convert the human name for the sort column into sql
	 */
	private String getSortColumn(ArrayList<TableColumn> columns, String sort) {
		String col = "prikey"; // default to prikey
		sort = sort.trim();
		for (int i = 0; i < columns.size(); i++) {
			if (columns.get(i).humanName.equals(sort)) {
				TableColumn c = columns.get(i);

				if (c.isCalculate()) {
					col = c.calculation.sql.toString();
				} else {
					col = c.name;
				}
				break;
			}
		}
		return col;
	}
	
	/*
	 * Convert the human name for the sort column into sql
	 */
	private String getColumnType(ArrayList<TableColumn> columns, String name) {
		String type = null; 
		name = name.trim();
		if(name.equals("prikey") || name.equals("parkey")) {
			type = "int";
		} else {
			for (int i = 0; i < columns.size(); i++) {
				if (columns.get(i).humanName.equals(name)) {
					TableColumn c = columns.get(i);
					type = c.type;
					break;
				}
			}
		}
		return type;
	}

}
