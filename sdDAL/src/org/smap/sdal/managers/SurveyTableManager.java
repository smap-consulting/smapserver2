package org.smap.sdal.managers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.LanguageItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.Pulldata;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.ServerCalculation;
import org.smap.sdal.model.SqlFrag;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
 * Manage the csvtable entries that point to survey data rather than data loaded directly from a CSV
 */
public class SurveyTableManager {

	private static Logger log = Logger.getLogger(SurveyTableManager.class.getName());
	
	/*
	 * Class to return SQL
	 */
	private class SqlDef {
		private String sql;
		private String order_by;
		private boolean hasWhere = false; 
		private ArrayList<String> colNames;
		private ArrayList<String> qnames;
		private boolean hasRbacFilter = false;
		private ArrayList<SqlFrag> rfArray = null;
		private ArrayList<SqlFrag> calcArray = null;
		private boolean hasGeom = false;
	}
	
	LogManager lm = new LogManager(); // Application log
	public static String PD_IDENT = "linked_s_pd_";
	
	Connection sd = null;
	Connection cResults = null;
	ResourceBundle localisation = null;
	private int tableId = 0;
	ResultSet rs = null;
	
	// Global variables
	private SqlDef sqlDef = null;
	private boolean non_unique_key = false;
	private boolean chart;
	private String linked_sIdent;
	private String chart_key;
	private boolean linked_s_pd = false;
	
	/*
	 * Constructor to create a table to hold the CSV data if it does not already exist
	 */
	public SurveyTableManager(Connection sd, Connection cResults, ResourceBundle l, int oId, int sId, String fileName, String user)
			throws Exception {
		
		this.sd = sd;
		this.cResults = cResults;
		this.localisation = l;
		
		if(oId <= 0) {
			log.info("************************ Error: Create Survey Table Manager : Organisation id is less than or equal to 0");
		} else {
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
			
			String sqlGetCsvTable = "select id, sqldef, non_unique_key, chart, linked_sident, chart_key, linked_s_pd "
					+ "from csvtable "
					+ "where o_id = ? "
					+ "and s_id = ? "
					+ "and survey "
					+ "and filename = ? ";
			PreparedStatement pstmtGetCsvTable = null;
			
			String sqlInsertCsvTable = "insert into csvtable (id, o_id, s_id, filename, survey, ts_initialised) "
					+ "values(nextval('csv_seq'), ?, ?, ?, true, now())";
			PreparedStatement pstmtInsertCsvTable = null;
			try {
				pstmtGetCsvTable = sd.prepareStatement(sqlGetCsvTable);
				pstmtGetCsvTable.setInt(1, oId);
				pstmtGetCsvTable.setInt(2, sId);
				pstmtGetCsvTable.setString(3, fileName);
				log.info("Getting csv table id: " + pstmtGetCsvTable.toString());
				ResultSet rs = pstmtGetCsvTable.executeQuery();
				
				if(rs.next()) {
					tableId = rs.getInt(1);
					sqlDef = gson.fromJson(rs.getString(2), SqlDef.class);
					non_unique_key = rs.getBoolean(3);
					chart = rs.getBoolean(4);
					linked_sIdent = rs.getString(5);
					chart_key = rs.getString(6);
					linked_s_pd = rs.getBoolean(7);
				} else {
					/*
					 * Create the headers and sql
					 */				
					pstmtInsertCsvTable = sd.prepareStatement(sqlInsertCsvTable, Statement.RETURN_GENERATED_KEYS);
					pstmtInsertCsvTable.setInt(1, oId);
					pstmtInsertCsvTable.setInt(2, sId);
					pstmtInsertCsvTable.setString(3, fileName);
					log.info("Create a new csv file entry (Survey Manager): " + pstmtInsertCsvTable.toString());
					pstmtInsertCsvTable.executeUpdate();
					ResultSet rsKeys = pstmtInsertCsvTable.getGeneratedKeys();
					if(rsKeys.next()) {
						tableId = rsKeys.getInt(1);
					} else {
						throw new Exception("Failed to create CSV Table entry");
					}
				}
				if(sqlDef == null || sqlDef.colNames.size() == 0) {
					getSqlAndHeaders(sd, cResults, sId, fileName);
				}
	
				
			} finally {
				try {pstmtGetCsvTable.close();} catch(Exception e) {}
				try {pstmtInsertCsvTable.close();} catch(Exception e) {}
			}
		}
	}

	/*
	 * Constructor that does not attempt to connect to a table or create a new table
	 */
	public SurveyTableManager(Connection sd, ResourceBundle l)
			throws Exception {
		
		this.sd = sd;
		this.localisation = l;
		
	}
	
	/*
	 * Get columns
	 */
	public ArrayList<String> getColumns() {
		ArrayList<String> cols = null;
		if(sqlDef != null) {
			cols = sqlDef.colNames;
		}
		return cols;
	}

	
	/*
	 * Get a result set of data for a lookup
	 * type = lookup || choices
	 */
	public void initData(
			PreparedStatement pstmt, 
			String type, 
			String selection, 
			ArrayList<String> arguments, 
			SqlFrag expressionFrag,		// A more general approach than using "whereColumns". The latter should probably be deprecated
			String tz,
			ArrayList<SqlFrag> qArray,
			ArrayList<SqlFrag> fArray
			) throws Exception {
		
		if(sqlDef != null && sqlDef.colNames != null && sqlDef.colNames.size() > 0) {
			StringBuilder sql = new StringBuilder(sqlDef.sql);
			
			if(expressionFrag != null || selection != null) { 
				if(sqlDef.hasWhere) {
					sql.append(" and ");
				} else {
					sql.append(" where ");
				}
				if(expressionFrag != null) {
					sql.append(" ( ").append(expressionFrag.sql).append(")");
				} else if(selection != null) {
					sql.append(selection);
				}
			}

			sql.append(sqlDef.order_by);
			String sqlString = sql.toString();
			sqlString = sqlString.replace("\\", "");		// Extra \ escapes are required of the string is passed to PSQL for generation - we don't need them here
			pstmt = cResults.prepareStatement(sqlString);
			int paramCount = 1;
			if (sqlDef.calcArray != null) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, sqlDef.calcArray, paramCount, tz);
			}
			if (qArray != null) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, qArray, paramCount, tz);
			}
			if (fArray != null) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, fArray, paramCount, tz);
			}
			if (sqlDef.hasRbacFilter) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, sqlDef.rfArray, paramCount, tz);
			}
			
			if(expressionFrag != null) {
				paramCount = GeneralUtilityMethods.setFragParams(pstmt, expressionFrag, paramCount, tz);
			} else if(arguments != null) {
				log.info("Setting parameters: " + pstmt.toString());
				for(String arg : arguments) {
					log.info("Parameter: " + arg);
					pstmt.setString(paramCount++, arg);
				}
			}		
			
			log.info("Init data: " + pstmt.toString());
			try {
				rs = pstmt.executeQuery();
			} catch (Exception e) {
				String msg = e.getMessage();
				if(msg != null && msg.contains("does not exist")) {
					log.info("Attempting to get data from a survey that has had no data submitted. " + msg);
				} else {
					log.log(Level.SEVERE, msg, e);
					throw new ApplicationException(msg + " : " + pstmt.toString());
				}
				rs = null;
			}
		} else {
			rs = null;
		}
	}
	
	/*
	 * Get a line of data
	 */
	public ArrayList<KeyValueSimp> getLine() throws SQLException {
		ArrayList<KeyValueSimp> line = null;
		
		if(rs != null && rs.next()) {
			line = new ArrayList<KeyValueSimp> ();
			for (int i = 0; i < sqlDef.colNames.size(); i++) {
				String qname = sqlDef.colNames.get(i);
				// support labels separated by commas
				String [] multQuestions = qname.split(",");
				String value = "";
				if(multQuestions.length > 1) {
					for(String q : multQuestions) {
						String v = rs.getString(q.trim());
						if(v != null) {
							if(value.length() > 0) {
								value += ", ";
							}
							value += v;
						}
					}
				} else {
					value = rs.getString(qname);
				}
				if (value == null) {
					value = "";
				}
				line.add(new KeyValueSimp(qname, value));
			}
		}
		return line;
	}
	
	/*
	 * Get a data hashmap
	 */
	public HashMap<String, String> getLineAsHash() throws SQLException {
		
		HashMap<String, String> line = null;
		
		if(rs != null && rs.next()) {
			line = new HashMap<String, String> ();
			for (int i = 0; i < sqlDef.colNames.size(); i++) {
				String col = sqlDef.colNames.get(i);
				String value = rs.getString(col);
				if (value == null) {
					value = "";
				}
				line.put(col, value);
			}
		}
		return line;
	}
	
	/*
	 * Get a choice
	 */
	public Option getLineAsOption(String oValue, 
			ArrayList<LanguageItem> items,
			ArrayList<KeyValueSimp> wfFilterColumns) throws SQLException {
		Option o = null;
		
		if(rs != null && rs.next()) {
			o = new Option ();
			o.value = rs.getString(oValue);
			for(LanguageItem item : items) {
				Label l = new Label();
				if(item.text.contains(",")) {
					String[] comp = item.text.split(",");
					l.text = "";
					for(int i = 0; i < comp.length; i++) {
						if(i > 0) {
							l.text += ", ";
						}
						l.text += rs.getString(comp[i].trim());
					}
				} else {
					l.text = rs.getString(item.text);
				}
				
				o.labels.add(l);
				
				if(wfFilterColumns != null && wfFilterColumns.size() > 0) {
					o.cascade_filters = new HashMap<>();
					for(KeyValueSimp fc : wfFilterColumns) {
						o.cascade_filters.put(fc.k, rs.getString(fc.k)); 
					}
				}
			}
		}
		return o;
	}
	
	/*
	 * Get an sql filter clause
	 */
	public String getFilter(String key_column, String key_value) throws Exception {
		
		String filter = "";
		if(sqlDef != null && key_column != null && key_value != null) {
			for(String colName : sqlDef.colNames) {
				if(key_column.equals(colName)) {
					filter = key_column + "::text = ?";  
					break;
				}
			}	
		}
		return filter;
	}
	
	/*
	 * Delete entries in csv table
	 */
	public void delete(int sId) throws SQLException {
		
		String sqlDelete = "delete from csvtable where s_id = ? and survey";
		PreparedStatement pstmtDelete = null;
				
		try {
			pstmtDelete = sd.prepareStatement(sqlDelete);
			pstmtDelete.setInt(1, sId);
			pstmtDelete.executeUpdate();
					
		} catch(Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			try {pstmtDelete.close();} catch(Exception e) {}
		}
	}
	
	/*
	 * Get the SQL needed to retrieve the data as well as the headers
	 */
	public void getSqlAndHeaders(Connection sd, Connection cRel, int sId, // The survey that contains the manifest item
			String filename) throws Exception {

		ResultSet rs = null;
		boolean chart = false;
		int linked_sId = 0;
		String data_key = null;
		ArrayList<Pulldata> pdArray = null;

		String sqlPulldata = "select pulldata from survey where s_id = ?";
		PreparedStatement pstmtPulldata = null;

		String sqlAppearance = "select q_id, appearance from question "
				+ "where f_id in (select f_id from form where s_id = ?) " + "and appearance is not null;";
		PreparedStatement pstmtAppearance = null;

		String sqlCalculate = "select q_id, calculate from question "
				+ "where f_id in (select f_id from form where s_id = ?) " + "and calculate is not null;";
		PreparedStatement pstmtCalculate = null;

		String sqlUpdate = "update csvtable set "
				+ "chart = ?,"
				+ "non_unique_key = ?,"
				+ "linked_sident = ?, "
				+ "chart_key = ?,"
				+ "linked_s_pd = ?,"
				+ "sqldef = ? "
				+ "where id = ?";
		PreparedStatement pstmtUpdate = null;
		try {

			ArrayList<String> uniqueColumns = new ArrayList<String>();
			/*
			 * Get parameters There are three types of linked CSV files generated 
			 * 1. Parent child records where there can be many records from a sub form that match the
			 *  key. Filename starts with "linked_s_pd_" (PD_IDENT) 
			 * 2. Normal lookup where there is only one record that should match a key. Filename starts with
			 *  "linked_"
			 * 3. Time series data.  Filename starts with "chart_s"
			 */
			if (filename.startsWith(PD_IDENT)) {
				linked_s_pd = true;

				linked_sIdent = filename.substring(PD_IDENT.length());

				pstmtPulldata = sd.prepareStatement(sqlPulldata);
				pstmtPulldata.setInt(1, sId);
				log.info("Get pulldata key from survey: " + pstmtPulldata.toString());
				rs = pstmtPulldata.executeQuery();
				if (rs.next()) {
					Type type = new TypeToken<ArrayList<Pulldata>>() {}.getType();
					pdArray = new Gson().fromJson(rs.getString(1), type);
					if (pdArray == null) {
						throw new Exception("Pulldata definition not found for survey: " + sId + " and file " + filename
								+ ". Set the pulldata definition from the online editor file menu.");
					}
					for (int i = 0; i < pdArray.size(); i++) {
						String pulldataIdent = pdArray.get(i).survey;

						if (pulldataIdent.equals("self")) {
							pulldataIdent = linked_sIdent;
						}
						log.info("PulldataIdent: " + pulldataIdent);

						if (pulldataIdent.equals(linked_sIdent)) {
							data_key = pdArray.get(i).data_key;
							non_unique_key = true;
							break;
						}
					}
				} else {
					throw new Exception("No record found for pull data");
				}

				if (data_key == null) {
					throw new Exception("Pulldata data_key not found");
				}

			} else if (filename.startsWith("linked_s")){
				int idx = filename.indexOf('_');
				linked_sIdent = filename.substring(idx + 1);
			} else if (filename.startsWith("chart_s")) {  // Form: chart_sxx_yyyy_keyname we want sxx_yyyy
				chart = true;
				if(filename.startsWith("chart_self")) {
					linked_sIdent = "self";
					int idx1 = filename.indexOf('_');
					int idx2 = filename.indexOf('_', idx1 + 1);
					if(idx2 > 0) {
						chart_key = filename.substring(idx2 + 1);
					}
				} else {
					int idx1 = filename.indexOf('_');
					int idx2 = filename.indexOf('_', idx1 + 1);
					idx2 = filename.indexOf('_', idx2 + 1);
					if(idx2 > 0) {
						linked_sIdent = filename.substring(idx1 + 1, idx2);
						chart_key = filename.substring(idx2 + 1);
					} else {
						linked_sIdent = filename.substring(idx1 + 1);
					}
				}
			}

			if (linked_sIdent != null && linked_sIdent.equals("self")) {
				linked_sId = sId;
				linked_sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			} else {
				linked_sId = GeneralUtilityMethods.getSurveyId(sd, linked_sIdent);
				if(!GeneralUtilityMethods.inSameOrganisation(sd, sId, linked_sId)) {
					throw new ApplicationException("Cannot link to external survey: " + linked_sIdent + " as it is in a different organisation");
				}
			}
			if(linked_sId == 0) {
				throw new ApplicationException("Error: Survey with identifier " + linked_sIdent + " was not found");
			}

			// 3.Get question names from appearance
			pstmtAppearance = sd.prepareStatement(sqlAppearance);
			pstmtAppearance.setInt(1, sId);
			log.info("Appearance cols: " + pstmtAppearance.toString());
			rs = pstmtAppearance.executeQuery();
			while (rs.next()) {
				int qId = rs.getInt(1);
				String appearance = rs.getString(2);
				ArrayList<String> columns = GeneralUtilityMethods.getManifestParams(sd, qId, appearance, 
						filename, true, linked_sIdent);
				if (columns != null) {
					for (String col : columns) {
						if (!uniqueColumns.contains(col)) {
							uniqueColumns.add(col);
						}
					}
				}
			}

			// 4. Get question names from calculate
			pstmtCalculate = sd.prepareStatement(sqlCalculate);
			pstmtCalculate.setInt(1, sId);
			log.info("Calculate cols: " + pstmtCalculate.toString());
			rs = pstmtCalculate.executeQuery();
			while (rs.next()) {
				int qId = rs.getInt(1);
				String calculate = rs.getString(2);
				ArrayList<String> columns = GeneralUtilityMethods.getManifestParams(sd, qId, calculate, filename,
						false, linked_sIdent);
				if (columns != null) {
					for (String col : columns) {
						if (!uniqueColumns.contains(col)) {
							uniqueColumns.add(col);
							log.info("Adding unique column: " + col);
						}
					}
				}
			}

			log.info("Unique Columns: " + uniqueColumns.size());
			// 5. Get the sql as long as there is data to retrieve
			
			if(uniqueColumns.size() > 0) {
				
				sqlDef = getSql(sd, linked_sId, uniqueColumns, linked_s_pd, data_key, chart_key);
				if(sqlDef != null) {
					sqlDef.qnames = uniqueColumns;		// Set question names to requested names
				}

				pstmtUpdate = sd.prepareStatement(sqlUpdate);
				pstmtUpdate.setBoolean(1, chart);
				pstmtUpdate.setBoolean(2, non_unique_key);
				pstmtUpdate.setString(3, linked_sIdent);
				pstmtUpdate.setString(4, chart_key);
				pstmtUpdate.setBoolean(5, linked_s_pd);
				pstmtUpdate.setString(6, new Gson().toJson(sqlDef));
				pstmtUpdate.setInt(7, tableId);
				log.info("Add sql info: " + pstmtUpdate.toString());
				pstmtUpdate.executeUpdate();
			} else {
				throw new ApplicationException("Error: no columns found in linked survey " + sId + " for file " + filename);
			}

			

		} finally {
			if (pstmtAppearance != null)	{try {pstmtAppearance.close();} catch (Exception e) {}}
			if (pstmtCalculate != null) {try {pstmtCalculate.close();	} catch (Exception e) {}}
			if (pstmtUpdate != null) {try {pstmtUpdate.close();} catch (Exception e) {}}
			if (pstmtPulldata != null) {	try {pstmtPulldata.close();} catch (Exception e) {}}
		}

	}
	
	/*
	 * Get the SQL to retrieve dynamic CSV data TODO replace this with the query
	 * generator from SDAL
	 */
	public SqlDef getSql(Connection sd, int sId, ArrayList<String> qnames, boolean linked_s_pd, String data_key,
			String chart_key) throws Exception {

		StringBuffer sql = new StringBuffer("select ");
		if(chart_key == null) {		// Time series data should not be made distinct
			sql.append("distinct ");
		}
		StringBuffer where = new StringBuffer("");
		StringBuffer tabs = new StringBuffer("");
		StringBuffer order_cols = new StringBuffer("");
		String linked_s_pd_sel = null;
		SqlDef newSqlDef = new SqlDef();
		ArrayList<String> colNames = new ArrayList<>();
		ArrayList<String> subTables = new ArrayList<>();
		HashMap<String, String> tables = new HashMap<>();
		Form topForm = GeneralUtilityMethods.getTopLevelForm(sd, sId);

		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
	
		String sqlGetTable = "select f_id, table_name from form " 
				+ "where s_id = ? " 
				+ "and parentform = ?";
		PreparedStatement pstmtGetTable = null;
		
		try {

			// 1. Get the columns in the group
			SurveyManager sm = new SurveyManager(localisation, "UTC");					
			String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
			HashMap<String, QuestionForm> refQuestionMap = sm.getGroupQuestionsMap(sd, groupSurveyIdent, null, false);

			log.info("Question forms: " + refQuestionMap.toString());

			boolean first = true;
			if (linked_s_pd) {
				linked_s_pd_sel = GeneralUtilityMethods.convertAllxlsNamesToQuery(data_key, sId, sd, null);	// data_key should not include sequence
				sql.append(linked_s_pd_sel);
				sql.append(" as _data_key");
				first = false;
			}

			tables.put(topForm.tableName, topForm.tableName); // Always add top level form
			for (int i = 0; i < qnames.size(); i++) {
				String name = qnames.get(i);
				if (name.equals("_data_key") || name.equals("_count")) {
					continue; // Generated not extracted
				}
				String colName = null;
				String colType = null;
				String serverCalculate = null;
				String[] multNames = name.split(",");		// Allow for comma separated labels
				for(String n : multNames) {
					n = n.trim();
					String tableName;
					
					QuestionForm qf = refQuestionMap.get(n);
					
					if (qf != null && qf.published) {
						colName = qf.columnName;
						tableName = qf.tableName;
						colType = qf.qType;
						serverCalculate = qf.serverCalculate;
						SqlFrag calculation = null;
						
						if(colType.equals("server_calculate") && serverCalculate != null) {
							ServerCalculation sc = gson.fromJson(serverCalculate, ServerCalculation.class);
							calculation = new SqlFrag();
							sc.populateSql(calculation, localisation);
							colName = calculation.sql.toString();
							if(newSqlDef.calcArray == null) {
								newSqlDef.calcArray = new ArrayList<>();
							}
							newSqlDef.calcArray.add(calculation);
						} else if(colType.equals("geopoint")) {
							colName = "ST_Y(" + tableName + "." + colName + ") || ' ' || ST_X(" + tableName + "." + colName + ")";
						} else if(colType.equals("geoshape") || colType.equals("geotrace")) {
							colName = "ST_AsText(" + tableName + "." + colName + ")";
							newSqlDef.hasGeom = true;
						}
						
					} else if (SmapServerMeta.isServerReferenceMeta(n)) {
						colName = n; // For columns that are not questions such as _hrk, _device
						tableName = topForm.tableName;
					} else if(GeneralUtilityMethods.hasColumn(cResults, topForm.tableName, n)) {
						// This is not group friendly but will pick up meta items that have been referenced in the top form
						colName = n; // For columns that are not questions such as _hrk, _device
						tableName = topForm.tableName;
					} else {
						if(GeneralUtilityMethods.tableExists(cResults, topForm.tableName)) {
							// Only report the error if the top level table has been created otherwise probably no data has been submitted and all columns would be unpublished and missing
							lm.writeLog(sd, sId, null, LogManager.ERROR, n + " " + localisation.getString("imp_nfi"), 0, null);
						}
						continue; // Name not found
					}
					colNames.add(n);
					tables.put(tableName, tableName);
	
					if (!first) {
						sql.append(",");
					}
					sql.append(colName);
					sql.append(" as ");
					sql.append("\\\"" + n + "\\\"");
					first = false;
	
				}
			}

			// 2. Add the tables
			pstmtGetTable = sd.prepareStatement(sqlGetTable);
			pstmtGetTable.setInt(1, sId);
			log.info("Tables: " + tables.size() + " : " + tables.toString());
			getTables(pstmtGetTable, 0, null, tabs, where, tables, subTables);
			log.info("Subtables: " + subTables.size());
			
			// 2.5 Add the order clause
			sql.append(",")
				.append(topForm.tableName)
				.append(".prikey as prikey_").append(topForm.tableName);
			order_cols.append(topForm.tableName  + ".prikey desc");
			if (subTables.size() > 0) {
				for (String subTable : subTables) {
					sql.append(",")
						.append(subTable)
						.append(".prikey as prikey_").append(subTable);
					order_cols.append(","  + subTable  + ".prikey desc");   // Use descending to align with local data
				}
			}

			sql.append(" from ");
			sql.append(tabs);

			// 3. Add the where clause
			if (where.length() > 0) {
				sql.append(" where ");
				newSqlDef.hasWhere = true;
				sql.append(where);
			}

			// 4. Add the RBAC/Row filter
			// Add RBAC/Role Row Filter
			newSqlDef.rfArray = null;
			newSqlDef.hasRbacFilter = false;

			// If this is a pulldata linked file then order the data by _data_key and then
			// the primary keys of sub forms
			StringBuffer orderBy = new StringBuffer("");
			if(chart_key != null) {
				orderBy.append(" order by ");
				orderBy.append(chart_key);
				orderBy.append(" asc");		// Historically used ascending
			} else if (linked_s_pd) {
				orderBy.append(" order by _data_key");
				if (subTables.size() > 0) {
					for (String subTable : subTables) {
						orderBy.append(",");
						orderBy.append(subTable);
						orderBy.append(".prikey asc");	// Historically this has used ascending ordering
					}
				} else {
					orderBy.append(" asc");
				}
			} else if(order_cols != null) {
				// order by the columns
				orderBy.append(" order by ");
				orderBy.append(order_cols);
			}
			newSqlDef.order_by = orderBy.toString();

		} finally {
			
			if (pstmtGetTable != null) try {pstmtGetTable.close();} catch (Exception e) {}
		}

		newSqlDef.sql = sql.toString();
		newSqlDef.colNames = colNames;
		return newSqlDef;
	}
	
	/*
	 * Get table details
	 */
	private void getTables(PreparedStatement pstmt, int parentId, String parentTable, StringBuffer tabs,
			StringBuffer where, HashMap<String, String> tables, ArrayList<String> subTables) throws SQLException {

		ArrayList<Integer> parents = new ArrayList<>();
		ArrayList<String> parentTables = new ArrayList<>();

		pstmt.setInt(2, parentId);
		log.info("Get tables: " + pstmt.toString());
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			int fId = rs.getInt(1);
			String table = rs.getString(2);

			log.info("Processing form: " + fId + " : " + table + " : " + parentId);
			log.info(tables.toString());
			
			/*
			 * Ignore tables that where no questions have been asked for
			 * Use Left outer join processing so that situations where a subform is empty do not cause disappearance of data
			 */
			if (tables.get(table) != null) {

				if(parentId != 0) {
					subTables.add(table);
				}
				
				// Update table list
				if (tabs.length() > 0) {
					tabs.append(" left outer join ");
					tabs.append(table);
					tabs.append(" on ");
					tabs.append(table);
					tabs.append(".parkey = ");
					tabs.append(parentTable);
					tabs.append(".prikey");
					//tabs.append(",");
				} else {
					tabs.append(table);
				}

				// update where statement
				if (parentId == 0) {
					if (where.length() > 0) {
						where.append(" and ");
					}
					where.append(table);
					where.append("._bad = 'false'");
				} 
				parents.add(fId);
				parentTables.add(table);
			}

		}

		for (int i = 0; i < parents.size(); i++) {
			int fId = parents.get(i);
			String table = parentTables.get(i);
			getTables(pstmt, fId, table, tabs, where, tables, subTables);
		}

	}

	public ArrayList<Option> getChoices(String ovalue, ArrayList<LanguageItem> languageItems, 
			ArrayList<KeyValueSimp> wfFilterColumns) throws SQLException {
		ArrayList<Option> choices = new ArrayList<> ();
		
		Option o = null;
		HashMap<String, String> choicesLoaded = new HashMap<String, String> ();		// Eliminate duplicates
		while((o = getLineAsOption(ovalue, languageItems, wfFilterColumns)) != null) {
			StringBuffer uniqueChoice = new StringBuffer("");
			uniqueChoice.append(o.value);
			if(wfFilterColumns != null && wfFilterColumns.size() > 0) {
				for(KeyValueSimp fc : wfFilterColumns) {	
					String filterValue = o.cascade_filters.get(fc.k);
					uniqueChoice.append(":::").append(filterValue);
				}
			}
			if(choicesLoaded.get(uniqueChoice.toString()) == null) {
				choices.add(o);
				choicesLoaded.put(uniqueChoice.toString(), "x");
			}
		}
		
		return choices;
	}
	
	public ArrayList<String> getQuestionNames(int sId, String filename) throws SQLException {
		
		ArrayList<String> qnames = null;
		
		String sql = "select sqldef "
				+ "from csvtable "
				+ "where s_id = ? "
				+ "and filename = ? ";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  sId);
			pstmt.setString(2, filename);
			ResultSet rs = pstmt.executeQuery();
			log.info("Get sqldef in order to get columns: " + pstmt.toString());
			if(rs.next()) {
				String s = rs.getString(1);
				if(s != null) {
					Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
					SqlDef sqlDef = gson.fromJson(s, SqlDef.class);
					if(sqlDef.colNames.size() == 0) {
						qnames = sqlDef.qnames;		// Use requested names
					} else {
						qnames = sqlDef.colNames;	// Use validated names that are in the table
					}
					
				}
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
		return qnames;
				
	}
	
	/*
	 * Generate a CSV file from the survey reference data
	 */
	public boolean generateCsvFile(Connection cResults, File f, int sId, String userName, String basePath) {
		PreparedStatement pstmtData = null;
		boolean status = false;
		try {
			String sql = sqlDef.sql + sqlDef.order_by;			// Escape quotes when passing sql to psql
			String sqlNoEscapes = sql.replace("\\", "");		// Remove escaping of quotes when used in prepared statement
			
			if(sqlDef.colNames.size() == 0) {
				log.info("++++++ No column names present in table. Creating empty file");
				
				// Use requested columns which will be in the qnames list
				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(f.getAbsoluteFile()), "UTF8"));
				for (int i = 0; i < sqlDef.qnames.size(); i++) {
					if(i > 0) {
						bw.write(",");
					}
					bw.write(sqlDef.qnames.get(i));
				}
				bw.newLine();
				bw.flush();
				bw.close();				
			} else if (linked_s_pd && non_unique_key) {
				// 6. Create the file
				pstmtData = cResults.prepareStatement(sqlNoEscapes);
				
				log.info("Get CSV data: " + pstmtData.toString());
				rs = pstmtData.executeQuery();

				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(f.getAbsoluteFile()), "UTF8"));

				// Write header
				bw.write("_data_key");
				if (non_unique_key) {
					bw.write(",_count");
				}
				for (int i = 0; i < sqlDef.colNames.size(); i++) {
					String col = sqlDef.colNames.get(i);
					bw.write(",");
					bw.write(col);
				}
				bw.newLine();

				/*
				 * Class to store a set of records for a single key when non unique key has
				 * been specified This allows us to count the number of duplicate keys before
				 * writing the data to the csv file
				 */
				ArrayList<StringBuilder> nonUniqueRecords = new ArrayList<StringBuilder>();

				// Write data
				String currentDkv = null; // Current value of the data key
				String dkv = null;
				while (rs.next()) {
					dkv = rs.getString("_data_key");
					if(dkv == null) {
						continue;	// Ignore null keys
					}
					if (!dkv.equals(currentDkv)) {
						// A new data key
						writeRecords(non_unique_key, nonUniqueRecords, bw, currentDkv);
						nonUniqueRecords = new ArrayList<StringBuilder>();
						currentDkv = dkv;
					}

					// Store the record
					StringBuilder s = new StringBuilder("");
					// Don't write the key yet
					if (non_unique_key) {
						s.append(",");
					}
					for (int i = 0; i < sqlDef.colNames.size(); i++) {
						String col = sqlDef.colNames.get(i);
						s.append(",");
						String value = rs.getString(col);
						if (value == null) {
							value = "";
						}
						s.append(value);
					}
					nonUniqueRecords.add(s);
				}

				// Write the records for the final key
				writeRecords(non_unique_key, nonUniqueRecords, bw, currentDkv);

				bw.flush();
				bw.close();
			} else if (chart) {

				HashMap<String, ArrayList<String>> chartData = new HashMap<> ();
				pstmtData = cResults.prepareStatement(sqlNoEscapes);
				
				if(rs != null) {
					rs.close();
				}
				log.info("####### Getting chart data: " + pstmtData.toString());
				rs = pstmtData.executeQuery();

				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(f.getAbsoluteFile()), "UTF8"));

				// Write header
				bw.write(chart_key);
				
				for (int i = 0; i < sqlDef.colNames.size(); i++) {
					String col = sqlDef.colNames.get(i);
					if(!col.equals(chart_key)) {
						bw.write(",");
						bw.write(col);
					}
				}
				bw.newLine();

				// Write data
				String currentDkv = null; // Current value of the data key
				String dkv = null;
				while (rs.next()) {
					dkv = rs.getString(chart_key);
					if (dkv != null && !dkv.equals(currentDkv)) {
						// A new data key
						if(currentDkv != null) {
							writeChartRecords(sqlDef.colNames, chartData, bw, currentDkv, chart_key);
							chartData = new HashMap<String, ArrayList<String>> ();
						}
					}
					currentDkv = dkv;

					for (int i = 0; i < sqlDef.colNames.size(); i++) {
						String col = sqlDef.colNames.get(i);
						if(!col.equals(chart_key)) {
							ArrayList<String> vList = chartData.get(col);
							if(vList == null) {
								vList = new ArrayList<String> ();
								chartData.put(col, vList);
							}
							vList.add(rs.getString(col));
							
						}
					}

				}

				// Write the records for the final key
				writeChartRecords(sqlDef.colNames, chartData, bw, currentDkv, chart_key);

				bw.flush();
				bw.close();
			} else if (sqlDef.hasGeom) { 	// CSV files with geotrace or geoshape elements have to be generated without using PSQL
				
			
				pstmtData = cResults.prepareStatement(sqlNoEscapes);
				
				if(rs != null) {
					rs.close();
				}
				log.info("####### Progressively getting data with geometry: " + pstmtData.toString());
				rs = pstmtData.executeQuery();

				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(f.getAbsoluteFile()), "UTF8"));

				// Write header		
				for (int i = 0; i < sqlDef.colNames.size(); i++) {
					String col = sqlDef.colNames.get(i);
					if(i > 0) {
						bw.write(",");
					}
					bw.write(col);
				}
				bw.newLine();

				// Write data
				while (rs.next()) {

					for (int i = 0; i < sqlDef.colNames.size(); i++) {
						String col = sqlDef.colNames.get(i);
						if(i > 0) {
							bw.write(",");
						}
						String val = rs.getString(col);
						if(val == null) {
							bw.write("");
						} else if(val.startsWith("LINESTRING") || val.startsWith("POLYGON")) {
							String ftVal = GeneralUtilityMethods.convertGeomToFieldTaskFormat(val);
							bw.write("\"");
							bw.write(ftVal);
							bw.write("\"");
						} else {
							bw.write("\"");
							bw.write(val);
							bw.write("\"");
						}
						
					}
					bw.newLine();
				}

				bw.flush();
				bw.close();
				
			} else {
				// Use PSQL to generate the file as it is faster
				int code = 0;
				pstmtData = cResults.prepareStatement(sql);
				
				String filePath = f.getAbsolutePath();
				int idx = filePath.indexOf(".csv");
				if(idx >= 0) {
					filePath = filePath.substring(0, idx);		// remove extension as it is added by the script
				}
				String scriptPath = basePath + "_bin" + File.separator + "getshape.sh";
				String[] cmd = { "/bin/sh", "-c",
						scriptPath + " results linked " + "\"" + pstmtData.toString() + "\" "
								+ filePath + " csvnozip" };
				log.info("Getting linked data: " + cmd[2]);
				Process proc = Runtime.getRuntime().exec(cmd);
				code = proc.waitFor();
				if(code > 0) {
					int len;
					if ((len = proc.getErrorStream().available()) > 0) {
						byte[] buf = new byte[len];
						proc.getErrorStream().read(buf);
						log.info("Command error:\t\"" + new String(buf) + "\"");
					}
				} else {
					int len;
					if ((len = proc.getInputStream().available()) > 0) {
						byte[] buf = new byte[len];
						proc.getInputStream().read(buf);
						log.info("Completed getshape process:\t\"" + new String(buf) + "\"");
					}
				}
			}
			status = true;

		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			lm.writeLog(sd, sId, userName, LogManager.ERROR, "Creating CSV file: " + e.getMessage(), 0, null);
			status = false;
		} finally {
			if (pstmtData != null) {try {pstmtData.close();} catch (Exception e) {}}
		}
		return status;		// True for success
	}
	
	/*
	 * Write a set of records for a single data key
	 */
	private void writeRecords(boolean non_unique_key, ArrayList<StringBuilder> nonUniqueRecords, BufferedWriter bw,
			String dkv) throws IOException {

		if (non_unique_key && nonUniqueRecords.size() > 0) {
			// Write the number of records
			bw.write(dkv);
			bw.write(",");
			bw.write(String.valueOf(nonUniqueRecords.size()));
			bw.newLine();
		}

		// Write each record
		for (int i = 0; i < nonUniqueRecords.size(); i++) {
			bw.write(dkv);
			if (non_unique_key) {
				bw.write("_");
				bw.write(String.valueOf(i + 1)); // To confirm with position(..) which starts at 1
			}
			bw.write(nonUniqueRecords.get(i).toString());
			bw.newLine();
		}
	}
	
	/*
	 * Write timeseries data
	 */
	private void writeChartRecords(ArrayList<String> cols, HashMap<String, ArrayList<String>> data, BufferedWriter bw,
			String dkv, String chart_key) throws IOException {

		bw.write(dkv);
		for(String col : cols) {
			if(!col.equals(chart_key)) {
				bw.write(",");
				ArrayList<String> vList = data.get(col);
				if(vList != null) {
					int idx = 0;
					for(String v : vList) {
						if(v != null) {
							if(idx++ > 0) {
								bw.write(":");
							}
							bw.write(v);
						}
					}
				}
			}
		}
		bw.newLine();
	}
	
	/*
	 * Return true if the file needs to be regenerated If regeneration is required
	 * then also increment the version of the linking form so that it will get the
	 * new version
	 */
	public boolean testForRegenerateFile(Connection sd, Connection cRel, int sId, String logicalFilePath, File currentPhysicalFile) throws SQLException, ApplicationException {

		boolean fileExists = currentPhysicalFile.exists();
		
		boolean regenerate = false;
		boolean tableExists = true;

		String sql = "select count (*) from linked_forms " 
				+ "where linked_s_id = ? " 
				+ "and linker_s_id = ? "
				+ "and link_file = ? ";
		PreparedStatement pstmt = null;

		String sqlInsert = "insert into linked_forms "
				+ "(Linked_s_id, linker_s_id, link_file, download_time) " 
				+ "values(?, ?, ?, now())";
		PreparedStatement pstmtInsert = null;

		try {

			int linked_sId = GeneralUtilityMethods.getSurveyId(sd, linked_sIdent);
			
			if(!GeneralUtilityMethods.inSameOrganisation(sd, sId, linked_sId)) {
				log.info("---------------------------- Authorisation exception");
				throw new ApplicationException("Cannot link to external survey: " + linked_sIdent + " as it is in a different organisation");
			}
			
			log.info("Set autocommit false");
			sd.setAutoCommit(false);
			// Get data on the link between the two surveys
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, linked_sId);
			pstmt.setInt(2, sId);
			pstmt.setString(3, logicalFilePath);
			log.info("Test for regen: " + pstmt.toString());

			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				int count = rs.getInt(1);
				if (count > 0) {
					regenerate = false;
					log.info("Regenerate is false");
				} else {

					String table = GeneralUtilityMethods.getMainResultsTable(sd, cRel, linked_sId);

					if (table != null) {
						regenerate = true;
						log.info("Need to regenerate");

						pstmtInsert = sd.prepareStatement(sqlInsert);
						
						// Create an entry in linked forms for all grouped surveys that this this survey links to
						String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, linked_sId );
						HashMap<Integer, Integer> groupSurveys = GeneralUtilityMethods.getGroupSurveys(sd, groupSurveyIdent);
						if(groupSurveys.size() > 0) {
							for(int gSId : groupSurveys.keySet()) {
								pstmtInsert.setInt(1, gSId);
								pstmtInsert.setInt(2, sId);
								pstmtInsert.setString(3, logicalFilePath);
								pstmtInsert.executeUpdate();
								log.info("Insert record: " + pstmtInsert.toString());
							}
						}
					} else {
						log.info("Table " + table + " not found. Probably no data has been submitted");
						tableExists = false;
						// Delete the file if it exists
						log.info("Deleting file -------- : " + currentPhysicalFile.getAbsolutePath());
						currentPhysicalFile.delete();
						
						fileExists = false;
					}

				}

			}
			sd.commit();
		} finally {
			try {sd.setAutoCommit(true);} catch(Exception e) {};
			if (pstmt != null) {	try {pstmt.close();} catch (Exception e) {}}
			if (pstmtInsert != null) {try {pstmtInsert.close();} catch (Exception e) {}}
		}

		if (tableExists && !fileExists) {
			regenerate = true; // Override regenerate if the file has been deleted
		}
		if(!tableExists) {
			regenerate = true; // Force creation of an empty file
		}

		log.info("Result of regenerate question is: " + regenerate);
		if(regenerate) {
			log.info("xoxoxoxoxoxoxo regenerate: " + logicalFilePath);
		}
		return regenerate;
	}
}
