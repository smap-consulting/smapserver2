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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Pulldata;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.SqlFrag;
import com.google.gson.Gson;
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
 * Manage creation of files
 */
public class ExternalFileManager {

	private static Logger log = Logger.getLogger(ExternalFileManager.class.getName());
	
	private static ResourceBundle localisation = null;
	LogManager lm = new LogManager(); // Application log

	public ExternalFileManager(ResourceBundle l) {
		localisation = l;
	}
	
	/*
	 * Class to return SQL
	 */
	class SqlDef {
		String sql;
		ArrayList<String> colNames;
		boolean hasRbacFilter = false;
		ArrayList<SqlFrag> rfArray = null;
	}

	/*
	 * Call this method when a linker survey, that is a survey that links to another
	 * survey changes. This will result in regenerate being called next time the
	 * survey is downloaded
	 */
	public void linkerChanged(Connection sd, int sId) throws SQLException {
		String sql = "delete from linked_forms where linker_s_id = ?;";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			log.info("Linker changed: " + pstmt.toString());
			pstmt.executeUpdate();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Linker changed", e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
				}
			}
			;
		}
	}

	/*
	 * Create a linked file
	 */
	public boolean createLinkedFile(Connection sd, Connection cRel, int oId, int sId, // The survey that contains the manifest item
			String filename, String filepath, String userName, String tz) throws Exception {

		boolean regenerate = false;
		
		log.info("createLinkedFile: " + filename);

		try {
			File f = new File(filepath + ".csv"); // file path does not include the extension because getshape.sh adds it
			SurveyTableManager stm = new SurveyTableManager(sd, cRel, localisation, oId, sId, filename, userName);  

			log.info("Test for regenerate of file: " + f.getAbsolutePath() + " File exists: " + f.exists());
			regenerate = stm.regenerateFile(sd, cRel,  sId, f);
			if(regenerate) {
				stm.regenerateCsvFile(cRel, f, sId, userName, filepath);
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			lm.writeLog(sd, sId, userName, LogManager.ERROR, "Creating CSV file: " + e.getMessage(), 0);
		} 

		return regenerate;
	}

	/*
	 * ******************************************************************* private
	 * methods
	 */
	/*
	 * Get the SQL to retrieve dynamic CSV data TODO replace this with the query
	 * generator from SDAL
	 */
	/*
	private SqlDef getSql(Connection sd, int sId, ArrayList<String> qnames, boolean linked_s_pd, String data_key,
			String chart_key) throws Exception {

		StringBuffer sql = new StringBuffer("select ");
		if(chart_key == null) {		// Time series data should not be made distinct
			sql.append("distinct ");
		}
		StringBuffer where = new StringBuffer("");
		StringBuffer tabs = new StringBuffer("");
		StringBuffer order_cols = new StringBuffer("");
		String linked_s_pd_sel = null;
		SqlDef sqlDef = new SqlDef();
		ArrayList<String> colNames = new ArrayList<>();
		ArrayList<String> subTables = new ArrayList<>();
		HashMap<Integer, Integer> forms = new HashMap<>();
		Form topForm = GeneralUtilityMethods.getTopLevelForm(sd, sId);
		String dateColumn = null;

		String sqlGetTable = "select f_id, table_name from form " 
				+ "where s_id = ? " 
				+ "and parentform = ?";
		PreparedStatement pstmtGetTable = null;

		try {
			int fId;

			// 1. Get the columns in the group
			SurveyManager sm = new SurveyManager(localisation, "UTC");					
			String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
			HashMap<String, QuestionForm> refQuestionMap = sm.getGroupQuestionsMap(sd, groupSurveyIdent, null, false);
			
			boolean first = true;
			
			if (linked_s_pd) {
				linked_s_pd_sel = GeneralUtilityMethods.convertAllxlsNamesToQuery(data_key, sId, sd);
				sql.append(linked_s_pd_sel);
				sql.append(" as _data_key");
				first = false;
			}

			forms.put(topForm.id, topForm.id); // Always add top level form
			for (int i = 0; i < qnames.size(); i++) {
				String name = qnames.get(i);
				if (name.equals("_data_key") || name.equals("_count")) {
					continue; // Generated not extracted
				}
				String colName = null;
				String qType = null;
				
				QuestionForm qf = refQuestionMap.get(name);
				if (qf != null && qf.published) {
					colName = qf.columnName;
					fId = qf.f_id;
					qType = qf.qType;				
				} else if(SmapServerMeta.isServerReferenceMeta(name)) {
					colName = name; // For columns that are not questions such as _hrk, _device
					fId = topForm.id;
				} else {
					continue; // Name not found
				}
				colNames.add(colName);
				forms.put(fId, fId);
				if(qType != null && (qType.equals("date") || qType.equals("dateTime"))) {
					dateColumn = colName;
				}

				if (!first) {
					sql.append(",");
					order_cols.append(",");
				}
				sql.append(colName);
				sql.append(" as ");
				sql.append(name);
				first = false;

				order_cols.append(colName);
			}

			// 2. Add the tables
			pstmtGetTable = sd.prepareStatement(sqlGetTable);
			pstmtGetTable.setInt(1, sId);
			getTables(pstmtGetTable, 0, null, tabs, where, forms, subTables);

			// 2.5 Add the primary keys of sub tables so they can be sorted on
			if (linked_s_pd && subTables.size() > 0) {
				for (String subTable : subTables) {
					sql.append(",");
					sql.append(subTable);
					sql.append(".prikey");
				}
			}

			sql.append(" from ");
			sql.append(tabs);

			// 3. Add the where clause
			if (where.length() > 0) {
				sql.append(" where ");
				sql.append(where);
			}

			// 4. Add the RBAC/Row filter
			// Add RBAC/Role Row Filter
			sqlDef.rfArray = null;
			sqlDef.hasRbacFilter = false;
			

			// If this is a pulldata linked file then order the data by _data_key and then
			// the primary keys of sub forms
			if(chart_key != null) {
				sql.append(" order by ").append(chart_key);
				if(dateColumn != null) {
					sql.append(",").append(dateColumn);
				}
				sql.append(" asc");
			} else if (linked_s_pd) {
				sql.append(" order by _data_key");
				if (subTables.size() > 0) {
					for (String subTable : subTables) {
						sql.append(",");
						sql.append(subTable);
						sql.append(".prikey asc");
					}
				} else {
					sql.append(" asc");
				}
			} else {
				// order by the columns
				sql.append(" order by ");
				sql.append(order_cols);
				sql.append(" asc");
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw e;
		} finally {			
			if (pstmtGetTable != null) try {pstmtGetTable.close();} catch (Exception e) {}
		}

		sqlDef.sql = sql.toString();
		sqlDef.colNames = colNames;
		return sqlDef;
	}
*/
	
	/*
	 * Get table details
	 */
	/*
	private void getTables(PreparedStatement pstmt, int parentId, String parentTable, StringBuffer tabs,
			StringBuffer where, HashMap<Integer, Integer> forms, ArrayList<String> subTables) throws SQLException {

		ArrayList<Integer> parents = new ArrayList<>();
		ArrayList<String> parentTables = new ArrayList<>();

		pstmt.setInt(2, parentId);
		log.info("Get tables: " + pstmt.toString());
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			int fId = rs.getInt(1);
			String table = rs.getString(2);

			/*
			 * Ignore forms that where no questions have been asked for
			 
			if (forms.get(fId) != null) {

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
			getTables(pstmt, fId, table, tabs, where, forms, subTables);
		}

	}
	*/
}
