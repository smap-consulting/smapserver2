package org.smap.sdal.managers;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import org.smap.sdal.Utilities.CSVParser;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.CsvTable;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.CsvHeader;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.LanguageItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.SelectChoice;
import org.smap.sdal.model.SqlFrag;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.opencsv.CSVReader;

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
 * Manage the table that stores details on the forwarding of data onto other
 * systems
 */
public class CsvTableManager {

	private static Logger log = Logger.getLogger(CsvTableManager.class.getName());

	
	Connection sd = null;
	ResourceBundle localisation = null;
	private int tableId = 0;
	private String tableName = null;
	private String schema = "csv";
	private String fullTableName = null;
	private ArrayList<CsvHeader> headers = null;
	CSVParser parser = null;
	CSVReader csvReader = null;
	
	private final String PKCOL = "_id";
	private final String ACOL = "_action";
	private final String TSCOL = "_changed_ts";
	private final String SORTCOL = "sortby";
	
	private final int ADD_ENTRY = 1;
	private final int UPDATE_ENTRY = 2;
	private final int DELETE_ENTRY = 3;
	
	private String sqlGetCsvTable = "select id, headers from csvtable where o_id = ? and s_id = ? and filename = ?";
	
	/*
	 * Constructor to create a table to hold the CSV data if it does not already exist
	 */
	public CsvTableManager(Connection sd, ResourceBundle l, int oId, int sId, String fileName)
			throws Exception {
		
		this.sd = sd;
		this.localisation = l;
		parser = new CSVParser(localisation);
		
		Type headersType = new TypeToken<ArrayList<CsvHeader>>() {}.getType();
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		
		PreparedStatement pstmtGetCsvTable = null;
		
		String sqlInsertCsvTable = "insert into csvtable (id, o_id, s_id, filename, headers, ts_initialised) "
				+ "values(nextval('csv_seq'), ?, ?, ?, ?, now())";
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
				String hString = rs.getString(2);
				if(hString != null) {
					headers = gson.fromJson(hString, headersType);
				}
			} else {
				pstmtInsertCsvTable = sd.prepareStatement(sqlInsertCsvTable, Statement.RETURN_GENERATED_KEYS);
				pstmtInsertCsvTable.setInt(1, oId);
				pstmtInsertCsvTable.setInt(2, sId);
				pstmtInsertCsvTable.setString(3, fileName);
				pstmtInsertCsvTable.setString(4, null);
				log.info("Create a new csv file entry (Table Manager): " + pstmtInsertCsvTable.toString());
				pstmtInsertCsvTable.executeUpdate();
				ResultSet rsKeys = pstmtInsertCsvTable.getGeneratedKeys();
				if(rsKeys.next()) {
					tableId = rsKeys.getInt(1);
				} else {
					throw new Exception("Failed to create CSV Table entry");
				}
			}
			tableName = "csv" + tableId;
			fullTableName = "csv." + tableName;
		} finally {
			try {pstmtGetCsvTable.close();} catch(Exception e) {}
			try {pstmtInsertCsvTable.close();} catch(Exception e) {}
		}
	}

	/*
	 * Constructor that does not attempt to connect to a table or create a new table
	 */
	public CsvTableManager(Connection sd, ResourceBundle l)
			throws Exception {
		
		this.sd = sd;
		this.localisation = l;
		parser = new CSVParser(localisation);
		
	}
	
	/*
	 * Get a list of the CSV tables
	 */
	public ArrayList<CsvTable> getTables(int oId, int sId) throws Exception{
		ArrayList<CsvTable> tables = new ArrayList<> ();
		String sqlSelect = "select id, filename, headers from csvtable where o_id = ?";
		String sqlsId = " and s_id = ?";
		String sqlNosId = " and not survey";
		String sqlOrder = " order by filename asc";
		
		String sql = sqlSelect + (sId > 0 ? sqlsId : sqlNosId) + sqlOrder;
		PreparedStatement pstmt = null;;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  oId);
			if(sId > 0) {
				pstmt.setInt(2, sId);
			}
			
			Type headersType = new TypeToken<ArrayList<CsvHeader>>() {}.getType();
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
			
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				CsvTable t = new CsvTable();
				t.id = rs.getInt(1);
				t.filename = rs.getString(2);
				t.headers = gson.fromJson(rs.getString(3), headersType);
				tables.add(t);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		
		return tables;
	}
	/*
	 * Update the table with data from the file
	 */
	public void updateTable(File newFile) throws Exception {

		PreparedStatement pstmtCreateSeq = null;
		PreparedStatement pstmtCreateTable = null;
		PreparedStatement pstmtAlterColumn = null;
		
		try {
			// Open the files
			FileReader readerNew = new FileReader(newFile);
			csvReader = new CSVReader(readerNew);
	
			// Get the column headings from the new file
			int maxCols = 100;
			String cols[] = csvReader.readNext();
			if(cols.length > maxCols) {
				String msg = localisation.getString("msg_too_many_cols");
				msg = msg.replace("%s1", String.valueOf(cols.length));
				msg = msg.replace("%s2", String.valueOf(maxCols));
				throw new Exception(msg);
			}
			headers = new ArrayList<CsvHeader> ();
			for(String n : cols) {
				if(n != null && !n.isEmpty()) {
					headers.add(new CsvHeader(GeneralUtilityMethods.removeBOM(n), GeneralUtilityMethods.cleanNameNoRand(n)));
				}
			}
			
			/*
			 * 1. Create or modify the table
			 */
			boolean tableExists = GeneralUtilityMethods.tableExistsInSchema(sd, tableName, schema);
			if(!tableExists) {
				// Create the key sequence
				String sequenceName = fullTableName + "_seq";
				StringBuffer sqlCreate = new StringBuffer("create sequence ").append(sequenceName).append(" start 1");
				pstmtCreateSeq = sd.prepareStatement(sqlCreate.toString());
				try { 
					pstmtCreateSeq.executeUpdate();
				} catch(Exception e) {
					log.info(e.getMessage());  // Ignore error
				}
				
						// Create the table
				sqlCreate = new StringBuffer("create table ").append(fullTableName).append("(");
				sqlCreate.append(PKCOL).append(" integer default nextval('").append(sequenceName).append("')");
				sqlCreate.append(",").append(ACOL + " integer");
				sqlCreate.append(",").append(TSCOL + " TIMESTAMP WITH TIME ZONE");
				for(CsvHeader c : headers) {
					sqlCreate.append(",").append(c.tName).append(" text");
				}				
				sqlCreate.append(")");
				pstmtCreateTable = sd.prepareStatement(sqlCreate.toString());
				log.info("Create table: " + pstmtCreateTable.toString());
				pstmtCreateTable.executeUpdate();
				
			} else {
				
				// Add any new columns, old unused columns will be deleted after loading of data as 
				//  they may be needed in order to identify rows to delete
				for(CsvHeader c : headers) {
					if(!GeneralUtilityMethods.hasColumnInSchema(sd, tableName, c.tName, schema)) {
						String sqlAddColumn = "alter table " + fullTableName + " add column " + c.tName + " text";
						if(pstmtAlterColumn != null) {try{pstmtAlterColumn.close();} catch(Exception e) {}}
						pstmtAlterColumn = sd.prepareStatement(sqlAddColumn);
						log.info("alter: " + pstmtAlterColumn.toString());
						pstmtAlterColumn.executeUpdate();
					}
				}
			}
			updateHeaders();		// Store the csv headers information with the match between file name and table name
			
			/*
			 * 2. Read in the CSV file 
			 * Get the differences between this load and the previous load
			 * For performance check to see if there are more than 100 deletes in a delta if so replace the entire file
			 */
			ArrayList<String[]> listNew = new ArrayList<> ();
		
			String[] newLine = csvReader.readNext();
			while (newLine != null) {
				listNew.add(newLine);
				newLine = csvReader.readNext();
			}
			
			/*
			 * 3. Upload the data
			 */
			truncate();
			insert(listNew, headers.size(), newFile.getName());
			updateInitialisationTimetamp();	
			
			/*
			 * 4. Delete any columns that are no longer used
			 */
			ArrayList<String> tableCols = GeneralUtilityMethods.getColumnsInSchema(sd, tableName, schema);
			if(tableCols.size() != headers.size() + 1) {		// If the number of columns match then table cannot have any columns that need deleting
				log.info("Checking for columns in csv file that need to be deleted");
				for(String tableCol : tableCols) {
					if(tableCol.equals(PKCOL) || tableCol.equals(ACOL) || tableCol.equals(TSCOL)) {
						continue;
					}
					boolean missing = true;
					for(CsvHeader h : headers) {
						if(h.tName.equals(tableCol)) {
							missing = false;
							break;
						}
					}
					if(missing) {
						String sqlAddColumn = "alter table " + fullTableName + " drop column " + tableCol;
						if(pstmtAlterColumn != null) {try{pstmtAlterColumn.close();} catch(Exception e) {}}
						pstmtAlterColumn = sd.prepareStatement(sqlAddColumn);
						pstmtAlterColumn.executeUpdate();
					}
				}
			}
			
		} finally {
			if(pstmtCreateSeq != null) {try{pstmtCreateSeq.close();} catch(Exception e) {}}
			if(pstmtCreateTable != null) {try{pstmtCreateTable.close();} catch(Exception e) {}}
			if(pstmtAlterColumn != null) {try{pstmtAlterColumn.close();} catch(Exception e) {}}
			if(csvReader != null) {try{csvReader.close();}catch(Exception e) {}}
		}
		
	}
	
	/*
	 * Get choices from the table
	 */
	public ArrayList<Option> getChoices(int oId, int sId, String fileName, String ovalue, 
			ArrayList<LanguageItem> items,
			ArrayList<String> matches,
			ArrayList<KeyValueSimp> wfFilters) throws SQLException, ApplicationException {
		
		ArrayList<Option> choices = null;
		
		PreparedStatement pstmtGetCsvTable = null;	
		try {
			pstmtGetCsvTable = sd.prepareStatement(sqlGetCsvTable);
			pstmtGetCsvTable.setInt(1, oId);
			pstmtGetCsvTable.setInt(2, sId);
			pstmtGetCsvTable.setString(3, fileName);
			log.info("Getting csv file name (survey lvl): " + pstmtGetCsvTable.toString());
			ResultSet rs = pstmtGetCsvTable.executeQuery();
			if(rs.next()) {
				choices = readChoicesFromTable(rs.getInt(1), ovalue, items, matches, fileName, wfFilters);				
			} else {
				pstmtGetCsvTable.setInt(2, 0);		// Try organisational level
				log.info("Getting csv file name (organisational): " + pstmtGetCsvTable.toString());
				ResultSet rsx = pstmtGetCsvTable.executeQuery();
				if(rsx.next()) {
					choices = readChoicesFromTable(rsx.getInt(1), ovalue, items, matches, fileName, wfFilters);	
				} else {
					log.info("CSV file not found: " + fileName);
				}
				
			}
		} finally {
			try {pstmtGetCsvTable.close();} catch(Exception e) {}
		}
		// Don't return a null list
		if(choices == null) {
			choices = new ArrayList<Option> ();
		}
		
		return choices;
	}
	
	/*
	 * Look up a value
	 */
	public ArrayList<HashMap<String, String>> lookup(int oId, int sId, String fileName, String key_column, 
			String key_value, String expression, String tz, String selection, ArrayList<String> arguments) throws SQLException, ApplicationException {
		
		ArrayList<HashMap<String, String>> records = null;
		
		PreparedStatement pstmtGetCsvTable = null;	
		try {
			pstmtGetCsvTable = sd.prepareStatement(sqlGetCsvTable);
			pstmtGetCsvTable.setInt(1, oId);
			pstmtGetCsvTable.setInt(2, sId);
			pstmtGetCsvTable.setString(3, fileName);
			log.info("Getting csv file name for lookup value: (survey level) " + pstmtGetCsvTable.toString());
			ResultSet rs = pstmtGetCsvTable.executeQuery();
			if(rs.next()) {
				records = readRecordsFromTable(rs.getInt(1), rs.getString(2), key_column, key_value, fileName, expression, tz, selection, arguments);				
			} else {
				pstmtGetCsvTable.setInt(2, 0);		// Try organisational level
				log.info("Getting csv file name fo lookup value: (organisation level) " + pstmtGetCsvTable.toString());
				ResultSet rsx = pstmtGetCsvTable.executeQuery();
				if(rsx.next()) {
					records = readRecordsFromTable(rsx.getInt(1), rsx.getString(2), key_column, key_value, fileName, expression, tz, selection, arguments);	
				} else {
					throw new ApplicationException(localisation.getString("mf_mf") + " " + fileName);
				}
			}
		} finally {
			try {pstmtGetCsvTable.close();} catch(Exception e) {}
		}
		
		return records;
	}
	
	/*
	 * Get an array of choices
	 */
	public ArrayList<SelectChoice> lookupChoices(int oId, int sId, String fileName, 
			String value_column, 
			String label_columns,
			HashMap<String, String> mlLabelColumns,
			String selection,
			ArrayList<String> arguments,
			SqlFrag expressionFrag) throws SQLException, ApplicationException {
		
		ArrayList<SelectChoice> choices = null;

		PreparedStatement pstmtGetCsvTable = null;	
		try {
			pstmtGetCsvTable = sd.prepareStatement(sqlGetCsvTable);
			pstmtGetCsvTable.setInt(1, oId);
			pstmtGetCsvTable.setInt(2, sId);
			pstmtGetCsvTable.setString(3, fileName);
			log.info("Getting csv file name: (survey)" + pstmtGetCsvTable.toString());
			ResultSet rs = pstmtGetCsvTable.executeQuery();
			if(rs.next()) {
				if(mlLabelColumns != null) {
					choices = mlReadChoicesFromTable(rs.getInt(1), rs.getString(2), value_column, 
							mlLabelColumns, fileName,
							selection, arguments, expressionFrag);	
				} else {
					choices = readChoicesFromTable(rs.getInt(1), rs.getString(2), value_column, label_columns, 
							fileName,
							selection, arguments, expressionFrag);	
				}
			} else {
				pstmtGetCsvTable.setInt(2, 0);		// Try organisational level
				log.info("Getting csv file name: (organisation)" + pstmtGetCsvTable.toString());
				ResultSet rsx = pstmtGetCsvTable.executeQuery();
				if(rsx.next()) {
					if(mlLabelColumns != null) {
						choices= mlReadChoicesFromTable(rsx.getInt(1), rsx.getString(2), 
								value_column, mlLabelColumns, fileName,
								selection, arguments, expressionFrag);	
					} else {
						choices= readChoicesFromTable(rsx.getInt(1), rsx.getString(2), 
								value_column, label_columns, fileName,
								selection, arguments, expressionFrag);	
					}
				}				
			}
		} finally {
			try {pstmtGetCsvTable.close();} catch(Exception e) {}
		}
		
		return choices;
	}
	
	
	/*
	 * Delete csv tables
	 */
	public void delete(int oId, int sId, String fileName) throws SQLException {
		
		String sqlFile = "select id from csvtable where o_id = ? and s_id = ? and filename = ?";
		String sqlSurvey = "select id from csvtable where o_id = ? and s_id = ?";
		String sqlOrg = "select id from csvtable where o_id = ?";
		PreparedStatement pstmt = null;
		
		PreparedStatement pstmtDrop = null;
		
		String sqlDelete = "delete from csvtable where id = ?";
		PreparedStatement pstmtDelete = null;
				
		try {
			pstmtDelete = sd.prepareStatement(sqlDelete);
			
			if(fileName != null) {
				pstmt = sd.prepareStatement(sqlFile);	// If the filename is specified then just delete that
				pstmt.setInt(1, oId);
				pstmt.setInt(2,  sId);
				pstmt.setString(3, fileName);
			} else if(sId > 0) {
				pstmt = sd.prepareStatement(sqlSurvey);	// If the survey id is specified then just resources for that survey
				pstmt.setInt(1, oId);
				pstmt.setInt(2,  sId);
			} else if(oId > 0) {
				pstmt = sd.prepareStatement(sqlOrg);	// If the organisation id is specified then just resources for that organisation
				pstmt.setInt(1, oId);
			}
			
			log.info("Get shared resources to delete: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				int id = rs.getInt(1);
				String sqlDrop = "drop table csv.csv" + id;
				String sqlDropSeq = "drop sequence if exists csv.csv" + id + "_seq cascade";
				
				if(pstmtDrop != null) {try {pstmtDrop.close();} catch(Exception e) {}}
				pstmtDrop = sd.prepareStatement(sqlDrop);
				log.info("Dropping resource table: "  + pstmtDrop.toString());
				pstmtDrop.executeUpdate();
				
				if(pstmtDrop != null) {try {pstmtDrop.close();} catch(Exception e) {}}
				pstmtDrop = sd.prepareStatement(sqlDropSeq);
				log.info("Dropping resource sequence: "  + pstmtDrop.toString());
				pstmtDrop.executeUpdate();
				
				pstmtDelete.setInt(1, id);
				pstmtDelete.executeUpdate();
			}
		} catch(Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			try {pstmt.close();} catch(Exception e) {}
			try {pstmtDrop.close();} catch(Exception e) {}
			try {pstmtDelete.close();} catch(Exception e) {}
		}
	}
	
	/*
	 * Read the choices out of a file - CSV files are now stored in tables
	 */
	private ArrayList<Option> readChoicesFromTable(int tableId, String ovalue, ArrayList<LanguageItem> items,
			ArrayList<String> matches, String filename, ArrayList<KeyValueSimp> wfFilterColumns) throws SQLException, ApplicationException {
			
		ArrayList<Option> choices = new ArrayList<Option> ();
		
		String table = "csv.csv" + tableId;
		PreparedStatement pstmt = null;
		try {
			String cValue = GeneralUtilityMethods.cleanNameNoRand(ovalue);
			StringBuffer sql = new StringBuffer("select ");
			sql.append(cValue);
			for(LanguageItem item : items) {
				if(item.text.contains(",")) {
					String[] comp = item.text.split(",");
					for(int i = 0; i < comp.length; i++) {
						sql.append(",").append(GeneralUtilityMethods.cleanNameNoRand(comp[i]));
					}
				} else {
					sql.append(",").append(GeneralUtilityMethods.cleanNameNoRand(item.text));
				}
			}
			// Get filter values for webforms
			if(wfFilterColumns != null) {
				for(KeyValueSimp fc : wfFilterColumns) {
					sql.append(",").append(GeneralUtilityMethods.cleanNameNoRand(fc.k));
				}
			}
			sql.append(" from ").append(table);
			sql.append(" where ");
			// Eliminate deleted entries
			sql.append("not _action=").append(DELETE_ENTRY);
			if(matches != null && matches.size() > 0) {
				sql.append(" and ").append(cValue).append(" in (");
				int idx = 0;
				for(int i = 0; i < matches.size(); i++) {
					if(idx++ > 0) {
						sql.append(", ");
					}					
					sql.append("?");
				}
				sql.append(")");
			}
			
			if(GeneralUtilityMethods.hasColumn(sd, "csv" + tableId, SORTCOL)) {
				sql.append(" order by ").append(SORTCOL).append(" asc");
			} else {
				sql.append(" order by ").append(PKCOL).append(" asc");
			}
			
			pstmt = sd.prepareStatement(sql.toString());		
			if(matches != null && matches.size() > 0) {
				int idx = 1;
				for(String match : matches) {									
					pstmt.setString(idx++, match);;
				}
			}
			log.info("Get CSV values: " + pstmt.toString());
			ResultSet rsx = pstmt.executeQuery();
			HashMap<String, String> choicesLoaded = new HashMap<String, String> ();		// Eliminate duplicates
			
			while(rsx.next()) {
				
				StringBuffer uniqueChoice = new StringBuffer("");
				
				int idx = 1;
				Option o = new Option();
				o.value = rsx.getString(idx++);
				uniqueChoice.append(o.value);
				
				o.labels = new ArrayList<Label> ();
				o.externalLabel = items;
				o.externalFile = true;
				for(LanguageItem item : items) {
					Label l = new Label();
					if(item.text.contains(",")) {
						String[] comp = item.text.split(",");
						l.text = "";
						for(int i = 0; i < comp.length; i++) {
							if(i > 0) {
								l.text += ", ";
							}
							l.text += rsx.getString(idx++);
						}
					} else {
						l.text = rsx.getString(idx++);
					}
					o.labels.add(l);					
				}
					
				if(wfFilterColumns != null) {
					o.cascade_filters = new HashMap<>();
					for(KeyValueSimp fc : wfFilterColumns) {					
						String fv = rsx.getString(idx++);
						o.cascade_filters.put(fc.k, fv); 
						uniqueChoice.append(":::").append(fv);
					}
				}
				
				if(choicesLoaded.get(uniqueChoice.toString()) == null) {
					choices.add(o);
					choicesLoaded.put(uniqueChoice.toString(), "x");
				}
			}	
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw new ApplicationException("Error getting choices from csv file: " + filename + " " + e.getMessage());
		} finally {
			try {pstmt.close();} catch(Exception e) {}
		}
		return choices;
	}
	
	/*
	 * Read data records from a csv table
	 */
	private ArrayList<HashMap<String, String>> readRecordsFromTable(int tableId, String sHeaders, String key_column, String key_value,
			String filename, String expression, String tz, String selection, ArrayList<String> arguments) throws SQLException, ApplicationException {
			
		ArrayList<HashMap<String, String>> records = new ArrayList<> ();
		
		Type headersType = new TypeToken<ArrayList<CsvHeader>>() {}.getType();
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		ArrayList<CsvHeader> headers = gson.fromJson(sHeaders, headersType);
		
		String table = "csv.csv" + tableId;
		PreparedStatement pstmt = null;
		try {
			
			StringBuilder sql = new StringBuilder("select distinct ");
			StringBuilder columns = new StringBuilder("");
			String tKeyColumn = null;
			boolean first = true;
			for(CsvHeader item : headers) {
				if(!first) {
					columns.append(",");
				}
				first = false;
				columns.append(item.tName);
				if(key_column != null && item.fName.equals(key_column)) {
					tKeyColumn = item.tName;
				}
			}
			
			sql.append(columns).append(" from ").append(table);
			
			SqlFrag expressionFrag = null;
			if(expression != null) {
				// Convert #{qname} syntax to ${qname} syntax - also remove any enclosing single quotes
				expression = expression.replace("#{", "${");
				expression = expression.replace("\'${", "${");
				expression = expression.replace("}\'", "}");
				expressionFrag = new SqlFrag();
				log.info("Lookup with expression: " + expression);
				expressionFrag.addSqlFragment(expression, false, localisation, 0);
				sql.append(" where ( ").append(expressionFrag.sql).append(")");
			} else if(tKeyColumn == null) {
				throw new ApplicationException("Column " + key_column + " not found in table " + table);
			} else {
				sql.append(" where ").append(selection);
			}
			
			pstmt = sd.prepareStatement(sql.toString());
			int paramCount = 1;
			if(expression != null) {
				paramCount = GeneralUtilityMethods.setFragParams(pstmt, expressionFrag, paramCount, tz);
			} else {
				for(String arg : arguments) {
					pstmt.setString(paramCount++, arg);
				}
			}
			log.info("Get CSV lookup values: " + pstmt.toString());
			ResultSet rsx = pstmt.executeQuery();
			
			while(rsx.next()) {
				HashMap<String, String> record = new HashMap<> ();
				for(CsvHeader item : headers) {
					record.put(item.fName, rsx.getString(item.tName));
				}
				records.add(record);			
			}	
		} catch (Exception e) {
			String s = pstmt == null ? "" : pstmt.toString();
			log.log(Level.SEVERE, e.getMessage() + " : " + s, e);
			throw new ApplicationException(localisation.getString("c_error") + " : " + filename + " " 
					+ e.getMessage() + " : " + s);
		} finally {
			try {pstmt.close();} catch(Exception e) {}
		}
		return records;
	}
	
	/*
	 * Do a lookup of choices - online dynamic request
	 */
	private ArrayList<SelectChoice> readChoicesFromTable(int tableId, String sHeaders, String value_column, 
			String label_columns,
			String filename,
			String selection,
			ArrayList<String> arguments,
			SqlFrag expressionFrag) throws SQLException, ApplicationException {
			
		ArrayList<SelectChoice> choices = new ArrayList<> ();
		
	
		ArrayList<String> labelColumnArray = new ArrayList<String> ();
		String [] a = label_columns.split(",");
		for(String v : a) {
			v = v.trim();
			labelColumnArray.add(v);
		}
		
		Type headersType = new TypeToken<ArrayList<CsvHeader>>() {}.getType();
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		ArrayList<CsvHeader> headers = gson.fromJson(sHeaders, headersType);
		
		String table = "csv.csv" + tableId;
		HashMap<String, String> choiceMap = new HashMap<>();
		boolean hasSortBy = false;
		PreparedStatement pstmt = null;
		try {
			
			StringBuffer sql = new StringBuffer("select ");
			boolean foundValue = false;
			int foundLabel = 0;
			
			// Map value column to table column and flag if there is a sortby column
			for(CsvHeader item : headers) {
				if(item.fName.equals(value_column)) {
					sql.append(item.tName);
					foundValue = true;
				}
				if(item.fName.equals("sortby")) {
					hasSortBy = true;
				}
				if(hasSortBy && foundValue) {
					break;	// no need to go on
				}
			}
			
			// Map label columnns to table columns
			sql.append(",");
			boolean first = true;
			for(String label : labelColumnArray) {
				for(CsvHeader item : headers) {
					if(item.fName.equals(label)) {
						if(!first) {
							sql.append(" || ',' || ");
						}
						first = false;
						sql.append(item.tName);				
						foundLabel++;
						break;
					}
				}
			}
			sql.append(" as __label ");
			
			
			if(!foundValue) {
				throw new ApplicationException("Column " + value_column + " not found in table " + table);
			} else if(foundLabel != labelColumnArray.size()) {
				throw new ApplicationException("Columns " + label_columns + " not found in table " + table);
			}
			
			
			// Check the where columns
			/*
			for(String col : whereColumns) {
				boolean foundCol = false;
				for(CsvHeader item : headers) {
					if(item.fName.equals(col)) {
						foundCol = true;
						break;
					}
				}
				if(!foundCol) {
					throw new ApplicationException("Column " + col + " not found in table " + table);
				}
			}
			*/
			
			sql.append(" from ").append(table);
			if(selection != null) {
				sql.append(" where ").append(selection);
			}
			
			if(hasSortBy) {
				sql.append(" order by sortby::real asc");
			}
				
			pstmt = sd.prepareStatement(sql.toString());	
			int paramIndex = 1;
			if(expressionFrag != null) {
				paramIndex = GeneralUtilityMethods.setFragParams(pstmt, expressionFrag, paramIndex, "UTC");
			} else if(arguments != null) {
				for(String arg : arguments) {
					pstmt.setString(paramIndex++, arg);
				}
			}
			log.info("Get CSV choices: " + pstmt.toString());
			ResultSet rsx = pstmt.executeQuery();
			
			int idx = 0;
			while(rsx.next()) {
				String value = rsx.getString(value_column);
				if(value != null) {
					value = value.trim();
					if(choiceMap.get(value) == null) {		// Only add unique values
						choices.add(new SelectChoice(value, rsx.getString("__label"), idx++));
						choiceMap.put(value, value);
						log.info("#####: " + " add choice: " + value + " : " + rsx.getString("__label"));
					}
				}			
			}	
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw new ApplicationException("Error getting choices from csv file: " + filename + " " + e.getMessage());
		} finally {
			try {pstmt.close();} catch(Exception e) {}
		}
		return choices;
	}
	
	/*
	 * Do a multi language lookup of choices - online dynamic request
	 */
	private ArrayList<SelectChoice> mlReadChoicesFromTable(int tableId, String sHeaders, String value_column, 
			HashMap<String, String> mlLabelColumns,
			String filename,
			String selection,
			ArrayList<String> arguments,
			SqlFrag expressionFrag) throws SQLException, ApplicationException {
			
		ArrayList<SelectChoice> choices = new ArrayList<> ();
		
		
		Type headersType = new TypeToken<ArrayList<CsvHeader>>() {}.getType();
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		ArrayList<CsvHeader> headers = gson.fromJson(sHeaders, headersType);
		
		String table = "csv.csv" + tableId;
		HashMap<String, String> choiceMap = new HashMap<>();
		boolean hasSortBy = false;
		PreparedStatement pstmt = null;
		try {
			
			StringBuffer sql = new StringBuffer("select ");
			boolean foundValue = false;
			
			// Map value column to table column and flag if there is a sortby column
			for(CsvHeader item : headers) {
				if(item.fName.equals(value_column)) {
					sql.append(item.tName);
					foundValue = true;
				}
				if(item.fName.equals("sortby")) {
					hasSortBy = true;
				}
				if(hasSortBy && foundValue) {
					break;	// no need to go on
				}
			}
			
			// Map label columnns to table columns
			sql.append(",");
			boolean firstLang = true;
			for(String lang : mlLabelColumns.keySet()) {
				if(!firstLang) {
					sql.append(",");
				}
				boolean first = true;
				int foundLabel = 0;
				String label_columns = mlLabelColumns.get(lang).trim();
				ArrayList<String> labelColumnArray = new ArrayList<String> ();
				String [] a = label_columns.split(",");
				for(String v : a) {
					v = v.trim();
					labelColumnArray.add(v);
				}
				for(String label : labelColumnArray) {
					for(CsvHeader item : headers) {
						if(item.fName.equals(label)) {
							if(!first) {
								sql.append(" || ',' || ");
							}
							first = false;
							sql.append(item.tName);				
							foundLabel++;
							break;
						}
					}
				}
				sql.append(" as __label_").append(lang).append(" ");
				
				if(!foundValue) {
					throw new ApplicationException("Column " + value_column + " not found in table " + table);
				} else if(foundLabel != labelColumnArray.size()) {
					throw new ApplicationException("Columns " + label_columns + " not found in table " + table);
				}
				firstLang = false;
			}
			
			// Check the where columns
			/*
			for(String col : whereColumns) {
				boolean foundCol = false;
				for(CsvHeader item : headers) {
					if(item.fName.equals(col)) {
						foundCol = true;
						break;
					}
				}
				if(!foundCol) {
					throw new ApplicationException("Column " + col + " not found in table " + table);
				}
			}
			*/
			
			sql.append(" from ").append(table);
			if(selection != null) {
				sql.append(" where ").append(selection);
			}
			
			if(hasSortBy) {
				sql.append(" order by sortby::real asc");
			}
				
			pstmt = sd.prepareStatement(sql.toString());	
			int paramIndex = 1;
			if(expressionFrag != null) {
				paramIndex = GeneralUtilityMethods.setFragParams(pstmt, expressionFrag, paramIndex, "UTC");
			} else if(arguments != null) {
				for(String arg : arguments) {
					pstmt.setString(paramIndex++, arg);
				}
			}

			log.info("Get CSV choices (multi language): " + pstmt.toString());
			ResultSet rsx = pstmt.executeQuery();
			
			int idx = 0;
			while(rsx.next()) {
				String value = rsx.getString(value_column);
				if(value != null) {
					value = value.trim();
					if(choiceMap.get(value) == null) {		// Only add unique values
						SelectChoice choice = new SelectChoice();
						choice.index = idx++;
						choice.value = value;
						choice.mlChoices = new HashMap<>();
						for(String lang : mlLabelColumns.keySet()) {
							choice.mlChoices.put(lang, rsx.getString("__label_" + lang));
						}
						choices.add(choice)
;						choiceMap.put(value, value);
					}
				}			
			}	
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw new ApplicationException("Error getting choices from csv file: " + filename + " " + e.getMessage());
		} finally {
			try {pstmt.close();} catch(Exception e) {}
		}
		return choices;
	}
	
	/*
	 * Save the headers so that when generating csv output we can use the original file name header
	 */
	private void updateInitialisationTimetamp() throws SQLException {
		String sql = "Update csvtable set ts_initialised = now() where id = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  tableId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Update the initialisation timestamp
	 * Any request for the CSV data where the date of the request is older than this timestamp will result in the
	 * csv data on the device being reinitialised
	 */
	private void updateHeaders() throws SQLException {
		String sql = "Update csvtable set headers = ? where id = ?";
		PreparedStatement pstmt = null;
		
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, gson.toJson(headers));
			pstmt.setInt(2,  tableId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Get the number of records from the csv table
	 */
	private int recordCount() throws SQLException {
		String sql = "select count(*) from " + fullTableName + " where not _action = ?";
		PreparedStatement pstmt = null;
		int count = 0;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, DELETE_ENTRY);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				count = rs.getInt(1);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
		return count;
	}
	
	/*
	 * Truncate the csv table
	 */
	private void truncate() throws SQLException {
		String sql = "truncate " + fullTableName;
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Insert a csv record into the table
	 */
	private void insert(ArrayList<String[]> records, int headerSize, String filename) throws SQLException, IOException {
		
		if(records.size() == 0) {
			return;
		}
		
		// Create sql
		StringBuffer sql = new StringBuffer("insert into ").append(fullTableName).append( " (");
		StringBuffer params = new StringBuffer("");

		sql.append(PKCOL);
		params.append("nextval('").append(fullTableName).append("_seq')");
		
		sql.append(",").append(ACOL);
		params.append(",").append(ADD_ENTRY);		
		sql.append(",").append(TSCOL);
		params.append(",").append("now()");
		
		for(CsvHeader h : headers) {
			sql.append(",").append(h.tName);
			params.append(",").append("?");
		}
		
		
		sql.append(") values (").append(params).append(")");
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 0;
			for(String[] data : records) {
				if(data.length > 0) {
					for(int i = 0; i < headerSize; i++) {
						String v = "";	// fill empty cells with zero length string
						if(i < data.length) {
							v = data[i];
						}
						v = v.trim();
						pstmt.setString(i + 1, v);
					}
					if(idx++ == 0) {
						log.info("Insert first record of csv values: " + pstmt.toString());
						log.info("Number of records: " + records.size());
					}
					pstmt.executeUpdate();
				}
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}

	}
	
	/*
	 * Remove a CSV record from the table
	 */
	private void remove(ArrayList<String> records, String filename) throws SQLException, IOException {
		
		if(records.size() == 0) {
			return;
		}
		
		// Create sql
		StringBuffer sql = new StringBuffer("update ").append(fullTableName);
		sql.append(" set ").append(ACOL).append(" = ").append(DELETE_ENTRY);
		sql.append( " where ");
		int idx = 0;
		for(CsvHeader h : headers) {
			if(idx++ > 0) {
				sql.append(" and ");
			}
			sql.append(h.tName);
			sql.append(" = ?");
		}
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql.toString());
			int lineNumber = 1;
			for(String r : records) {
				String[] data = parser.parseLine(r, lineNumber++, filename);
				for(int i = 0; i < data.length; i++) {
					pstmt.setString(i + 1, data[i]);
				}
				log.info("Remove record: " + pstmt.toString());
				pstmt.executeUpdate();
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
	}

}
