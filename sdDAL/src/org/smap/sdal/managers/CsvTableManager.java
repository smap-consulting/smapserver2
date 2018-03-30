package org.smap.sdal.managers;

import java.io.BufferedReader;
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
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.CSVParser;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ActionManager.Update;

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
 * Manage the table that stores details on the forwarding of data onto other
 * systems
 */
public class CsvTableManager {

	private static Logger log = Logger.getLogger(CsvTableManager.class.getName());

	private class CsvHeader {
		String fName;			// Name in file
		String tName;			// Name in table
		public CsvHeader(String f, String t) {
			fName = f;
			tName = t;
		}
	}
	
	Connection sd = null;
	ResourceBundle localisation = null;
	private int tableId = 0;
	private int oId = 0;
	private int sId = 0;
	private String fileName = null;
	private String tableName = null;
	private String schema = "csv";
	private String fullTableName = null;
	private ArrayList<CsvHeader> headers = null;
	CSVParser parser = new CSVParser();
	
	private final String PKCOL = "_id";
	private final String ACOL = "_action";
	private final String TSCOL = "_changed_ts";
	
	private final int ADD_ENTRY = 1;
	private final int UPDATE_ENTRY = 2;
	private final int DELETE_ENTRY = 3;
	
	public CsvTableManager(Connection sd, ResourceBundle l, int oId, int sId, String fileName)
			throws Exception {
		
		this.sd = sd;
		this.localisation = l;
		this.oId = oId;
		this.sId = sId;
		this.fileName = fileName;
		
		Type headersType = new TypeToken<ArrayList<CsvHeader>>() {}.getType();
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		
		String sqlGetCsvTable = "select id, headers from csvtable where o_id = ? and s_id = ? and filename = ?";
		PreparedStatement pstmtGetCsvTable = null;
		
		String sqlInsertCsvTable = "insert into csvtable (id, o_id, s_id, filename, headers) "
				+ "values(nextval('csv_seq'), ?, ?, ?, ?)";
		PreparedStatement pstmtInsertCsvTable = null;
		try {
			pstmtGetCsvTable = sd.prepareStatement(sqlGetCsvTable);
			pstmtGetCsvTable.setInt(1, oId);
			pstmtGetCsvTable.setInt(2, sId);
			pstmtGetCsvTable.setString(3, fileName);
			log.info("Getting csv file name: " + pstmtGetCsvTable.toString());
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
				log.info("Create a new csv file entry: " + pstmtInsertCsvTable.toString());
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
	 * Update the table with data from the file
	 */
	public void updateTable(File newFile, File oldFile) throws IOException, SQLException {
		
		BufferedReader brNew = null;
		BufferedReader brOld = null;
		boolean delta = true;			// If set true then apply a delta between the new and old file

		PreparedStatement pstmtCreateSeq = null;
		PreparedStatement pstmtCreateTable = null;
		PreparedStatement pstmtAlterColumn = null;
		
		try {
			// Open the files
			FileReader readerNew = new FileReader(newFile);
			brNew = new BufferedReader(readerNew);
	
			FileReader readerOld = null;
			if (oldFile != null && oldFile.exists()) {
				readerOld = new FileReader(oldFile);
				brOld = new BufferedReader(readerOld);
			} else {
				delta = false;
			}
				
			// Get the column headings from the new file
			CSVParser parser = new CSVParser();
			String newLine = brNew.readLine();
			String cols[] = parser.parseLine(newLine);
			headers = new ArrayList<CsvHeader> ();
			for(String n : cols) {
				headers.add(new CsvHeader(n, GeneralUtilityMethods.cleanNameNoRand(n)));
			}
			
			/*
			 * 1. Create or modify the table
			 */
			boolean tableExists = GeneralUtilityMethods.tableExistsInSchema(sd, tableName, schema);
			if(!tableExists) {
				delta = false;
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
			ArrayList<String> listNew = new ArrayList<String> ();
			ArrayList<String> listOld = new ArrayList<String> ();
			ArrayList<String> listAdd = null;
			ArrayList<String> listDel = null;
			int recordCount = recordCount();
		
			while (newLine != null) {
				newLine = brNew.readLine();
				if(newLine != null) {
					listNew.add(newLine);
				}
			}
			
			if(delta) {			
			
				newLine = brOld.readLine();	// Skip over header
				while (newLine != null) {
					newLine = brOld.readLine();
					if(newLine != null) {
						listOld.add(newLine);
					}
				}
				
				listAdd = new ArrayList<String>(listNew);
				listDel = new ArrayList<String>(listOld);
				
				if(recordCount != listOld.size()) {
					// mismatch between the current size of the table and the old csv file - just reload the whole thing
					delta = false;
					listOld = null;
				} else {
					listAdd.removeAll(listOld);
					listDel.removeAll(listNew);
					
					System.out.println("Applying delta add: " + listAdd.size() + " delete: " + listDel.size());
					if(listDel.size() > 100 || listDel.size() == recordCount) {
						// Too many records to delete or all of the records need to be deleted just load the new data into an empty table
						delta = false;
						listOld = null;
						listAdd = null;
						listDel = null;
					}
				}
			}
			
			/*
			 * 3. Upload the data
			 */
			if(delta) {
				insert(listAdd);
				remove(listDel);
			} else {
				truncate();
				insert(listNew);
			}		
			
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
			if(brNew != null) {try{brNew.close();}catch(Exception e) {}}
			if(brOld != null) {try{brOld.close();}catch(Exception e) {}}
		}
		
	}
	
	/*
	 * Save the headers so that when generating csv output we can use the original file name header
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
	private void insert(ArrayList<String> records) throws SQLException, IOException {
		
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
			for(String r : records) {
				String[] data = parser.parseLine(r);
				for(int i = 0; i < data.length; i++) {
					pstmt.setString(i + 1, data[i]);
				}
				log.info("Inserting record: "  + pstmt.toString());
				pstmt.executeUpdate();
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}

	}
	
	/*
	 * Remove a CSV record from the table
	 */
	private void remove(ArrayList<String> records) throws SQLException, IOException {
		
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
			for(String r : records) {
				System.out.println("Remove record: " + r);
				String[] data = parser.parseLine(r);
				for(int i = 0; i < data.length; i++) {
					pstmt.setString(i + 1, data[i]);
				}
				log.info("Removing record: "  + pstmt.toString());
				pstmt.executeUpdate();
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
	}

}
