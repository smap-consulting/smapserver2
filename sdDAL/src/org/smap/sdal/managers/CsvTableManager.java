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

		PreparedStatement pstmtCreateTable = null;
		PreparedStatement pstmtAddColumn = null;
		
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
			
			// Create or modify the table
			boolean tableExists = GeneralUtilityMethods.tableExistsInSchema(sd, tableName, schema);
			if(!tableExists) {
				delta = false;
				StringBuffer sqlCreate = new StringBuffer("create table ").append(fullTableName).append("(");
				int idx = 0;
				for(CsvHeader c : headers) {
					if(idx++ > 0) {
						sqlCreate.append(",");
					}
					sqlCreate.append(c.tName).append(" text");
				}
				if(idx++ > 0) {
					sqlCreate.append(",");
				}
				sqlCreate.append("changed_ts TIMESTAMP WITH TIME ZONE");
				sqlCreate.append(")");
				pstmtCreateTable = sd.prepareStatement(sqlCreate.toString());
				pstmtCreateTable.executeUpdate();
				
			} else {
				System.out.println("Do a table merge");
				
				// Add any new columns, old unused columns will be deleted after loading of data as 
				//  they may be needed in order to identify rows to delete
				for(CsvHeader c : headers) {
					if(!GeneralUtilityMethods.hasColumn(sd, fullTableName, c.tName)) {
						String sqlAddColumn = "alter table " + fullTableName + " add column " + c.tName + " text";
						if(pstmtAddColumn != null) {try{pstmtAddColumn.close();} catch(Exception e) {}}
						pstmtAddColumn = sd.prepareStatement(sqlAddColumn);
						pstmtAddColumn.executeUpdate();
					}
				}
			}
			updateHeaders();		// Store the csv headers information with the match between file name and table name
		} finally {
			if(pstmtCreateTable != null) {try{pstmtCreateTable.close();} catch(Exception e) {}}
			if(pstmtAddColumn != null) {try{pstmtAddColumn.close();} catch(Exception e) {}}
		}
		
	}
	
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

}
