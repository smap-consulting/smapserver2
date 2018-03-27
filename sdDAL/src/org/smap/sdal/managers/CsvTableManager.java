package org.smap.sdal.managers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.CSVParser;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.SurveyViewDefn;
import org.smap.sdal.model.TableColumn;
import org.smap.sdal.model.User;
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

	Connection sd = null;
	ResourceBundle localisation = null;
	private int tableId = 0;
	private int oId = 0;
	private int sId = 0;
	private String fileName = null;
	
	public CsvTableManager(Connection sd, ResourceBundle l, int oId, int sId, String fileName)
			throws Exception {
		
		this.sd = sd;
		this.localisation = l;
		this.oId = oId;
		this.sId = sId;
		this.fileName = fileName;
		
		String sqlGetCsvTable = "select id from csvtable where o_id = ? and s_id = ? and filename = ?";
		PreparedStatement pstmtGetCsvTable = null;
		
		String sqlCreateCsvTable = "insert into csvtable (id, o_id, s_id, filename) "
				+ "values(nextval('csv_seq'), ?, ?, ?)";
		PreparedStatement pstmtCreateCsvTable = null;
		try {
			pstmtGetCsvTable = sd.prepareStatement(sqlGetCsvTable);
			pstmtGetCsvTable.setInt(1, oId);
			pstmtGetCsvTable.setInt(2, sId);
			pstmtGetCsvTable.setString(3, fileName);
			log.info("Getting csv file name: " + pstmtGetCsvTable.toString());
			ResultSet rs = pstmtGetCsvTable.executeQuery();
			if(rs.next()) {
				tableId = rs.getInt(1);
			} else {
				pstmtCreateCsvTable = sd.prepareStatement(sqlCreateCsvTable, Statement.RETURN_GENERATED_KEYS);
				pstmtCreateCsvTable.setInt(1, oId);
				pstmtCreateCsvTable.setInt(2, sId);
				pstmtCreateCsvTable.setString(3, fileName);
				log.info("Create a new csv file entry: " + pstmtCreateCsvTable.toString());
				pstmtCreateCsvTable.executeUpdate();
				ResultSet rsKeys = pstmtCreateCsvTable.getGeneratedKeys();
				if(rsKeys.next()) {
					tableId = rsKeys.getInt(1);
				} else {
					throw new Exception("Failed to create CSV Table entry");
				}
			}
		} finally {
			try {pstmtGetCsvTable.close();} catch(Exception e) {}
			try {pstmtCreateCsvTable.close();} catch(Exception e) {}
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
			ArrayList<String> colNames = new ArrayList<String> ();
			for(String h : cols) {
				System.out.println("------- " + h);
				colNames.add(GeneralUtilityMethods.cleanNameNoRand(h));
			}
			
			// Create or modify the table
			String tableName = "csv.csv" + tableId;
			boolean tableExists = GeneralUtilityMethods.tableExists(sd, tableName);
			if(!tableExists) {
				delta = false;
				StringBuffer sqlCreate = new StringBuffer("create table ").append(tableName).append("(");
				int idx = 0;
				for(String n : colNames) {
					if(idx++ > 0) {
						sqlCreate.append(",");
					}
					sqlCreate.append(n).append(" text");
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
			}
		} finally {
			if(pstmtCreateTable != null) {try{pstmtCreateTable.close();} catch(Exception e) {}}
		}
		
	}

}
