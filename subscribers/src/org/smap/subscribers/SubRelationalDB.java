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

package org.smap.subscribers;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;

import org.smap.model.IE;
import org.smap.model.SurveyInstance;
import org.smap.model.SurveyTemplate;
import org.smap.model.TableManager;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.ForeignKeyManager;
import org.smap.sdal.managers.KeyManager;
import org.smap.sdal.managers.LinkageManager;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.managers.SubmissionEventManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.AuditData;
import org.smap.sdal.model.AuditItem;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.DataItemChange;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.model.ForeignKey;
import org.smap.sdal.model.MediaChange;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.UniqueKey;
import org.smap.server.entities.Form;
import org.smap.server.entities.SubscriberEvent;
import org.smap.server.exceptions.SQLInsertException;
import org.smap.server.utilities.UtilityMethods;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SubRelationalDB extends Subscriber {

	private static Logger log =
			Logger.getLogger(Subscriber.class.getName());
	
	private class Keys {
		ArrayList<Integer> duplicateKeys = new ArrayList<Integer>();
		int newKey = 0;
	}

	String gBasePath = null;
	String gFilePath = null;
	String gAuditFilePath = null;
	
	private Survey survey = null;
	private Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	
	/**
	 * @param args
	 */
	public SubRelationalDB() {
		super();

	}

	@Override
	public String getDest() {
		return "upload";

	}

	@Override
	public ArrayList<MediaChange> upload(SurveyInstance instance, InputStream is, String submittingUser, 
			boolean temporaryUser,
			String server, String device, SubscriberEvent se, String confFilePath, String formStatus,
			String basePath, String filePath, String updateId, int ue_id, Date uploadTime,
			String surveyNotes, String locationTrigger, String auditFilePath, ResourceBundle l, Survey survey)  {

		localisation = l;
		tz = "UTC";			// Default default time zone
		gBasePath = basePath;
		gFilePath = filePath;
		gAuditFilePath = auditFilePath;

		if(gBasePath == null || gBasePath.equals("/ebs1")) {
			gBasePath = "/ebs1/servers/" + server.toLowerCase();
		}
		formStatus = (formStatus == null) ? "complete" : formStatus;

		// Open the configuration file
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DatabaseConnections dbc = new DatabaseConnections();
	
		SubmissionEventManager sem = new SubmissionEventManager();
		
		try {

			GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);			
			
			this.survey = survey;

			int assignmentId = getAssignmentId(dbc.sd, ue_id);
			
			writeAllTableContent(dbc.sd, dbc.results, instance, submittingUser, server, device, 
					formStatus, updateId, uploadTime, surveyNotes, 
					locationTrigger, assignmentId, survey.o_id);	
			
			/*
			 * Create an item in the submission event queue for the processing of
			 * notifications, Tasks and Linkage events associated with this submission
			 */
			sem.writeToQueue(dbc.sd, ue_id, linkageItems, gson);
			
			/*
			 * Update the assignment status
			 */
			if(assignmentId > 0) {
				String id = updateId;
				if(id == null) {
					id = instance.getUuid();
				}
				applyAssignmentStatus(dbc.sd, dbc.results, assignmentId, ue_id, id);
			}
			
			se.setStatus("success");			

		} catch (SQLInsertException e) {  

			se.setStatus("error");
			se.setReason(e.getMessage());

		} catch (Exception e) {
			e.printStackTrace();
			se.setStatus("error");
			se.setReason("Configuration File:" + e.getMessage());
			
		} finally {
			try {
				if (dbc.sd != null) {
					dbc.sd.close();
				}
			} catch (SQLException e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			
			try {
				if (dbc.results != null) {
					dbc.results.close();
				}
			} catch (SQLException e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
		}

		return mediaChanges;
	}
	
	/*
	 * Apply any changes to assignment status
	 */
	private void applyAssignmentStatus(Connection sd, Connection cResults, int assignmentId, int ue_id, 
			String updateId) {

		PreparedStatement pstmt = null;
		PreparedStatement pstmtRepeats = null;
		PreparedStatement pstmtUpdateId = null;

		String sql = "update assignments set status = 'submitted', completed_date = now() "
				+ "where id = ? ";

		String sqlRepeats = "update tasks set repeat_count = repeat_count + 1 "
				+ "where id = (select task_id from assignments where id = ?)";
		
		String sqlUpdateId = "update tasks set update_id = ? "
				+ "where id = (select task_id from assignments where id = ?) "
				+ "and update_id is null";

		try {

			pstmt = sd.prepareStatement(sql);
			pstmtRepeats = sd.prepareStatement(sqlRepeats);
			pstmtUpdateId = sd.prepareStatement(sqlUpdateId);
			
			if(assignmentId > 0) {
				pstmt.setInt(1, assignmentId);
				log.info("Updating assignment status: " + pstmt.toString());
				pstmt.executeUpdate();

				pstmtRepeats.setInt(1, assignmentId);
				log.info("Updating task repeats: " + pstmtRepeats.toString());
				pstmtRepeats.executeUpdate();

				// Cancel other assignments if complete_all is not set for the task
				TaskManager tm = new TaskManager(localisation, tz);
				tm.cancelOtherAssignments(sd, cResults, assignmentId);
				
				// If this task created new data then set its updateId to point to that new data
				pstmtUpdateId.setString(1, updateId);
				pstmtUpdateId.setInt(2, assignmentId);
				log.info("Updating task updateId: " + pstmtUpdateId.toString());
				pstmtUpdateId.executeUpdate();
				
				// Write a message to the record event manager
				RecordEventManager rem = new RecordEventManager();
				rem.writeTaskStatusEvent(
						sd, 
						cResults,
						0,
						assignmentId,
						TaskManager.STATUS_T_SUBMITTED,
						null,			// Assigned not changed
						null,
						false);			// Task Title not changed
			}


		} catch (SQLException e) {
			e.printStackTrace();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtRepeats != null) {pstmtRepeats.close();}} catch (SQLException e) {}
			try {if (pstmtUpdateId != null) {pstmtUpdateId.close();}} catch (SQLException e) {}

		}
	}
	
	/*
	 * Get the assignment ID
	 */
	private int getAssignmentId(Connection sd, int ue_id) {

		int assignmentId = 0;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		String sql = "select ue.assignment_id " +
				" from upload_event ue " +
				" where ue.ue_id = ? and ue.assignment_id is not null";

		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, ue_id);
			rs = pstmt.executeQuery();

			if(rs.next()) {
				assignmentId = rs.getInt(1);
				log.info("Assignment id: " + assignmentId);
				
			}


		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (rs != null) {rs.close();}} catch (SQLException e) {}
		}
		return assignmentId;
	}

	/*
	 * Write the submission to the database
	 */
	private void writeAllTableContent(Connection sd, Connection cResults, SurveyInstance instance, String remoteUser, 
			String server, String device, String formStatus, String updateId,
			Date uploadTime, String surveyNotes, String locationTrigger,
			int assignmentId, int oId) throws SQLInsertException {

		int sId = survey.id;
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtHrk = null;
		PreparedStatement pstmtAddHrk = null;
		boolean resAutoCommitSetFalse = false;
		ArrayList<ForeignKey> foreignKeys = new ArrayList<> ();

		try {

			if(cResults.getAutoCommit()) {
				log.info("Set autocommit results false");
				resAutoCommitSetFalse = true;
				cResults.setAutoCommit(false);
			}
			IE topElement = instance.getTopElement();

			// Make sure the top element matched a form in the template
			if(topElement.getType() == null) {
				String msg = "Error: Top element name " + topElement.getName() + " in survey did not match a form in the template";
				throw new Exception(msg);
			}
			
			KeyManager km = new KeyManager(localisation);
			UniqueKey uk = km.get(sd, survey.groupSurveyIdent);
			boolean hasHrk = (uk.key.trim().length() > 0);
			Keys keys = writeTableContent(
					topElement, 
					0, 
					instance.getTemplateName(), 
					remoteUser, 
					server, 
					device, 
					instance.getUuid(), 
					formStatus, 
					instance.getVersion(), 
					surveyNotes,
					locationTrigger,
					cResults,
					sd,
					sId,
					uploadTime,
					"/main",
					assignmentId,
					foreignKeys,
					null,				// audit data
					1,		// record counter
					oId);

			// Commit so that we have the data
			cResults.commit();
			
			/*
			 * Update existing records
			 */
			int existingKey = 0;
			if(keys.duplicateKeys.size() > 0) {
				log.info("Dropping duplicate");
			} 

			/*
			 * Key policy is applied if the table has an HRK
			 */
			org.smap.sdal.model.Form topLevelForm = null;
			topLevelForm = GeneralUtilityMethods.getTopLevelForm(sd, sId);
			String keyPolicy = uk.key_policy;
			
			// Make sure the key policy is valid
			if(!SurveyManager.isValidSurveyKeyPolicy(keyPolicy)) {
				keyPolicy = SurveyManager.KP_NONE;
			}
			log.info("################### Processing key policy:" + keyPolicy + ": " + hasHrk + " : " + assignmentId );
			
			String hrkSql = GeneralUtilityMethods.convertAllxlsNamesToQuery(uk.key, sId, sd, topLevelForm.tableName);
			if(updateId != null) {
				// Direct update to a record
				log.info("Direct update with Existing unique id:" + updateId);
				existingKey = getKeyFromId(cResults, topElement, updateId);

				boolean replace = true;		// Always replace for direct updates
				if(existingKey != 0) {
					log.info("Existing key:" + existingKey);
					combineTableContent(sd, cResults, sId, hrkSql, topLevelForm.tableName, keys.newKey, 
							topLevelForm.id,
							existingKey, replace, remoteUser, updateId, survey.groupSurveyIdent, survey.ident, localisation);		// Use updateId as the instance in order to get the thread.  The new instance will not have been committed yet
				} 
			} else if(hasHrk && !keyPolicy.equals(SurveyManager.KP_NONE)) {
				boolean replace = keyPolicy.equals(SurveyManager.KP_REPLACE);
				if(keyPolicy.equals(SurveyManager.KP_MERGE) || keyPolicy.equals(SurveyManager.KP_REPLACE)) {					
					log.info("Apply merge-replace policy");
					combineTableContent(sd, cResults, sId, hrkSql, topLevelForm.tableName, keys.newKey, topLevelForm.id, 0, 
							replace, remoteUser, instance.getUuid(), survey.groupSurveyIdent, survey.ident, localisation);
				} else if(keyPolicy.equals(SurveyManager.KP_DISCARD)) {
					log.info("Apply discard policy");
					discardTableContent(cResults, topLevelForm.tableName, keys.newKey);
				} 
				
			}  

			/*
			 * Update any Human readable keys if this survey has them
			 * This has to happen after merge so that previous HRK's are preserved
			 */
			if(hasHrk) {
				
				if(!GeneralUtilityMethods.hasColumn(cResults, topLevelForm.tableName, "_hrk")) {
					// This should not be needed as the _hrk column should be in the table if an hrk has been specified for the survey
					log.info("Error:  _hrk being created for table " + topLevelForm.tableName + " this column should already be there");
					String sqlAddHrk = "alter table " + topLevelForm.tableName + " add column _hrk text;";
					pstmtAddHrk = cResults.prepareStatement(sqlAddHrk);
					pstmtAddHrk.executeUpdate();
				}

				String sql = "select prikey from " + topLevelForm.tableName 
						+ " where _hrk is null "
						+ "order by prikey asc";
				pstmt = cResults.prepareStatement(sql);
				
				String sqlHrk = "update " + topLevelForm.tableName + " m set _hrk = "
						+ hrkSql
						+ " where prikey = ?;";
				pstmtHrk = cResults.prepareStatement(sqlHrk);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					pstmtHrk.setInt(1, rs.getInt(1));
					log.info("Applying HRK: " + pstmtHrk.toString());
					pstmtHrk.executeUpdate();
				}	
			}
			
			/*
			 * Record any foreign keys that need to be set between forms
			 */
			if(foreignKeys.size() > 0) {
				ForeignKeyManager fkm = new ForeignKeyManager();
				fkm.saveKeys(sd, updateId, foreignKeys, sId); 
			}
			
			cResults.commit();

			GeneralUtilityMethods.clearLinkedForms(sd, sId, localisation);  // Clear any entries in linked_forms for this survey - The CSV files will need to be refreshed
			
			/*
			 * If this is a simple create without an HRK then write to the record event manager
			 */
			if(updateId == null && !hasHrk) {
				RecordEventManager rem = new RecordEventManager();
				rem.writeEvent(sd, cResults, 
						RecordEventManager.CREATED, 
						RecordEventManager.STATUS_SUCCESS,
						remoteUser, 
						topLevelForm.tableName, 
						instance.getUuid(), 
						null, 					// Change object
						null, 					// Task object
						null,					// Notification object
						null, 
						sId, 
						null,
						0,
						0);	
			}

		} catch (Exception e) {
			if(cResults != null) {
				try {

					e.printStackTrace();
					cResults.rollback();
					throw new SQLInsertException(e.getMessage());

				} catch (SQLException ex) {

					log.info(ex.getMessage());
					throw new SQLInsertException(e.getMessage());

				}

			} else {

				String mesg = "Error: Connection to the database is null";
				log.info("        " + mesg);
				throw new SQLInsertException(mesg);

			}

		} finally {

			if(resAutoCommitSetFalse) {
				log.info("Set autocommit results true");
				resAutoCommitSetFalse = false;
				try {cResults.setAutoCommit(true);} catch(Exception e) {}
			}
			
			if(pstmtHrk != null) try{pstmtHrk.close();}catch(Exception e) {};
			if(pstmtAddHrk != null) try{pstmtAddHrk.close();}catch(Exception e) {};
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {};
		}		
	}

	/*
	 * Method to write the table content
	 */
	private  Keys writeTableContent(
			IE element, 
			int parent_key, 
			String sIdent, 
			String remoteUser, 
			String server, 
			String device, 
			String uuid, 
			String formStatus, 
			int version,
			String surveyNotes,
			String locationTrigger,
			Connection cResults,
			Connection sd,
			int sId,
			Date uploadTime,
			String auditPath,
			int assignmentId,
			ArrayList<ForeignKey> foreignKeys,
			AuditData auditData,
			int recCounter,
			int oId) throws SQLException, Exception {

		Keys keys = new Keys();
		PreparedStatement pstmt = null;

		try {
			/*
			 * Write the Instance element to a table if it is a form type The sub elements
			 * of a complex question will be written to their own table as well as being
			 * handled as a composite/complex question by the parent form
			 */

			if (element.getType() != null && (element.getType().equals("form")
					|| (element.getQType() != null && element.getQType().equals("geopolygon"))
					|| (element.getQType() != null && element.getQType().equals("geolinestring")))) {
				// Write form
				String tableName = element.getTableName();
				List<IE> columns = element.getQuestions();
			
				/*
				 * If this is the top level form then 1) create all the tables for this survey
				 * if they do not already exist 2) Check if this survey is a duplicate
				 */
				keys.duplicateKeys = new ArrayList<Integer>();
				TableManager tm = new TableManager(localisation, tz);
				CMS cms = null;
				if (parent_key == 0) { // top level survey has a parent key of 0
					
					// Create new tables
					SurveyTemplate template = new SurveyTemplate(localisation); 
					template.readDatabase(sd, cResults, sIdent, false);	
					ArrayList<String> tablesCreated = tm.writeAllTableStructures(sd, cResults, sId, template,  0);
					
					boolean tableChanged = false;
					boolean tablePublished = false;
					keys.duplicateKeys = checkDuplicate(cResults, tableName, uuid);

					if (keys.duplicateKeys.size() > 0 && getDuplicatePolicy() == DUPLICATE_DROP) {
						throw new Exception("Duplicate survey: " + uuid);
					}
					// Apply any updates that have been made to the table structure since the last
					// submission
					tableChanged = tm.applyTableChanges(sd, cResults, sId, tablesCreated);

					// Add any previously unpublished columns not in a changeset (Occurs if this is
					// a new survey sharing an existing table)
					tablePublished = tm.addUnpublishedColumns(sd, cResults, sId, tableName);

					if (tableChanged || tablePublished) {
						List<Form> forms = template.getAllForms();	
						for(Form f : forms) {
							tm.markPublished(sd, f.getId(), sId); // only mark published if there have been changes made
						}
					}
					
					/*
					 * Get Case Management Settings
					 */
					CaseManager cm = new CaseManager(localisation);				
					String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
					cms = cm.getCaseManagementSettings(sd, groupSurveyIdent);
				}

				boolean isBad = false;
				boolean complete = true;
				String bad_reason = null;
				if (formStatus != null && (formStatus.equals("incomplete") || formStatus.equals("draft"))) {
					isBad = true;
					bad_reason = "incomplete";
					complete = false;
				}

				/*
				 * Write the record
				 */
				if (columns.size() > 0 || parent_key == 0) {
					
					// Every survey should have the above columns
					GeneralUtilityMethods.ensureTableCurrent(cResults, tableName, parent_key == 0);

					/*
					 * Process audit data
					 */
					if(parent_key == 0 && gAuditFilePath != null) {
						File auditFile = new File(gAuditFilePath);
						auditData = GeneralUtilityMethods.getAuditHashMap(auditFile, auditPath, localisation);
					}			
					String auditString = null;
					if (auditData != null) {
						HashMap<String, AuditItem> auditValues = 
								GeneralUtilityMethods.getAuditValues(auditData,
								getColNames(columns), localisation);

						auditString = gson.toJson(auditValues);
					}
					// Raw audit data
					String rawAuditString = null;
					if(auditData != null && auditData.rawAudit != null) {
						rawAuditString = auditData.rawAudit.toString();
					}
					
					ArrayList<ForeignKey> thisTableKeys = new ArrayList<> ();
					pstmt = getSubmissionStatement(sd, cResults, 
							sIdent,
							device,
							server,
							tableName, 
							columns, 
							thisTableKeys,
							parent_key,
							remoteUser,
							cms,
							complete,
							uploadTime,
							sId,
							version,
							surveyNotes,
							locationTrigger,
							assignmentId,
							isBad, 
							bad_reason,
							auditString,
							rawAuditString,
							oId);
					
					log.info("1111111111: " + pstmt.toString());
					pstmt.executeUpdate();
					ResultSet rs = pstmt.getGeneratedKeys();
					
					if (rs.next()) {
						parent_key = rs.getInt(1);
						keys.newKey = parent_key;
					}
					// Add primary key, instanceId and table name to the foreign keys for this table
					for(ForeignKey fk : thisTableKeys) {
						fk.primaryKey = parent_key;
						fk.tableName = tableName;
						fk.instanceIdLaunchingForm  = uuid;
					}
					foreignKeys.addAll(thisTableKeys);
					
				}
				/*
				 * Write into the points table of geocompound types
				 */
				for(IE col : columns) {
					if(col.getQType().equals("geocompound")) {
						if (pstmt != null) try {	pstmt.close();} catch (Exception e) {}
						
						String compoundSql = "insert into " + tableName + "_" + col.getColumnName()
								+ "(parkey, properties, locn) "
								+ "values(?, ?, ST_GeomFromText(?, 4326))";
						pstmt = cResults.prepareStatement(compoundSql);
						
						String compoundValue = col.getValue();
						log.info("------------------------------ Adding compound Value: " + compoundValue);
						if(compoundValue != null) {
							String components[] = compoundValue.split("#");
							for(int i = 0; i < components.length; i++) {
								if(components[i].startsWith("marker:")) {
									String pointComponents [] = components[i].split(":");
									HashMap<String, String> propObj = new HashMap<>();
									String locnText = null;
									if(pointComponents.length > 1) {
										String locn = pointComponents[1];
										String pLocn[] = locn.split(" ");
										if(pLocn.length > 1) { 
											locnText = "POINT(" + pLocn[1] + " " + pLocn[0] + ")";
										} else {
											locnText = "POINT(0 0)";
										}
										
										String props;
										if(pointComponents.length > 2) {
											props = pointComponents[2];	
										} else {
											props = pointComponents[1];		// old versions of webforms could miss out on the point coords
										}
										
										String pComp [] = props.split(";");
										for(int k = 0; k < pComp.length; k++) {
											String pe [] = pComp[k].split("=");
											propObj.put(pe[0].trim(), pe[1].trim());
										}
									}
									pstmt.setInt(1, parent_key);
									pstmt.setString(2, gson.toJson(propObj));
									pstmt.setString(3, locnText);
									log.info("Update compound points: " + pstmt.toString());
									pstmt.executeUpdate();
								}
							}
							
						}
					}
				}
			}

			// Write any child forms
			List<IE> childElements = element.getChildren();
			int childRecCounter = 1;
			for (IE child : childElements) {

				if (child.getType() != null && (child.getType().equals("form")
						|| (child.getQType() != null && child.getQType().equals("geopolygon"))
						|| (child.getQType() != null && child.getQType().equals("geolinestring"))
						|| (child.getQType() != null && child.getQType().equals("begin group")))) {
					
					writeTableContent(child, parent_key, sIdent, remoteUser, server, device, uuid, formStatus, version,
							surveyNotes, locationTrigger, cResults, sd, sId, uploadTime,
							auditPath + "/" + child.getName() + "[" + childRecCounter + "]", assignmentId,
							foreignKeys, auditData, childRecCounter, oId);
					recCounter++;
				}
			}
			
		} finally {
			if (pstmt != null) try {	pstmt.close();} catch (Exception e) {}
		}

		return keys;

	}
	
	private class TableColumn {
		String colName;
		String value;
		String type;
		public TableColumn(String colName, String value, String type) {
			this.colName = colName;
			this.value = value;
			this.type = type;
		}
	}
	
	private class DynamicMetaValues {
		public Timestamp case_closed;
	}
	
	/*
	 * Get a prepared statement to insert the submission into the database
	 */
	private PreparedStatement getSubmissionStatement(
			Connection sd, 
			Connection cResults, 
			String sIdent,
			String device,
			String server,
			String tableName,
			List<IE> columns,
			ArrayList<ForeignKey> foreignKeys,
			int parentKey,
			String remoteUser,
			CMS cms,
			boolean complete,
			Date uploadTime,
			int sId,
			int version,
			String surveyNotes,
			String locationTrigger,
			int assignmentId,
			boolean isBad,
			String badReason,
			String audit,
			String auditRaw,
			int oId) throws SQLException {
		
		PreparedStatement pstmt = null;
		
		StringBuilder sql = new StringBuilder("");
		StringBuilder cols = new StringBuilder("");
		StringBuilder vals = new StringBuilder("");
		
		/*
		 * Convert the submission column names into 
		 * actual column names
		 */
		ArrayList<TableColumn> tableCols = new ArrayList<>();
		
		// Meta columns
		addTableCol(cols, vals, tableCols, "parkey", String.valueOf(parentKey), "int");
		if(parentKey == 0) {
			addTableCol(cols, vals, tableCols, "_user", String.valueOf(remoteUser), "string");
			addTableCol(cols, vals, tableCols, "_complete", String.valueOf(complete), "boolean");
			addTableCol(cols, vals, tableCols, SmapServerMeta.UPLOAD_TIME_NAME, String.valueOf(new Timestamp(uploadTime.getTime())), "timestamp");
			addTableCol(cols, vals, tableCols, SmapServerMeta.THREAD_CREATED, String.valueOf(new Timestamp(uploadTime.getTime())), "timestamp");
			addTableCol(cols, vals, tableCols, SmapServerMeta.SURVEY_ID_NAME, String.valueOf(sId), "int");
			addTableCol(cols, vals, tableCols, "_version", String.valueOf(version), "int");
			addTableCol(cols, vals, tableCols, "_survey_notes", surveyNotes, "string");
			addTableCol(cols, vals, tableCols, "_location_trigger", locationTrigger, "string");
			if(assignmentId == 0) {
				addTableCol(cols, vals, tableCols, SmapServerMeta.SCHEDULED_START_NAME, null, "timestamp");
			} else {
				addTableCol(cols, vals, tableCols, SmapServerMeta.SCHEDULED_START_NAME, String.valueOf(GeneralUtilityMethods.getScheduledStart(sd, assignmentId)), "timestamp");
			}
			addTableCol(cols, vals, tableCols, "_bad", String.valueOf(isBad), "boolean");
			addTableCol(cols, vals, tableCols, "_bad_reason", badReason, "string");
			// TODO case closed
		}
		addTableCol(cols, vals, tableCols, "_audit", audit, "string");
		addTableCol(cols, vals, tableCols, "_audit_raw", auditRaw, "string");
		
		/*
		 * Add the survey columns
		 */
		DynamicMetaValues dmv = addSurveyColumns(sd, cResults, sIdent, cms, device, server, tableName, columns, cols, vals, tableCols, foreignKeys, false, oId);		
		
		/*
		 * Add dynamic meta value columns
		 */
		if(parentKey == 0) {
			if(dmv.case_closed != null) {
				addTableCol(cols, vals, tableCols, "_case_closed", String.valueOf(dmv.case_closed), "timestamp");
			} else {
				addTableCol(cols, vals, tableCols, "_case_closed", null, "timestamp");
			}
		}
		
		/*
		 * Construct sql
		 */
		sql.append("insert into ")
			.append(tableName)
			.append(" (")
			.append(cols)
			.append(") values (")
			.append(vals)
			.append(")");
		
		/*
		 * Prepare the statement
		 */
		pstmt = cResults.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
		
		int idx = 1;
		for(TableColumn c : tableCols) {
			
			if(c.type.equals("int")) {	// int
				if(c.value == null) {
					pstmt.setNull(idx++, java.sql.Types.INTEGER);
				} else {
					pstmt.setInt(idx++, Integer.valueOf(c.value));
				}
				
			} else if(c.type.equals("double") || c.type.equals("decimal") || c.type.equals("range")) {	// decimal
				if(c.value == null) {
					pstmt.setNull(idx++, java.sql.Types.DOUBLE);
				} else {
					pstmt.setDouble(idx++, Double.valueOf(c.value));
				}
				
			} else if(c.type.equals("boolean")) {	// boolean
				if(c.value == null) {
					pstmt.setBoolean(idx++, false);
				} else {
					pstmt.setBoolean(idx++, Boolean.valueOf(c.value));
				}
				
			} else if(c.type.equals("timestamp") || c.type.equals("dateTime")) {	// Timestamp
				if(c.value == null) {
					pstmt.setTimestamp(idx++, null);
				} else {
					pstmt.setTimestamp(idx++, GeneralUtilityMethods.getTimestamp(c.value));
				}
			} else if(c.type.equals("date")) {	// Date
				if(c.value == null) {
					pstmt.setDate(idx++, null);
				} else {
					pstmt.setDate(idx++, java.sql.Date.valueOf(LocalDate.parse(c.value)));
				}
			} else if(c.type.equals("time")) {	// Time
				if(c.value == null) {
					pstmt.setTime(idx++, null);
				} else {
					String t = c.value;
					if(c.value.indexOf(".") > 0) {
						t = c.value.substring(0, c.value.indexOf("."));
					}
					
					pstmt.setTime(idx++, java.sql.Time.valueOf(t));
				}
			} else if(c.colName.equals("_hrk")) {	// Forcing an hrk value, ensure null if blank
				String value = c.value;
				if(value != null) {
					if(value.trim().length() == 0) {
						value = null;
					}
				} 
				pstmt.setString(idx++, value);
			}else {
				pstmt.setString(idx++, c.value);	// Default is String
			}

		} 
		
		return pstmt;
	}
	
	/*
	 * Add the survey columns to be used in a submission insert
	 */
	private DynamicMetaValues addSurveyColumns(
			Connection sd,
			Connection cResults,
			String sIdent,
			CMS cms,
			String device,
			String server,
			String tableName,
			List<IE> columns, 
			StringBuilder cols, 
			StringBuilder vals, 
			ArrayList<TableColumn> tableCols,
			ArrayList<ForeignKey> foreignKeys,
			boolean phoneOnly,
			int oId) {	
		
		DynamicMetaValues dmv = new DynamicMetaValues();
		LinkageManager linkMgr = new LinkageManager(localisation);
		
		/*
		 * Prepare for checking for case closed
		 */
		String statusQuestion = null;
		String finalStatus = null;
		if(cms != null && cms.settings != null && cms.settings.statusQuestion != null && cms.settings.finalStatus != null) {
			statusQuestion = cms.settings.statusQuestion;
			finalStatus = cms.settings.finalStatus;
		}
		
		for(IE col : columns) {
			String colType = col.getQType();
			String colName = col.getColumnName();
			boolean colPhoneOnly = phoneOnly || col.isPhoneOnly();	// Set phone only if the group is phone only or just this column
			
			// TODO Deprecate this all select questions are now stored compressed
			if(colType.equals("select") && !col.isCompressed()) {
				List<IE> options = col.getChildren();
				UtilityMethods.sortElements(options);
				HashMap<String, String> uniqueColumns = new HashMap<String, String> (); 
				for(IE option : options) {
					if(uniqueColumns.get(option.getColumnName()) == null) {
						uniqueColumns.put(option.getColumnName(), option.getColumnName());
						addTableCol(cols, vals, tableCols, option.getColumnName(), colPhoneOnly ? "" : option.getValue(), "int");
					}		
				}
			} else if(colType.equals("geopoint")) {
				
				GeopointComponents components = getGeopointComponents(col.getValue());
				addTableCol(cols, vals, tableCols, col.getColumnName(), components.value, "geopoint");

				if(GeneralUtilityMethods.hasColumn(cResults, tableName, col.getColumnName() + "_alt")) {
					// Geopoint also has altitude and accuracy
					addTableCol(cols, vals, tableCols, col.getColumnName() + "_alt", components.alt, "double");
					addTableCol(cols, vals, tableCols, col.getColumnName() + "_acc", components.acc, "double");
				}
			} else if(colType.equals("geopolygon") || colType.equals("geolinestring")
					|| colType.equals("geoshape") || colType.equals("geotrace") || colType.equals("geocompound")) {
				
				addTableCol(cols, vals, tableCols, col.getColumnName(), 
						getDbValue(sd, col, sIdent, device, server, colPhoneOnly, cResults, tableName, oId), colType);

			} else if(colType.equals("begin group")) {
				// Non repeating group, process these child columns at the same level as the parent
				addSurveyColumns(sd, cResults, sIdent, cms, device, server, tableName, col.getQuestions(), cols, vals, tableCols, foreignKeys, phoneOnly, oId);	
			} else {
				String value = getDbValue(sd, col, sIdent, device, server, colPhoneOnly, cResults, tableName, oId);
				addTableCol(cols, vals, tableCols, colName, value, colType);
				if(colName.equals("instanceid")) {
					addTableCol(cols, vals, tableCols, "_thread", value, colType);		// Initialise the _thread value
				}
				
				// Check for a foreign key, the value will start with :::::
				if(col.getValue() != null && col.getValue().startsWith(":::::") && col.getValue().length() > 5) {
					ForeignKey fl = new ForeignKey();
					fl.instanceId = col.getValue().substring(5);
					fl.qName = col.getName();
					foreignKeys.add(fl);
				}
				
				// Check for case closed
				if(statusQuestion != null && colName.equals(statusQuestion) && value.equals(finalStatus)) {
					dmv.case_closed = new Timestamp(new java.util.Date().getTime());
				}
				
				// Add to linkage items
				linkMgr.addDataitemToList(linkageItems, value, col.getAppearance(), col.getParameters(), sIdent, colName);
			}

		}
		
		return dmv;
	}
	
	/*
	 * Create a table column
	 */
	private void addTableCol(StringBuilder cols, 
			StringBuilder vals, 
			ArrayList<TableColumn> tableCols, 
			String colName, String value, String type) {
		
		// Add column name sql
		if(cols.length() > 0) {
			cols.append(",");
		}
		cols.append(colName);
		
		// Add value sql
		if(vals.length() > 0) {
			vals.append(",");
		}
		if(type.equals("geopoint")) {
			vals.append("ST_GeomFromText('POINT(' || ? || ')', 4326)");
		} else if(type.equals("geoshape")) {
			vals.append("ST_GeomFromText('POLYGON((' || ? || '))', 4326)");
		} else if(type.equals("geotrace") || type.equals("geocompound")) {
			vals.append("ST_GeomFromText('LINESTRING(' || ? || ')', 4326)");
		} else {
			vals.append("?");
		}
		
		// Add a table entry to allow insertion of value into value string
		tableCols.add(new TableColumn(colName, value, type));
		
	}
	/*
	 * Method to merge a previous records content into this new record
	 * Source = the old records
	 */
	private void combineTableContent(
			Connection sd,
			Connection cResults,
			int sId,
			String hrkSql,
			String table,
			int prikey,
			int f_id,
			int sourceKey,
			boolean replace,
			String user,
			String newInstance,
			String groupSurveyIdent,
			String ident,
			ResourceBundle localisation) throws SQLException, Exception {

		String sql = "select " + hrkSql + " from " + table + " m where m.prikey = ?";
		PreparedStatement pstmt = null;
		
		String sqlSource = "select prikey from " + table + " where _hrk = ? "
				+ "and prikey != ? "
				+ "and _bad = 'false' "
				+ "order by prikey desc limit 1";
		PreparedStatement pstmtSource = null;

		String sqlChildTables = "select table_name, f_id, name from form "
				+ "where parentform in (select f_id from form where parentform = 0 and s_id = ?) "
				+ "and reference = 'false'";
		PreparedStatement pstmtChildTables = null;
		
		String sqlChildTablesInGroup = "select distinct table_name from form "
				+ "where reference = 'false' "
				+ "and parentform in (select f_id from form where parentform = 0 "
				+ "and (s_id in (select s_id from survey where group_survey_ident = ? and deleted='false'))) ";
		PreparedStatement pstmtChildTablesInGroup = null;
		
		String sqlGetFormDetails = "select distinct f_id, name  from form "
				+ "where reference = 'false' "
				+ "and table_name = ? "
				+ "and s_id = ?";
		PreparedStatement pstmtFormDetails = null;

		String sqlTableMerge = "select table_name, merge, replace, append from form "
				+ "where s_id = ? "
				+ "and reference = 'false' ";
		PreparedStatement pstmtTableMerge = null;
		
		PreparedStatement pstmtChildKeys = null;
		PreparedStatement pstmtCopyChild = null;
		PreparedStatement pstmtCopyBack = null;
		
		ArrayList<DataItemChange> changes = null;
		try {

			String hrk = null;
			if(sourceKey == 0) {
				// Get the HRK
				pstmt = cResults.prepareStatement(sql);
				pstmt.setInt(1, prikey);
				log.info("Get HRK: " + pstmt.toString());
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()) {
					hrk = rs.getString(1);
				}
				// Get the prikey of the source record
				pstmtSource = cResults.prepareStatement(sqlSource);
				pstmtSource.setString(1, hrk);
				pstmtSource.setInt(2, prikey);
				rs = pstmtSource.executeQuery();
				
				if(rs.next()) {
					sourceKey = rs.getInt(1);
				}
			} 
			
			log.info("++++++++++++++ Merge Begins +++++++++++++++++");
			log.info("++++++++++++++ Source Key: " + sourceKey );
			RecordEventManager rem = new RecordEventManager();
			if(sourceKey > 0) {

				changes = mergeRecords(sd, cResults, table, prikey, sourceKey, replace, f_id, false, groupSurveyIdent);

				// Get the per table merge policy for this survey
				pstmtTableMerge = sd.prepareStatement(sqlTableMerge);
				pstmtTableMerge.setInt(1, sId);
				log.info("Get table merge policy: " + pstmtTableMerge.toString());
				ResultSet rtm = pstmtTableMerge.executeQuery();
				ArrayList<String> mergeTables = new ArrayList<> ();
				ArrayList<String> replaceTables = new ArrayList<> ();
				ArrayList<Integer> copiedSourceKeys = new ArrayList<> ();
				while(rtm.next()) {
					if(rtm.getBoolean("replace") /*|| replace*/) {
						replaceTables.add(rtm.getString(1));
						log.info("Need to replace table " + rtm.getString(1));
					} else if(rtm.getBoolean("merge")) {
						mergeTables.add(rtm.getString(1));
						log.info("Need to merge table " + rtm.getString(1));
					} else if(rtm.getBoolean("append")) {
						// No need to do anything
						log.info("Appending " + rtm.getString(1));
					} else {
						// Default
						if(replace) {
							log.info("Defaulting to replace " + rtm.getString(1));
							replaceTables.add(rtm.getString(1));
						} else {
							// No need to do anything
							log.info("Defaulting to append " + rtm.getString(1));
						}
					}
				}
				
				// Add the child records from the merged survey to the new survey
				ResultSet rsc = null;
				if(!ident.equals(groupSurveyIdent)) {
					pstmtChildTablesInGroup = sd.prepareStatement(sqlChildTablesInGroup);
					pstmtChildTablesInGroup.setString(1,  groupSurveyIdent);
					log.info("Get child tables for group: " + pstmtChildTablesInGroup.toString());
					rsc = pstmtChildTablesInGroup.executeQuery();
				
					pstmtFormDetails = sd.prepareStatement(sqlGetFormDetails);
					pstmtFormDetails.setInt(2, sId);
				} else {
					
					// Not in a group - update the child tables directly
					pstmtChildTables = sd.prepareStatement(sqlChildTables);
					pstmtChildTables.setInt(1,  sId);
					log.info("Get child tables: " + pstmtChildTables.toString());
					rsc = pstmtChildTables.executeQuery();
				}
				
				while(rsc.next()) {
					String tableName = rsc.getString(1);
					
					// Get the child id and form name which are needed for console history of merges
					int child_f_id = 0;
					String formname = null;
					
					if(!ident.equals(groupSurveyIdent)) {
						pstmtFormDetails.setString(1, tableName);
						ResultSet rsFD = pstmtFormDetails.executeQuery();
						if(rsFD.next()) {
							child_f_id = rsFD.getInt(1);
							formname = rsFD.getString(2);
						}
					} else {
						child_f_id = rsc.getInt(2);
						formname = rsc.getString(3);
					}
					if(GeneralUtilityMethods.tableExists(cResults, tableName)) {
						
						log.info("++++++++++++++ Table: " + tableName );
						
						ArrayList<ArrayList<DataItemChange>> subFormChanges = new ArrayList<ArrayList<DataItemChange>> ();
						
						/*
						 * Get the source keys and the target primary keys
						 */
						String sqlGetChildKeys = "select prikey from " + tableName + " where parkey = ? order by prikey asc";
						pstmtChildKeys = cResults.prepareStatement(sqlGetChildKeys);
						ArrayList<Integer> childPrikeys = new ArrayList<> ();
						ArrayList<Integer> childSourcekeys = new ArrayList<> ();
						
						pstmtChildKeys.setInt(1, prikey);
						ResultSet gk = pstmtChildKeys.executeQuery();
						while(gk.next()) {
							childPrikeys.add(gk.getInt(1));
						}
						pstmtChildKeys.setInt(1, sourceKey);
						gk = pstmtChildKeys.executeQuery();
						while(gk.next()) {
							childSourcekeys.add(gk.getInt(1));
						}
						String sqlCopyChild = "update " + tableName + " set parkey = ? where prikey = ?";
						pstmtCopyChild = cResults.prepareStatement(sqlCopyChild);
						
						if(mergeTables.contains(tableName)) {					
							
							log.info("====================== Merging " + childSourcekeys.size() + " records from " + tableName + " to " + childPrikeys.size() + " records");
							
							for(int i = 0; i < childSourcekeys.size(); i++) {
								
								if(i < childPrikeys.size()) {
									// merge
									log.info("Merge from " + childSourcekeys.get(i) + " to " + childPrikeys.get(i));
									if(child_f_id > 0) {
										subFormChanges.add(mergeRecords(
												sd,
												cResults, 
												tableName, 
												childPrikeys.get(i), childSourcekeys.get(i), 
												false,   // Doing a merge so set replace to false
												child_f_id,
												true,
												groupSurveyIdent));  
									}
								} else {
									// copy		
									pstmtCopyChild.setInt(1, prikey);
									pstmtCopyChild.setInt(2, childSourcekeys.get(i));
									log.info("Copy from " + childSourcekeys.get(i) + " to new parent " + prikey + " : " + pstmtCopyChild.toString());
									pstmtCopyChild.executeUpdate();
									copiedSourceKeys.add(childSourcekeys.get(i));
								}
								
							}
							
						} else if(replaceTables.contains(tableName)) {
							log.info("==================== Replacing " + childSourcekeys.size() + " records from " + tableName + " to " + childPrikeys.size() + " records");
							
							for(int i = 0; i < childSourcekeys.size(); i++) {
								if(i < childPrikeys.size()) {
									// merge
									log.info("Replace from " + childSourcekeys.get(i) + " to " + childPrikeys.get(i));
									if(child_f_id > 0) {
										subFormChanges.add(mergeRecords(
												sd,
												cResults, 
												tableName, 
												childPrikeys.get(i), 
												childSourcekeys.get(i), 
												true,    // Doing a replace so set replace to true
												child_f_id,
												true,
												groupSurveyIdent));  
									}
								} else {
									// Record the dropped record		
									if(child_f_id > 0) {
										subFormChanges.add(getChangeRecord(
												sd,
												cResults, 
												tableName, 
												childSourcekeys.get(i), false, child_f_id));
									}
											
								}
							}
							if(childPrikeys.size() > childSourcekeys.size()) {
								for(int i = childSourcekeys.size(); i < childPrikeys.size(); i++) {
									// Record the added record	
									if(child_f_id > 0) {
										subFormChanges.add(getChangeRecord(
												sd,
												cResults, 
												tableName, 
												childPrikeys.get(i), false, child_f_id));
									}
									
								}
							}
							
						} else {
							// Add. Therefore append old records to new
							for(int i = 0; i < childSourcekeys.size(); i++) {
								pstmtCopyChild.setInt(1, prikey);
								pstmtCopyChild.setInt(2, childSourcekeys.get(i));
								pstmtCopyChild.executeUpdate();
								copiedSourceKeys.add(childSourcekeys.get(i));
							}
							for(int i = 0; i < childPrikeys.size(); i++) {
								// Record the added records	
								if(child_f_id > 0) {
									subFormChanges.add(getChangeRecord(
											sd,
											cResults, 
											tableName, 
											childPrikeys.get(i), false, child_f_id));
								}
								
							}
							
						} 
						
						// Add the subform changes to the change record
						if(hasSubFormChanges(subFormChanges)) {
							changes.add(new DataItemChange(formname, formname, subFormChanges));
						}
						
						/*
						 * Restore child entries for source survey
						 * We do it in this way, ie move children to the new parent then copy back
						 *  so that we can preserve order of children which is determined by their primary keys
						 *  However if the record was merged then the order of the old records will be wrong
						 *  Deprecate -- source surveys will be deleted when change is working correctly
						 */
						pstmtCopyBack = cResults.prepareStatement(getCopyBackSql(cResults, tableName, sourceKey));
						for(int i = childSourcekeys.size() - 1; i >= 0; i--) {
							if(copiedSourceKeys.contains(childSourcekeys.get(i))) {
								pstmtCopyBack.setInt(1, childSourcekeys.get(i));
								log.info("Copy back: " + pstmtCopyBack.toString());
								pstmtCopyBack.executeUpdate();
							}
						}
						
					} else {
						log.info("Skipping update of parent keys for non existent table: " + tableName);
					}
				}
				
			} else {
				rem.writeEvent(sd, cResults, 
						RecordEventManager.CREATED, 
						RecordEventManager.STATUS_SUCCESS,
						user, table, 
						newInstance, 
						null, 					// Change object
						null, 					// Task object
						null,					// Notification object
						null, 
						sId, 
						null,
						0,
						0);	
			}
			
			if(changes != null) {
				// Save the changes
				Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				rem.writeEvent(sd, cResults, 
						RecordEventManager.CHANGES, 
						RecordEventManager.STATUS_SUCCESS,
						user, table, 
						newInstance, 
						gson.toJson(changes), 	// Change object
						null, 					// Task object
						null,					// Notification object
						null, 
						sId, 
						null,
						0,
						0);					
			}

			if(sourceKey > 0) {
				String value = (replace) ? localisation.getString("sub_rb") : localisation.getString("sub_mw");
				value +=  " " + prikey;
				
				UtilityMethodsEmail.markRecord(cResults, sd, localisation, table, 
						true, value, sourceKey, sId, f_id, true, false, user, true, tz, false);
			}

		} finally {
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {}
			if(pstmtSource != null) try{pstmtSource.close();}catch(Exception e) {}
			if(pstmtChildTables != null) try{pstmtChildTables.close();}catch(Exception e) {}
			if(pstmtChildTablesInGroup != null) try{pstmtChildTablesInGroup.close();}catch(Exception e) {}
			if(pstmtTableMerge != null) try{pstmtTableMerge.close();}catch(Exception e) {}
			if(pstmtChildKeys != null) try{pstmtChildKeys.close();}catch(Exception e) {}
			if(pstmtCopyChild != null) try{pstmtCopyChild.close();}catch(Exception e) {}
			if(pstmtCopyBack != null) try{pstmtCopyBack.close();}catch(Exception e) {}
			if(pstmtFormDetails != null) try{pstmtFormDetails.close();}catch(Exception e) {}
		}

	}

	private boolean hasSubFormChanges(ArrayList<ArrayList<DataItemChange>> subFormChanges) {
		
		if(subFormChanges.size() > 0) {
			for(ArrayList<DataItemChange> recChanges : subFormChanges) {
				if(recChanges.size() > 0) {
					return true;
				}			
			}
		}
		return false;
	}
	
	/*
	 * Merge records in a table
	 * If replace is set then for questions that are in the submitting survey the merge is not applied
	 *  - This would mean that an update can set a value to null within its own scope
	 */
	private ArrayList<DataItemChange> mergeRecords(
			Connection sd,
			Connection cRel, 
			String table, 
			int prikey, 
			int sourceKey, 
			boolean replace,
			int f_id,
			boolean subform,
			String groupSurveyIdent) throws SQLException {
		
		ArrayList<DataItemChange> changes = new ArrayList<DataItemChange>();
		
		// Merge the non meta data and the human readable key (HRK)
		StringBuffer sqlCols = new StringBuffer("select column_name from information_schema.columns where table_name = ? "
				+ "and (column_name not like '\\_%' or column_name = '_hrk') "
				+ "and column_name != 'prikey' "
				+ "and column_name != 'parkey' "
				+ "and column_name != 'instanceid'");
		PreparedStatement pstmtCols = null;
		
		String sqlSubmissionCols = "select column_name, qtype, qName from question where f_id = ?";
		PreparedStatement pstmtSubmissionCols = null;
		
		PreparedStatement pstmtGetTarget = null;
		PreparedStatement pstmtGetSource = null;
		PreparedStatement pstmtGetTargetGeom = null;
		PreparedStatement pstmtGetSourceGeom = null;
		PreparedStatement pstmtUpdateTarget = null;
		
		HashMap<String, String> subCols = new HashMap<> (); 
		HashMap<String, String> subColNames = new HashMap<> (); 
		
		try {
			/*
			 * Get details on the columns that are in the submitting survey
			 */
			pstmtSubmissionCols = sd.prepareStatement(sqlSubmissionCols);
			pstmtSubmissionCols.setInt(1, f_id);
			ResultSet rsSubs = pstmtSubmissionCols.executeQuery();
			while(rsSubs.next()) {
				subCols.put(rsSubs.getString(1), rsSubs.getString(2));
				subColNames.put(rsSubs.getString(1), rsSubs.getString(3));
			}
			
			/*
			 * For each column in the table apply merge / replace and get changes
			 */
			pstmtCols = cRel.prepareStatement(sqlCols.toString());
			pstmtCols.setString(1, table);
			
			ResultSet rsCols = pstmtCols.executeQuery();
			int count = 0;
			while(rsCols.next()) {
				String col = rsCols.getString(1);

				String sqlGetTarget = "select " + col + " from " + table + " where prikey = ?";
				String sqlGetTargetGeom = "select ST_AsGeoJSON(" + col + ") from " + table + " where prikey = ?";
	
				if(pstmtGetTarget != null) try{pstmtGetTarget.close();}catch(Exception e) {}
				pstmtGetTarget = cRel.prepareStatement(sqlGetTarget);
				
				if(pstmtGetSource != null) try{pstmtGetSource.close();}catch(Exception e) {}
				pstmtGetSource = cRel.prepareStatement(sqlGetTarget);
				
				pstmtGetTarget.setInt(1, prikey);
				ResultSet rsGetTarget = pstmtGetTarget.executeQuery();
				if(rsGetTarget.next()) {
					String val = rsGetTarget.getString(1);
					String subColType = subCols.get(col);
					String qName = subColNames.get(col);
	
					/*
					 * Apply the merge if the policy is not replace or this is a question that is not in the submitting form
					 */
					if(( !replace || subColType == null) && (val == null || val.trim().length() == 0)) {
	
						String sqlUpdateTarget = "update " + table 
								+ " set " + col + " = (select " + col 
									+ " from " + table 
									+ " where prikey = ?) "
								+ "where prikey = ?";
						if(pstmtUpdateTarget != null) try{pstmtUpdateTarget.close();}catch(Exception e) {}
						pstmtUpdateTarget = cRel.prepareStatement(sqlUpdateTarget);
						pstmtUpdateTarget.setInt(1, sourceKey);
						pstmtUpdateTarget.setInt(2, prikey);
						if(count++ == 0) {		// Only log the first merge
							log.info(("Merging col: " + pstmtUpdateTarget.toString()));
						}
						pstmtUpdateTarget.executeUpdate();
					}
					
					/*
					 * Get the change value if this question is in the submitting form
					 */
					if(subColType != null) {
						
						String oldVal = null;
						
						// If this is a geo question then replace values with geoJson
						if(GeneralUtilityMethods.isGeometry(subColType)) {
							if(pstmtGetSourceGeom != null) try{pstmtGetSourceGeom.close();}catch(Exception e) {}
							pstmtGetSourceGeom = cRel.prepareStatement(sqlGetTargetGeom);						
							if(pstmtGetTargetGeom != null) try{pstmtGetTargetGeom.close();}catch(Exception e) {}
							pstmtGetTargetGeom = cRel.prepareStatement(sqlGetTargetGeom);
							
							pstmtGetSourceGeom.setInt(1, sourceKey);
							ResultSet rsGetGeom = pstmtGetSourceGeom.executeQuery();						
							if(rsGetGeom.next()) {
								oldVal = rsGetGeom.getString(1);
							}
							
							pstmtGetTargetGeom.setInt(1, prikey);
							rsGetGeom = pstmtGetTargetGeom.executeQuery();						
							if(rsGetGeom.next()) {
								val = rsGetGeom.getString(1);
							}
							
						} else {
							// Just get the source in raw string format
							pstmtGetSource.setInt(1, sourceKey);
							ResultSet rsGetSource = pstmtGetSource.executeQuery();
							if(rsGetSource.next()) {
								oldVal = rsGetSource.getString(1);
							}
						}
						
						if(oldVal == null && val == null) {
							continue;
						} else if(oldVal == null || val == null || !oldVal.equals(val)) {
							changes.add(new DataItemChange(col, qName, subColType, val, oldVal));
						} else {
							continue;
						}
					}
				}
	
			}

			if(!subform) {
				GeneralUtilityMethods.continueThread(cRel, table, prikey, sourceKey);
			}

			
		} finally {
			if(pstmtCols != null) try{pstmtCols.close();}catch(Exception e) {}
			if(pstmtSubmissionCols != null) try{pstmtSubmissionCols.close();}catch(Exception e) {}
			if(pstmtGetTargetGeom != null) try{pstmtGetTargetGeom.close();}catch(Exception e) {}
			if(pstmtGetSourceGeom != null) try{pstmtGetSourceGeom.close();}catch(Exception e) {}
			if(pstmtGetTarget != null) try{pstmtGetTarget.close();}catch(Exception e) {}
			if(pstmtGetSource != null) try{pstmtGetSource.close();}catch(Exception e) {}
			if(pstmtUpdateTarget != null) try{pstmtUpdateTarget.close();}catch(Exception e) {}
		}
		
		return changes;

	}
	
	/*
	 * Get a change record for either a new subform record or deletion of a subform record
	 */
	private ArrayList<DataItemChange> getChangeRecord(
			Connection sd,
			Connection cRel, 
			String table, 
			int prikey, 
			boolean replace,
			int f_id) throws SQLException {
		
		ArrayList<DataItemChange> changes = new ArrayList<DataItemChange>();
		
		// TODO calculate the values in the deleted or added record
		
		return changes;

	}
	
	/*
	 * Method to discard new submission
	 * Called if the policy is discard
	 */
	private void discardTableContent(
			Connection cRel,
			String table,
			int prikey) throws SQLException, Exception {

		String sqlHrk = "select _hrk from " + table + " where prikey = ?";
		PreparedStatement pstmtHrk = null;

		String sqlSource = "select prikey from " + table + " where _hrk = ? "
				+ "and prikey != ? "
				+ "and _bad = 'false' "
				+ "order by prikey desc";
		PreparedStatement pstmtSource = null;

		String sqlCloseNew = "update " + table + " set _bad = 'true', _bad_reason = ? "
				+ "where prikey = ? ";
		PreparedStatement pstmtCloseNew = null;

		try {
			boolean exists = false;

			// Get the HRK that identifies duplicates
			String hrk = null;
			pstmtHrk = cRel.prepareStatement(sqlHrk);
			pstmtHrk.setInt(1, prikey);
			ResultSet rs = pstmtHrk.executeQuery();
			if(rs.next()) {
				hrk = rs.getString(1);
			}

			// Get the prikey of existing data
			int sourceKey = 0;
			pstmtSource = cRel.prepareStatement(sqlSource);
			pstmtSource.setString(1, hrk);
			pstmtSource.setInt(2, prikey);
			rs = pstmtSource.executeQuery();
			if(rs.next()) {
				sourceKey = rs.getInt(1);
				exists = true;

			}

			if(exists) {
				pstmtCloseNew = cRel.prepareStatement(sqlCloseNew);
				pstmtCloseNew.setString(1, localisation.getString("sub_dif") + " " + sourceKey);
				pstmtCloseNew.setInt(2, prikey);
				log.info(("Discarding new: " + pstmtCloseNew.toString()));
				pstmtCloseNew.executeUpdate();
			}


		} finally {
			if(pstmtHrk != null) try{pstmtHrk.close();}catch(Exception e) {}
			if(pstmtSource != null) try{pstmtSource.close();}catch(Exception e) {}
			if(pstmtCloseNew != null) try{pstmtCloseNew.close();}catch(Exception e) {}
		}

	}

	/*
	 * Get the primary key from the unique instance id
	 */
	private  int getKeyFromId(Connection connection, IE element, String instanceId) throws SQLException {

		int key = 0;	
		String tableName = element.getTableName();

		String sql = "select prikey, _bad from " + tableName + " where instanceid = ?";
		PreparedStatement pstmt = null;
		
		String sqlGetThread = "select _thread from " + tableName + " where instanceid = ?";
		PreparedStatement pstmtGetThread = null;
		
		String sqlGetLatest = "select prikey from " + tableName + " where _thread = ? order by prikey desc limit 1";
		PreparedStatement pstmtGetLatest = null;
		
		try {
			pstmt = connection.prepareStatement(sql);
			pstmt.setString(1, instanceId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				key = rs.getInt(1);
				boolean bad = rs.getBoolean(2);
				if(bad) {
					// Try to get the latest good version of the thread.  During transition this may not happen so ignore errors and fall back to the old
					// way which was to create a new entry
					try {
						pstmtGetThread = connection.prepareStatement(sqlGetThread);
						pstmtGetThread.setString(1, instanceId);
						ResultSet rst = pstmtGetThread.executeQuery();
						if(rst.next()) {
							String thread = rst.getString(1);
							if(thread != null) {
								pstmtGetLatest = connection.prepareStatement(sqlGetLatest);
								pstmtGetLatest.setString(1, thread);
								ResultSet rsl = pstmtGetLatest.executeQuery();
								if(rsl.next()) {
									key = rsl.getInt(1);
								}
							}
						}
					} catch (Exception ex) {
						// Report and ignore
						log.log(Level.SEVERE, ex.getMessage(), ex);
					}
				}
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}

		return key;

	}

	/*
	 * Generate the sql for the column names
	 * delete
	 */
	String addSqlColumns(List<IE> columns, Connection cResults, String tableName) {
		StringBuffer sql = new StringBuffer("");

		for(IE col : columns) {
			String colType = col.getQType();
			if(colType.equals("select") && !col.isCompressed()) {
				List<IE> options = col.getChildren();
				UtilityMethods.sortElements(options);
				HashMap<String, String> uniqueColumns = new HashMap<String, String> (); 
				for(IE option : options) {
					if(uniqueColumns.get(option.getColumnName()) == null) {
						uniqueColumns.put(option.getColumnName(), option.getColumnName());
						sql.append(",").append(option.getColumnName());
					}		
				}
			} else if(colType.equals("geopolygon") || colType.equals("geolinestring") || colType.equals("geopoint")
					|| colType.equals("geoshape") || colType.equals("geotrace") || colType.equals("geocompound")) {
				sql.append(",").append(col.getColumnName());
				if(colType.equals("geopoint") && GeneralUtilityMethods.hasColumn(cResults, tableName, col.getColumnName() + "_alt")) {
					// Geopoint also has altitude and accuracy
					sql.append(",").append(col.getColumnName()).append("_alt,").append(col.getColumnName()).append("_acc");
				}
			} else if(colType.equals("begin group")) {
				// Non repeating group, process these child columns at the same level as the parent
				sql.append(addSqlColumns(col.getQuestions(), cResults, tableName));
			} else {
				String colName = col.getColumnName();
				sql.append(",").append(colName);
			}				
		}

		return sql.toString();
	}

	/*
	 * Format the value into a string appropriate to its type
	 */
	String getDbValue(Connection sd, IE col, String sIdent, String device, String server, 
			boolean phoneOnly, Connection cResults, String tableName, int oId) {

		String qType = col.getQType();
		String value = col.getValue();	
		String colName = col.getColumnName();

		// If the deviceId is not in the form results add it from the form meta data
		if(colName != null && colName.equals("_device")) {
			if(value == null || value.trim().length() == 0) {
				value = device;
			}
		}

		if(phoneOnly) {
			if(qType.equals("string")) {
				value = "'xxxx'";			// Provide feedback to user that this value was withheld
			} else {
				value = null;				// For non string types will just set to null
			}
		} else {

			if(value != null) {			

				if(qType.equals("string") || qType.equals("calculate") || qType.equals("select1") || qType.equals("barcode") 
						|| qType.equals("acknowledge")
						|| qType.equals("note")
						|| qType.equals("select")
						|| qType.equals("rank")) {		// Compressed select
					// No change to value

				} else if(qType.equals("int") || qType.equals("decimal")) {
					if(value.length() == 0) {
						value = null;
					}

				} else if(qType.equals("date") || qType.equals("dateTime") || qType.equals("time") ) {
					
					// Hack to fix badly formatted types
					if(qType.equals("time")) {
						if(value.contains("T")) {
							String v[] = value.split("T");
							if(v.length > 1) {
								value = v[1];
							} else {
								value = "";
							}
						}
					}
					if(value.length() == 0) {
						value = null;
					}

				} else if(qType.equals("range") ) {
					if(value.length() == 0) {
						value = null;
					}

				} else if(GeneralUtilityMethods.isAttachmentType(qType)) {

					log.info("Processing media. Value: " + value);
					if(value.length() == 0) {
						value = null;
					
					} else {
						/*
						 * If this is a new file then rename the attachment to use a UUID
						 * Where this is an update to an existing survey and the file has not been re-submitted then 
						 * leave its value unchanged
						 */
						String srcName = value;

						log.info("Creating file: " + srcName);				

						File srcXmlFile = new File(gFilePath);
						File srcXmlDirFile = srcXmlFile.getParentFile();
						File srcPathFile = new File(srcXmlDirFile.getAbsolutePath() + "/" + srcName);

						value = GeneralUtilityMethods.createAttachments(
								sd,
								srcName, 
								srcPathFile, 
								gBasePath, 
								sIdent,
								null,
								mediaChanges,
								oId);		

					}
				} else if(qType.equals("geoshape") || qType.equals("geotrace") || qType.equals("geocompound")) { // TODO
					/*
					 * Extract the linestring from geocompound types
					 */
					if(qType.equals("geocompound")) {
						String components[] = value.split("#");
						for(int i = 0; i < components.length; i++) {
							if(components[i].startsWith("line:")) {
								String lineComponents [] = components[i].split(":");
								if(lineComponents.length > 1) {
									value = lineComponents[1];
									break;
								}
								
							}
						}
					}
					
					/*
					 * ODK polygon / linestring
					 * The coordinates are in a String separated by ;
					 * Each coordinate consists of space separated lat lon height accuracy
					 *   Actually I'm not sure about the last two but they will be ignored anyway
					 *   To store as a Point in the db this order needs to be reversed to (lon lat)
					 */
					int min_points = 3;
					StringBuilder ptString = new StringBuilder("");
					if(qType.equals("geotrace") || qType.equals("geocompound")) {
						min_points = 2;
					}

					String coords[] = value.split(";");
					if(coords.length >= min_points) {
						
						for(int i = 0; i < coords.length; i++) {
							String [] points = coords[i].trim().split(" ");
							if(points.length > 1) {
								if(i > 0) {
									ptString.append(",");
								}
								ptString.append(points[1]);
								ptString.append(" ");
								ptString.append(points[0]);
							} else {
								log.info("Error: " + qType + " Badly formed point." + coords[i]);
							}
						}
						
						value = ptString.toString();

					} else {
						value = null;
						log.info("Error: " + qType + " Insufficient points for " + qType + ": " + coords.length);
					}

				} else if(qType.equals("geopolygon") || qType.equals("geolinestring")) {
					// Complex types
					IE firstPoint = null;
					String ptString = "";
					List<IE> points = col.getChildren();
					int number_points = points.size();
					if(number_points < 3 && qType.equals("geopolygon")) {
						value = null;
						log.info("Error: Insufficient points for polygon." + number_points);
					} else if(number_points < 2 && qType.equals("geolinestring")) {
						value = null;
						log.info("Error: Insufficient points for line." + number_points);
					} else {
						for(IE point : points) {

							String params[] = point.getValue().split(" ");
							if(params.length > 1) {
								if(firstPoint == null) {
									firstPoint = point;		// Used to loop back for a polygon
								} else {
									ptString += ",";
								}
								ptString += params[1] + " " + params[0];
							}
						}
						if(ptString.length() > 0) {
							if(qType.equals("geopolygon")) {
								if(firstPoint != null) {
									String params[] = firstPoint.getValue().split(" ");
									if(params.length > 1) {
										ptString += "," + params[1] + " " + params[0];
									}
								}
								value = "ST_GeomFromText('POLYGON((" + ptString + "))', 4326)";
							} else if(qType.equals("geolinestring")) {
								value = "ST_GeomFromText('LINESTRING(" + ptString + ")', 4326)";
							}
						} else {
							value = null;
						}
					}

				}
			} 
		}

		return value;
	}

	private class GeopointComponents {
		String value = null;
		String alt = null;
		String acc = null;
	}
	private GeopointComponents getGeopointComponents(String in) {
		
		GeopointComponents components = new GeopointComponents();
		
		if(in != null) {
			String params[] = in.split(" ");
			
			if(params.length > 1) {
				try {
					components.value = String.valueOf(Double.parseDouble(params[1])) + " "
							+ String.valueOf(Double.parseDouble(params[0]));
	
					// Add altitude and accuracy
					if(params.length > 3) {
						components.alt = String.valueOf(Double.parseDouble(params[2]));
						components.acc = String.valueOf(Double.parseDouble(params[3]));
						
					}
	
				} catch (Exception e) {
					log.info("Error: Invalid geometry point detected: "  + in);
				}
	
			} else {
				log.info("Info: Empty geometry point detected: " + in);
			}
		}
		return components;
	}
	
	/*
	 * Check for duplicates specified using a column named instanceid or _instanceid
	 * Return true if this instance has already been uploaded
	 */
	private ArrayList<Integer> checkDuplicate(Connection cResults, String tableName, String uuid) {

		ArrayList<Integer> duplicateKeys = new ArrayList<Integer> ();
		PreparedStatement pstmt = null;
		

		if(uuid != null && uuid.trim().length() > 0) {
			uuid = uuid.replace("'", "''");	// Escape apostrophes
			try {

				String colTest1 = "select column_name from information_schema.columns " +
						"where table_name = '" + tableName + "' and column_name = '_instanceid'";
				String sql1 = "select prikey from " + tableName + " where _instanceid = '" + uuid + "' " +
						"order by prikey asc;";

				pstmt = cResults.prepareStatement(colTest1);
				// Check for duplicates with the old _instanceid
				ResultSet res = pstmt.executeQuery();
				if(res.next()) {
					// Has _instanceid
					try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
					pstmt = cResults.prepareStatement(sql1);
					res = pstmt.executeQuery();
					while(res.next()) {
						duplicateKeys.add(res.getInt(1));
					}
				}

				String colTest2 = "select column_name from information_schema.columns " +
						"where table_name = '" + tableName + "' and column_name = 'instanceid'";
				String sql2 = "select prikey from " + tableName + " where instanceid = '" + uuid + "' " +
						"order by prikey asc;";

				// Check for duplicates with the new instanceid
				try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
				pstmt = cResults.prepareStatement(colTest2);
				res = pstmt.executeQuery();
				if(res.next()) {
					// Has instanceid
					try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
					pstmt = cResults.prepareStatement(sql2);
					res = pstmt.executeQuery();
					while(res.next()) {
						duplicateKeys.add(res.getInt(1));
					}
				}


				if(duplicateKeys.size() > 0) {
					log.info("Submission has " + duplicateKeys.size() + " duplicates for uuid: " + uuid);
				} 

			} catch (SQLException e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			} finally {
				try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			}
		}

		return duplicateKeys;
	}


	private ArrayList<String> getColNames(List<IE> cols) {
		ArrayList<String> colNames = new ArrayList<String> ();
		for(IE col : cols) {
			
			String colType = col.getQType();
			if(colType != null) {
				if(colType.equals("begin group")) {
					colNames.addAll(getColNames(col.getChildren()));
				} else {
					colNames.add(col.getName());
				}
			}
		}
		return colNames;	
	}
	
	private String getCopyBackSql(Connection cRel, String tableName, int parkey) throws SQLException {
		StringBuffer sql = new StringBuffer("");
		StringBuffer cols = new StringBuffer("");
		
		String sqlCols = "select column_name from information_schema.columns where table_name = ? "
				+ "and column_name != 'prikey' "
				+ "and column_name != 'parkey' ";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = cRel.prepareStatement(sqlCols);
			pstmt.setString(1, tableName);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				cols.append(",").append(rs.getString(1));
			}
			sql.append("insert into ").append(tableName).append(" (parkey").append(cols).append(")");
			sql.append(" select ").append(parkey).append(cols).append(" from ").append(tableName).append(" where prikey = ?");
			
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
		
		return sql.toString();
	}
	
	
}
