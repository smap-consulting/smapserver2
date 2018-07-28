package utilities;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.ResourceBundle;

import org.smap.model.SurveyTemplate;
import org.smap.model.TableManager;
import model.FormDesc;


public class TableCreation {
	/*
	 * Create the results tables if they do not exist
	 */
	public static void createSurveyTables(Connection sd, Connection results, 
			ResourceBundle localisation, 
			int sId,
			ArrayList<FormDesc> formList,
			String sIdent) throws Exception {
		
		TableManager tm = new TableManager(localisation);
		FormDesc topForm = formList.get(0);
		
		SurveyTemplate template = new SurveyTemplate(localisation); 
		template.readDatabase(sd, sIdent, false);	
		tm.writeAllTableStructures(sd, results, sId, template,  0);
		
		boolean tableChanged = false;
		boolean tablePublished = false;
	
		// Apply any updates that have been made to the table structure since the last submission
	
		tableChanged = tm.applyTableChanges(sd, results, sId);
	
		// Add any previously unpublished columns not in a changeset (Occurs if this is a new survey sharing an existing table)
		tablePublished = tm.addUnpublishedColumns(sd, results, sId, topForm.table_name);			
		if(tableChanged || tablePublished) {
			for(FormDesc f : formList) {
				tm.markPublished(sd, f.f_id, sId);		// only mark published if there have been changes made
			}
		}
	}
}
