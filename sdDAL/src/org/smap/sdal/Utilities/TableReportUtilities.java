package org.smap.sdal.Utilities;

import java.util.ArrayList;
import java.util.ResourceBundle;

import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.SurveyViewDefn;
import org.smap.sdal.model.TableColumn;
import org.smap.sdal.model.TableReportsColumn;

public class TableReportUtilities {

	/*
	 * Get the columns for the table report
	 */
	public static ArrayList<TableReportsColumn> getTableReportColumnList(SurveyViewDefn mfc, 
			ArrayList<ArrayList<KeyValue>> dArray, 
			ResourceBundle localisation) {
		
		ArrayList<TableReportsColumn> cols = new ArrayList<> ();
		ArrayList<KeyValue> record = null;
		
		if(dArray.size() > 0) {
			 record = dArray.get(0);
		}
		
		for(int i = 0; i < mfc.columns.size(); i++) {
			TableColumn tc = mfc.columns.get(i);
			if(!tc.hide && tc.include) {
				int dataIndex = -1;
				if(record != null) {
					dataIndex = getDataIndex(record, tc.displayName);
				}
				cols.add(new TableReportsColumn(dataIndex, tc.displayName, tc.barcode, 
						tc.includeText, tc.type));
			}
		}
	
		
		return cols;
	}
	
	/*
	 * Get the index into the data set for a column
	 */
	private static int getDataIndex(ArrayList<KeyValue> record, String name) {
		int idx = -1;
		
		for(int i = 0; i < record.size(); i++) {
			if(record.get(i).k.equals(name)) {
				idx = i;
				break;
			}
		}
		return idx;
	}
}
