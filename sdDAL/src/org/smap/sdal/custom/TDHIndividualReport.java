package org.smap.sdal.custom;

import java.util.ArrayList;

import org.apache.poi.ss.usermodel.Sheet;
import org.smap.sdal.model.QuestionLite;

public class TDHIndividualReport {
	public String name;
	public String tableName;
	public boolean reportEmpty;
	public Sheet sheet;
	public ArrayList<QuestionLite> questions = new ArrayList<> ();
	public ArrayList<TDHIndividualValues> values = null;
	
	public TDHIndividualReport(String name, String tableName) {
		this.name = name;
		this.tableName = tableName;
	}
}
