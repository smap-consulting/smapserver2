package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.constants.SmapServerMeta;

/*
 * Server Calculation
 */

public class ServerCalculation {
	private String expression;						// A simple calculation without expressions
	private ArrayList<Condition> conditions = null;	// Alternatively the conditions used to create the calculation

	public boolean hasConditions() {
		return conditions != null && conditions.size() > 0;
	}
	
	public void addExpression(String expression) {
		this.expression = expression;
	}
	
	public void addCondition(Condition c) {
		if(conditions == null) {
			conditions = new ArrayList<Condition>();
		}
		this.conditions.add(c);
	}
	
	public void addAllConditions(ArrayList<Condition> conds) {
		if(conditions == null) {
			conditions = new ArrayList<Condition>();
		}
		this.conditions.addAll(conds);
	}
	
	public String getExpression() {
		return expression;
	}
	
	public ArrayList<Condition> getConditions() {
		return conditions;
	}
	
	public void populateSql(SqlFrag sql, ResourceBundle localisation) throws Exception {
		// TODO add support for conditions
		if(hasConditions()) {
			sql.add("CASE");
			for(Condition c : conditions) {
				if(c.condition.toLowerCase().trim().equals("all")) {
					sql.add("ELSE");
					sql.addSqlFragment(c.value, false, localisation, 0);
				} else {
					sql.add("WHEN");
					sql.addSqlFragment(c.condition, true, localisation, 0);
					sql.add("THEN");
					sql.addSqlFragment(c.value, false, localisation, 0);
				}
			}
			sql.add("END");
		} else {
			sql.addSqlFragment(expression, false, localisation, 0);
		}
	}
}
