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

	public void addExpression(String expression) {
		this.expression = expression;
	}
}
