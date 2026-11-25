package org.smap.sdal.formexpressions;

import java.util.ArrayList;
import java.util.List;

/*
 * A node of a parsed form expression
 */
public class ExprNode {

	public static final String OP = "op";			// value is the operator: or, and, =, !=, <, <=, >, >=, +, -, *, div, mod, |
	public static final String NEG = "neg";			// Unary minus
	public static final String NUMBER = "number";
	public static final String STRING = "string";
	public static final String QUESTION = "question";	// ${name}, value is the name
	public static final String COLUMN = "column";		// #{name}, a column of a csv or reference file
	public static final String VARIABLE = "variable";	// $name
	public static final String FUNCTION = "function";	// value is the function name, children are its arguments
	public static final String PATH = "path";		// value is the path as written, children are any expressions in it
	public static final String GROUP = "group";		// ( expression )

	public String kind;
	public String value;
	public int position;		// Index in the expression of the first character, from 0
	public List<ExprNode> children = new ArrayList<> ();

	public ExprNode(String kind, String value, int position) {
		this.kind = kind;
		this.value = value;
		this.position = position;
	}

	public ExprNode add(ExprNode child) {
		children.add(child);
		return this;
	}
}
