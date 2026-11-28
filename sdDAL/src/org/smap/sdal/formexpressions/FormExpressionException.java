package org.smap.sdal.formexpressions;

/*
 * A form expression that cannot be read.  The key names a message in SmapResources, filled with the parameters
 */
public class FormExpressionException extends Exception {

	private static final long serialVersionUID = 1L;

	public String key;
	public int position;		// Index of the character the problem is at, -1 if it is not at one place
	public String [] params;

	public FormExpressionException(String key, int position, String ... params) {
		super(key);
		this.key = key;
		this.position = position;
		this.params = params;
	}
}
