package org.smap.sdal.Utilities;

import java.util.List;

/*
 * A change to a survey refused because of problems it would cause in the form, each described in words
 */
public class FormCheckException extends ApplicationException {

	private static final long serialVersionUID = 1L;

	public List<String> problems;

	public FormCheckException(String msg, List<String> problems) {
		super(msg);
		this.problems = problems;
	}
}
