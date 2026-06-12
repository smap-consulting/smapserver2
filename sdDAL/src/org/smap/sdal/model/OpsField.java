package org.smap.sdal.model;

/*
 * A single field (column name + value) of a case record shown in the ops case viewer.
 */
public class OpsField {
	public String name;
	public String value;

	public OpsField(String name, String value) {
		this.name = name;
		this.value = value;
	}
}
