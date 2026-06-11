package org.smap.sdal.model;

/*
 * A single point in a trend / sparkline series (date + value)
 */
public class OpsTrendPoint {
	public String date;		// yyyy-MM-dd
	public long value;

	public OpsTrendPoint(String date, long value) {
		this.date = date;
		this.value = value;
	}
}
