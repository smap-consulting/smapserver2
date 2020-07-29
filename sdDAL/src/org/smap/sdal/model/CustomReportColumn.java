package org.smap.sdal.model;

public class CustomReportColumn {
	public String column;
	public String heading;
	public int width;
	public boolean qrCode;
	
	public CustomReportColumn(String c, String h, int w, boolean qr) {
		column = c;
		heading = h;
		width = w;
		qrCode = qr;
	}
}
