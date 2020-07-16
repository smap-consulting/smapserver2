package org.smap.sdal.model;

public class TableReportsColumn {
	public String displayName;
	public int dataIndex;
	public boolean barcode = false;
	public boolean includeText = false;
	public String type;
	
	public TableReportsColumn(int dataIndex, String n, boolean barcode, 
			boolean includeText, String type) {
		this.dataIndex = dataIndex;
		this.displayName = n;		
		this.barcode = barcode;	
		this.includeText = includeText;
		this.type = type;
	}
	
}
