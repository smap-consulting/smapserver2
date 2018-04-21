package org.smap.sdal.model;

import java.util.ArrayList;

public class BillingDetail {
	public int oId;
	public String oName;
	public int year;
	public String month;				// Month of bill
	public String currency;			// Currency of bill
	
	public ArrayList<BillLineItem> line = new ArrayList<BillLineItem> ();

}
