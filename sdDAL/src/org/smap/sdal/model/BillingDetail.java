package org.smap.sdal.model;

import java.util.ArrayList;

public class BillingDetail {
	public static int USAGE = 1;
	public static int DISK = 2;
	public static int REKOGNITION = 3;
	public static int STATIC_MAP = 4;
	
	public int oId;
	public String oName;
	public int year;
	public String month;				// Month of bill
	public String currency;			// Currency of bill
	
	public ArrayList<BillLineItem> line = null;

}
