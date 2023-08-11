package org.smap.sdal.model;

import java.util.ArrayList;

public class BillingDetail {
	public static int SUBMISSIONS = 1;
	public static int DISK = 2;
	public static int REKOGNITION = 3;
	public static int STATIC_MAP = 4;
	public static int MONTHLY = 5;
	public static int TRANSLATE = 6;
	public static int TRANSCRIBE = 7;
	public static int TRANSCRIBE_MEDICAL = 8;
	public static int GOOGLE_STATIC_MAP = 9;
	public static int MAPTILER_STATIC_MAP = 10;
	public static int SENTIMENT = 11;
	
	public int oId;
	public int eId;
	public int ratesOid;
	public int ratesEid; 
	public String oName;
	public int year;
	public String month;				// Month of bill
	public String currency;			// Currency of bill
	public boolean enabled;			// Set true if billing is enabled
	
	public ArrayList<BillLineItem> line = null;

}
