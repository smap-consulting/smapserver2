package org.smap.sdal.model;

public class BillingDetail {
	public int oId;
	public String oName;
	public int year;
	public String month;				// Month of bill
	public String currency;			// Currency of bill
	
	public int submissions;
	public int freeSubmissions;
	public Double submissionUnitCost;
	public Double submissionAmount;
	
	public Double diskUsage;
	public int freeDisk;
	public Double diskUnitCost;
	public Double diskAmount;

}
