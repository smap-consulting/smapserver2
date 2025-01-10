package org.smap.sdal.model;

public class Notification {
	public int id;
	public boolean bundle;
	public String name;
	public String trigger;
	public String target;
	public int s_id;						// Deprecate in favour of sIdent
	public String bundle_ident;
	public String sIdent;
	public String filter;
	public String s_name;				// submission / update only
	public String updateSurvey;
	public String updateQuestion;		// Update only
	public String updateValue;	
	public boolean enabled;
	public String remote_user;
	public String remote_password;
	public String remote_host;
	public boolean update_password;
	public String instanceId;
	public int alert_id;
	public String alert_name;
	public NotifyDetails notifyDetails;
	
	public int tgId;				// reminder only
	public String period;			// reminder only
	public String tg_name;			// reminder only
	
	public String project;			// Reports only
	
	public int p_id;				// Project
	public String periodic_period;	// periodic
	public String periodic_time;	// GSON does not seem to allow specification of a time format
	public int periodic_week_day;	// Day of week for a weekly report
	public int periodic_month_day;	// Day of month for a monthly or yearly report
	public int periodic_month;		// Month for a yearly report
	public int r_id;				// Report Id
	
}
