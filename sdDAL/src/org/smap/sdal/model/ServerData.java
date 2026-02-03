package org.smap.sdal.model;

public class ServerData {
	public String email_type;
	public String aws_region;
	public String smtp_host;
	public String email_domain;
	public String email_user;
	public String email_password;
	public int email_port;
	public String version;
	public String mapbox_default;
	public String google_key;
	public String maptiler_key;
	public String vonage_application_id;
	public String vonage_webhook_secret;
	public String sms_url;
	public int ratelimit;
	public double password_strength;
	public int keep_erased_days;
	public String css;
	public boolean sec_mgr_del;
	private int api_max_records;
	
	public void setMaxRecords(int v) {
		api_max_records = v;
	}
	
	public int getMaxRecords() {
		if(api_max_records < 0) {
			return 0;
		} else {
			return api_max_records;
		}
	}
}
