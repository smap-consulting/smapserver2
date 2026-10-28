package org.smap.sdal.model;

import org.smap.sdal.Utilities.Secrets;

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
	public String wa_access_token;
	public String wa_app_secret;
	public String wa_verify_token;
	public String wa_api_version;
	public String sms_url;
	public int ratelimit;
	public double password_strength;
	public int keep_erased_days;
	public String css;
	public boolean sec_mgr_del;
	public String turnstile_site_key;
	public String turnstile_secret_key;
	public String sharepoint_url;
	public String sharepoint_client_id;
	public String sharepoint_realm;
	public String sharepoint_cert_pem;
	public String sharepoint_auth_type;   // "s2s" (default) or "ntlm"
	public String sharepoint_username;
	public String sharepoint_password;
	public String sharepoint_domain;
	public boolean mcp_enabled;
	public String mcp_client_registration;
	public int mcp_max_rows;
	public int mcp_token_ttl;
	public boolean mcp_allow_access;
	public boolean self_registration;
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

	/*
	 * Replace the secrets with a placeholder before the settings are sent to the browser
	 */
	public void hideSecrets() {
		email_password = Secrets.hide(email_password);
		vonage_webhook_secret = Secrets.hide(vonage_webhook_secret);
		turnstile_secret_key = Secrets.hide(turnstile_secret_key);
		sharepoint_cert_pem = Secrets.hide(sharepoint_cert_pem);
		sharepoint_password = Secrets.hide(sharepoint_password);
		wa_access_token = Secrets.hide(wa_access_token);
		wa_app_secret = Secrets.hide(wa_app_secret);
	}

	/*
	 * Settings sent back from the browser keep the stored secret where it sent the placeholder
	 */
	public void keepSecrets(ServerData stored) {
		email_password = Secrets.keep(email_password, stored.email_password);
		vonage_webhook_secret = Secrets.keep(vonage_webhook_secret, stored.vonage_webhook_secret);
		turnstile_secret_key = Secrets.keep(turnstile_secret_key, stored.turnstile_secret_key);
		sharepoint_cert_pem = Secrets.keep(sharepoint_cert_pem, stored.sharepoint_cert_pem);
		sharepoint_password = Secrets.keep(sharepoint_password, stored.sharepoint_password);
		wa_access_token = Secrets.keep(wa_access_token, stored.wa_access_token);
		wa_app_secret = Secrets.keep(wa_app_secret, stored.wa_app_secret);
	}
}
