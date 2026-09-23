package org.smap.sdal.model;

import java.util.HashMap;

/*
 * Organisation settings only an organisation administrator may change
 */
public class OrgAccessData {
	public boolean can_notify;
	public boolean can_use_api;
	public boolean can_submit;
	public boolean can_sms;
	public boolean email_task;
	public int refresh_rate;
	public HashMap<String, Integer> limits;
}
