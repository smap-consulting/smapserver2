package org.smap.sdal.model;

import java.util.HashMap;

/*
 * Organisation settings only an organisation administrator may change
 * Boxed so that a field the client did not send is null, and left as it is, rather than
 * read as false or 0
 */
public class OrgAccessData {
	public Boolean can_notify;
	public Boolean can_use_api;
	public Boolean can_submit;
	public Boolean can_sms;
	public Boolean email_task;
	public Integer refresh_rate;
	public HashMap<String, Integer> limits;
}
