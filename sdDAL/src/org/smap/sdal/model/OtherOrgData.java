package org.smap.sdal.model;

/*
 * Boxed so that a field the client did not send is null, and left as it is, rather than
 * read as false or 0
 */
public class OtherOrgData {

	public Integer password_strength;
	public String locale;
	public String timeZone;
	public String map_source;
	public Boolean can_edit;
	public Boolean send_optin;
	public Boolean enable_redact;
	public Boolean notification_webform;

}
