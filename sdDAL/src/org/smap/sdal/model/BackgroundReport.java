package org.smap.sdal.model;

import java.sql.Timestamp;
import java.util.HashMap;

public class BackgroundReport {
	public int id;
	public int oId;
	public int uId;
	public int pId;
	public String userName;
	public String status;
	public String status_loc;
	public String status_msg;
	public String report_type;
	public String report_name;
	public String filename;
	public String tz;
	public String language;
	public Timestamp start_time;
	public HashMap<String, String> params;
}
