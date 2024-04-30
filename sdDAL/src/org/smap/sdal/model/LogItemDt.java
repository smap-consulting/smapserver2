package org.smap.sdal.model;
import java.sql.Timestamp;

/*
 * Details of a log entry as accessed from the client
 */
public class LogItemDt {
	
	public int id;
	public Timestamp log_time;
	public int sId;
	public String sName;
	public String userIdent;
	public String event;
	public String note;
	public String server;
}
