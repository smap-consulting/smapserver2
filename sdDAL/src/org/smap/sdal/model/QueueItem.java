package org.smap.sdal.model;

import java.sql.Timestamp;

public class QueueItem {
	public int id;
	public int oId;
	public boolean media;
	public String filepath;
	public String status;
	public String reason;
	public Timestamp created_time;
	public Timestamp processed_time;
}
