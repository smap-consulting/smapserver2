package org.smap.sdal.model;

import java.sql.Timestamp;

public class WorkerInfo {
	public String hostname;
	public long pid;
	public String subscriber_type;
	public String queue_name;
	public Timestamp started_time;
	public Timestamp heartbeat;
}
