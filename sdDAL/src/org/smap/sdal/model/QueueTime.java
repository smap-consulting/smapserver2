package org.smap.sdal.model;

import java.util.HashMap;

public class QueueTime {
	public String recorded_at;
	public HashMap<String, Queue> data;
	
	public QueueTime(String recorded_at, HashMap<String, Queue> data) {
		this.recorded_at = recorded_at;
		this.data = data;
	}
}
