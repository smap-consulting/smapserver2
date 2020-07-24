package org.smap.sdal.model;

import java.util.HashMap;

public class HourlyLogSummaryItem {
	public int hour;
	public HashMap<String, Integer> events = new HashMap<> ();
}
