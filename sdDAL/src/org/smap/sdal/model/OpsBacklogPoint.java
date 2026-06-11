package org.smap.sdal.model;

/*
 * A single day in the case backlog / throughput trend.
 * net is the running change (opened - closed) accumulated by the caller.
 */
public class OpsBacklogPoint {
	public String date;		// yyyy-MM-dd
	public int opened;
	public int closed;
	public int net;			// cumulative (opened - closed) up to and including this day

	public OpsBacklogPoint(String date, int opened, int closed, int net) {
		this.date = date;
		this.opened = opened;
		this.closed = closed;
		this.net = net;
	}
}
