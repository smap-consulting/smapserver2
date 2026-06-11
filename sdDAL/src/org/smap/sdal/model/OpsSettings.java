package org.smap.sdal.model;

/*
 * Per-organisation Operations Monitor settings. Stored as a JSON blob in ops_settings.
 * Defaults match the agreed B.0.1 values (see docs/manager-reporting-solution.md).
 */
public class OpsSettings {
	public int staleIntervalDays = 14;	// open task/case older than this is "stale"
	public int trendDays = 30;			// window for sparklines / backlog / throughput
	public double ragAmberPct = 10.0;	// unit goes amber at this overdue %
	public double ragRedPct = 25.0;		// unit goes red at this overdue %
}
