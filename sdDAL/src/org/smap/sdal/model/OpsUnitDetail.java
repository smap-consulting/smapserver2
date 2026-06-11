package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.List;

/*
 * The L1 single-unit (role) drill-down detail. Built by OpsMonitorManager.getUnitDetail().
 */
public class OpsUnitDetail {
	public String role;
	public String generatedAt;

	// Headline figures (same basis as the OpsUnit summary on L0)
	public int openCases;
	public int openTasks;
	public int overdue;
	public double overduePct;
	public String rag;

	// Average cycle times in days (null/0 when no completed work)
	public double avgCycleDaysCases;
	public double avgCycleDaysTasks;

	// 30-day throughput, two series
	public List<OpsTrendPoint> casesClosed = new ArrayList<>();
	public List<OpsTrendPoint> tasksCompleted = new ArrayList<>();

	// 30-day case backlog for this unit
	public List<OpsBacklogPoint> backlog = new ArrayList<>();

	// The unit's at-risk items (overdue tasks + stale cases)
	public List<OpsItem> atRisk = new ArrayList<>();

	public OpsUnitDetail(String role) {
		this.role = role;
	}
}
