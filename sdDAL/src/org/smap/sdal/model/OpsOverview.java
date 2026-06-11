package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.List;

/*
 * The complete payload for the senior-manager Operations Overview (L0).
 * Built by OpsMonitorManager.getOverview() and returned by the /ops/overview endpoint.
 */
public class OpsOverview {
	public String generatedAt;					// ISO timestamp the snapshot was built
	public int unassignedCases;					// open cases not yet picked up by anyone
	public List<OpsKpi> kpis = new ArrayList<>();
	public List<OpsUnit> units = new ArrayList<>();
	public List<OpsAlert> alerts = new ArrayList<>();
	public List<OpsBacklogPoint> backlog = new ArrayList<>();
}
