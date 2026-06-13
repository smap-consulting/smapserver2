package org.smap.sdal.model;

/*
 * Workload / performance summary for one unit (role).
 * Only roles that currently own open work are returned.
 */
public class OpsUnit {
	public String role;
	public int openCases;
	public int openTasks;
	public int overdue;			// overdue tasks for this role
	public double overduePct;	// overdue / (openTasks) * 100
	public String rag;			// green || amber || red
	public boolean aggregate;	// true for the synthetic Unassigned / No-unit reconciliation rows (not a real role)
	public String itemType;		// for aggregate rows: the L2 list type to drill into (e.g. no_unit, unassigned)

	public OpsUnit(String role) {
		this.role = role;
	}
}
