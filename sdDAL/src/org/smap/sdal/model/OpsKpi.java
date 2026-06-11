package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.List;

/*
 * A headline KPI tile on the operations overview (value + optional trend + RAG colour)
 */
public class OpsKpi {
	public String key;					// machine key e.g. open_cases
	public String label;				// localised label
	public long value;
	public String rag;					// green || amber || red || none
	public List<OpsTrendPoint> trend = new ArrayList<>();	// optional sparkline series

	public OpsKpi(String key, String label, long value, String rag) {
		this.key = key;
		this.label = label;
		this.value = value;
		this.rag = rag;
	}
}
