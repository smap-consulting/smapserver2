package org.smap.sdal.model;

/*
 * A hexagonal grid that map panels can aggregate a survey bundle's data over
 */
public class RegionGrid {
	public int id;
	public String name;
	public int cell_size;		// Metres, the length of a hexagon's edge
	public String created_by;
}
