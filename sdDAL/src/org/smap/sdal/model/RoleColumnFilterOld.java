package org.smap.sdal.model;

/*
 * Form Class
 * Used for survey editing
 */
public class RoleColumnFilterOld implements Comparable {
	public int id;	
	
	public RoleColumnFilterOld() {
		
	}
	
public RoleColumnFilterOld(int id) {
		this.id = id;
	}

@Override
public int compareTo(Object o) {
	
	return id - (((RoleColumnFilterOld) o).id);
}
	
}
