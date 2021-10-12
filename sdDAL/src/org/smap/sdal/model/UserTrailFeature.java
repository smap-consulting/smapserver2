package org.smap.sdal.model;

import java.sql.Timestamp;

public class UserTrailFeature {	
	public int id;
	public double[] coordinates = new double[2];
	public Timestamp time;
	public long rawTime;
}
