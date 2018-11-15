package org.smap.sdal.model;

public class BillLineItem {
	
	public static int USAGE = 1;
	public static int DISK = 2;
	public static int REKOGNITION = 3;
	public static int STATIC_MAP = 4;
	
	public int item;
	public String name;
	public int quantity = 0;
	public int free = 0;
	public double unitCost = 0.0;
	public double amount = 0.0;

	public BillLineItem (String n, int q, int f, double uc, double amt) {
		name = n;
		quantity = q;
		free = f;
		unitCost = uc;
		amount = amt;
	}
}
