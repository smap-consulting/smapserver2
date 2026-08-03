package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * An offline map layer (mbtiles) managed on the server and downloaded by FieldTask
 */
public class OfflineLayer {
	public int id;
	public String name;
	public String fileName;
	public long size;
	public String md5;
	public int version;
	public String description;
	public String url;					// Set when the layer is sent to a device
	public String changedBy;
	public String changedTs;
	public ArrayList<Integer> projects;	// Projects that this layer is assigned to
	public int devices;					// Devices that have reported holding the current version
}
