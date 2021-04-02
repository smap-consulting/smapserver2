package org.smap.sdal.model;

/*
 * Manifest data passed back to devices
 */
public class ManifestDevice {			
	public String filename;
	public String hash;
	public String downloadUrl;
	
	public ManifestDevice(String f, String h, String u) {
		filename = f;
		hash = h;
		downloadUrl = u;
	}
}
