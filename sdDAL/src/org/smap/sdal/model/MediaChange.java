package org.smap.sdal.model;

public class MediaChange {
	public String srcName;
	public String dstName;
	public String srcPath;
	
	public MediaChange(String srcName, String dstName, String srcPath) {
		this.srcName = srcName;
		this.dstName = dstName;
		this.srcPath = srcPath;
	}
}
