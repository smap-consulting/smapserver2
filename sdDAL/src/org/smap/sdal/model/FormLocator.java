package org.smap.sdal.model;

import java.util.List;

public class FormLocator {
	public String ident;
	public String name;
	public int version;
	public String project;
	public int pid;				// project Id
	public boolean tasks_only;	// Set true if this form should not be available for ad-hoc tasks
	public boolean read_only;	// Set true if this form is read only
	public boolean search_local_data;
	public String url;
	public String manifestUrl;
	public boolean hasManifest;
	public boolean dirty;	// Set true if the manifest has been updated and the client should refresh
	public List<MediaFile> mediaFiles;
}
