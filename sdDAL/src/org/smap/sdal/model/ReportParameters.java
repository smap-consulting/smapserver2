package org.smap.sdal.model;

import java.sql.Date;
import java.util.ArrayList;

public class ReportParameters {
	
	public int fId = 0;
	public boolean split_locn = false;
	public boolean merge_select_multiple = false;
	public String language = "none";
	public boolean exp_ro = false;
	public boolean embedImages = false;
	public boolean landscape = false;
	public boolean excludeParents = false;
	public boolean hxl = false;	
	public Date startDate = null;
	public Date endDate = null;	
	public int dateId = 0;
	public String filter = null;
	public boolean meta = false;
	public String tz = "UTC";
	
	public void setParameters(ArrayList<KeyValueSimp> parameters) {
		for(KeyValueSimp p : parameters) {
			if(p.k.equals("form")) {
				fId = Integer.parseInt(p.v);
			} else if(p.k.equals("split_locn")) {
				split_locn = Boolean.parseBoolean(p.v);
			} else if(p.k.equals("merge_select_multiple")) {
				merge_select_multiple = Boolean.parseBoolean(p.v);
			} else if(p.k.equals("language")) {
				language = p.v;
			} else if(p.k.equals("tz")) {
				tz = p.v;
			} else if(p.k.equals("exp_ro")) {
				exp_ro = Boolean.parseBoolean(p.v);
			} else if(p.k.equals("embed_images")) {
				embedImages = Boolean.parseBoolean(p.v);
			} else if(p.k.equals("excludeParents")) {
				excludeParents = Boolean.parseBoolean(p.v);
			} else if(p.k.equals("hxl")) {
				hxl = Boolean.parseBoolean(p.v);
			} else if(p.k.equals("startDate")) {
				startDate = Date.valueOf(p.v);
			} else if(p.k.equals("endDate")) {
				endDate = Date.valueOf(p.v);
			} else if(p.k.equals("dateId") && p.v != null) {
				dateId = Integer.parseInt(p.v);
			} else if(p.k.equals("filter")) {
				filter = p.v;
			} else if(p.k.equals("meta")) {
				meta = Boolean.parseBoolean(p.v);
			} else if(p.k.equals("landscape")) {
				landscape = Boolean.parseBoolean(p.v);
			} 
		}
	}
}
