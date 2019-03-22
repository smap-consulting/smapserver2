package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.HashMap;

import com.google.gson.JsonElement;

public class Instance {
	public HashMap<String, String> values;
	public JsonElement the_geom; 
	public HashMap<String, ArrayList<Instance>> repeats;	
}
