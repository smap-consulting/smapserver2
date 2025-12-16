package org.smap.sdal.model;

import java.util.Map;

public class MCPQuery {
	private String tool;
	private Map<String, Object> params;
	
	public String getTool() {
		return tool;
	}
	
	public void setTool(String tool) {
		this.tool = tool;
	}
	
	public Map<String, Object> getParams() {
		return params;
	}
	
	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
}
