package org.smap.sdal.model;

import java.util.Map;

public class MCPResponse {
	private String status;
	private Object result;
	
	public MCPResponse(String status, Object result) {
		this.status = status;
		this.result = result;
		
	}
	public String getStatus() {
		return status;
	}
	
	public void setStatus(String status) {
		this.status = status;
	}
	
	public Object getResult() {
		return result;
	}
	
	public void setResult(Object result) {
		this.result = result;
	}
}
