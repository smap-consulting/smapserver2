package org.smap.sdal.model;

import java.sql.Timestamp;

public class TempUserFinal {
	public String userIdent;
	public String status;
	public Timestamp created;	
	
	public TempUserFinal(String userIdent, String status, Timestamp created) {
		this.userIdent = userIdent;
		this.status = status;
		this.created = created;
	}
}
