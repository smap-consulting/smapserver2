package org.smap.sdal.model;

import java.util.HashMap;

public class AuditData {
	public static final String AUDIT_RAW_COLUMN_NAME = "_audit_raw";
	
	public HashMap<String, AuditItem> auditItems;
	public StringBuffer rawAudit;
}
