package org.smap.sdal.model;

import java.sql.Timestamp;
import java.util.ArrayList;

public class NotifyDetails {

	// SharePoint list target
	public String sp_list_title;
	public String sp_operation;             // "insert" or "update"
	public String sp_match_column;          // SP column used to find existing row (update only)
	public String sp_match_field;           // Smap field whose value is matched (update only)
	public ArrayList<SharePointColumnMap> sp_column_map;
	public ArrayList<String> emails;
	public int emailQuestion = 0;				// legacy question identifier
	public String emailQuestionName = null;
	public  String emailMeta;
	public String smtp_host = null;
	public String email_domain = null;
	public String from = null;
	public String subject = null;
	public String content = null;
	public String attach = null;
	public boolean include_references;
	public boolean emailAssigned;
	public boolean launched_only;
	public String callback_url;
	public int pdfTemplateId;
	public String survey_case;
	public String assign_question;
	public String ourNumber;			// For SMS / WhatsApp notifications
	public String msgChannel;
	public Timestamp ts;				// Timestamp of message
}
