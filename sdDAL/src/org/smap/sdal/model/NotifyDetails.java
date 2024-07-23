package org.smap.sdal.model;

import java.util.ArrayList;

public class NotifyDetails {
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
}
