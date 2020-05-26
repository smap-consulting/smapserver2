package org.smap.sdal.model;

public class LanguageCode {
	public String code;
	public String name;
	public boolean translate;
	public boolean transcribe;
	public boolean transcribe_medical;
	
	public LanguageCode (String code, String name, boolean translate, boolean transcribe, boolean transcribe_medical) {
		this.code = code;
		this.name = name;
		this.translate = translate;
		this.transcribe = transcribe;
		this.transcribe_medical = transcribe_medical;
	}
}
