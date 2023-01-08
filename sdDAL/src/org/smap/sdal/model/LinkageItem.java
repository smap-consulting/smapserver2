package org.smap.sdal.model;


public class LinkageItem {
	
	public String sIdent;					// Keys
	public String colName;
	
	public String fp_location;				// Location on the body: hand or foot or unknown
	public String fp_side;					// left or right side of the body
	public int fp_digit;					// 0-5, 0 = thumb, 5 = palm
	
	public String fp_image;					// URL of image
	public String fp_iso_template;			// Fingerprint ISO data
	
	/*
	 * Create a Linkage item using an image
	 */
	public LinkageItem(String sIdent, String colName, String fp_location, String fp_side, int fp_digit, String fp_image, String fp_iso_template) {
		this.sIdent = sIdent;
		this.colName = colName;
		this.fp_location = fp_location;
		this.fp_side = fp_side;
		this.fp_digit = fp_digit;
		this.fp_image = fp_image;
		this.fp_iso_template = fp_iso_template;
		
		validateFingerprint();
	}
	
	private void validateFingerprint() {

		if(fp_digit < 0 || fp_digit > 5) {
			fp_location = "unknown";
			fp_digit = 0;
		} else if(fp_side != null &&
				!fp_side.equals("left") &&
				!fp_side.equals("right")) {
			fp_location = "unknown";
			fp_side = null;
		} else if(fp_location != null &&
				!fp_location.equals("hand") &&
				!fp_location.equals("foot")) {
			fp_location = "unknown";
		} 
		
		if(fp_location == null) {
			fp_location = "hand";	// Default to hand
		}
	}
}
