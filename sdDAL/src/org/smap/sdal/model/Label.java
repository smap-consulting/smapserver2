package org.smap.sdal.model;

/*
 * Describes a label which may be for
 *   - A question text
 *   - A question hint
 *   - etc
 * Used for survey editing
 */
public class Label {
	public String text;
	public String hint;
	public String guidance_hint;
	public String constraint_msg;
	public String required_msg;
	public String image;
	public String bigImage;
	public String audio;
	public String video;
	public String imageUrl;
	public String bigImageUrl;
	public String audioUrl;
	public String videoUrl;
	public String imageThumb;
	public String audioThumb;
	public String videoThumb;
	
	public boolean hasLabels() {
		if(text == null && hint == null && guidance_hint == null && constraint_msg == null && required_msg == null
				&& image == null && audio == null && video == null) {
			return false;
		}
		return true;
	}
}
