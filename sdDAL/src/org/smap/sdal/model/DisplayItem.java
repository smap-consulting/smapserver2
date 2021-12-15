package org.smap.sdal.model;

import java.util.logging.Logger;

import com.itextpdf.text.BaseColor;

public class DisplayItem {
	
	private static Logger log =
			 Logger.getLogger(DisplayItem.class.getName());

	public int width;			// Width of entire cell item relative to enclosing item
	public int widthLabel = 5;	// Percentage width of label (If label is full width value appears below)
	public int space = 0;			// Space to be added before this item
	public String value;
	public double valueHeight = -1.0;
	public String name;
	public String hint;
	public String text;
	public String type;
	public boolean isSet = false;
	public boolean isNewPage = false;
	public BaseColor labelbg;
	public BaseColor valuebg;
	public String markerColor;
	public boolean labelcaps = false;
	public boolean labelbold = false;
	public boolean bs = false;
	public int fIdx = 0;
	public int qIdx = 0;
	public int rec_number = 0;
	public String map;
	public String account;
	public String location;
	public String zoom;					// Map zoom
	public boolean isBarcode = false;
	public boolean isSignature = false;
	public boolean isHyperlink = false;
	public boolean hideRepeatingLabels = false;
	public boolean showImage = false;		// Show label image instead of label text
	public boolean stretch = false;			// Stretch an image to fit a box
	public boolean tsep = false;		// Thousands separator
	public LineMap linemap;
	public TrafficLight trafficLight;
	
	public void debug() {
		log.info("======== Display Item:   width: " + width + "   value: " + value + " text: " + text + " : " + type  );
	}
}
