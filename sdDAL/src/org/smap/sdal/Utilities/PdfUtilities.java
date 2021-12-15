package org.smap.sdal.Utilities;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.PDFTableManager;
import org.smap.sdal.model.DisplayItem;
import org.smap.sdal.model.DistanceMarker;
import org.smap.sdal.model.PdfMapValues;
import org.smap.sdal.model.PdfTrafficLightValues;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;

import com.itextpdf.text.Anchor;
import com.itextpdf.text.BadElementException;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.AcroFields;
import com.itextpdf.text.pdf.AcroFields.FieldPosition;
import com.itextpdf.text.pdf.ColumnText;
import com.itextpdf.text.pdf.PRStream;
import com.itextpdf.text.pdf.PdfCopy;
import com.itextpdf.text.pdf.PdfImportedPage;
import com.itextpdf.text.pdf.PdfName;
import com.itextpdf.text.pdf.PdfNumber;
import com.itextpdf.text.pdf.PdfObject;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PushbuttonField;
import com.itextpdf.text.pdf.RandomAccessFileOrArray;
import com.itextpdf.text.pdf.parser.LocationTextExtractionStrategy;
import com.itextpdf.text.pdf.parser.PdfImageObject;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;

public class PdfUtilities {

	private static Logger log =
			 Logger.getLogger(PDFTableManager.class.getName());
	
	private static LogManager lm = new LogManager();		// Application log
	
	public static void addImageTemplate(AcroFields pdfForm, String fieldName, String basePath, 
			String value, String serverRoot, PdfStamper stamper, Font symbols_font,
			boolean stretch) throws IOException, DocumentException {
		
		PushbuttonField ad = pdfForm.getNewPushbuttonFromField(fieldName);
		if(ad != null) {
			ad.setLayout(PushbuttonField.LAYOUT_ICON_ONLY);
			ad.setProportionalIcon(!stretch);
			try {
				File f = new File(basePath + "/" + value);
				if(f.exists()) {
					ad.setImage(Image.getInstance(basePath + "/" + value));
				} else {
					// must be on s3
					ad.setImage(Image.getInstance(serverRoot + "/" + value));
				}
				pdfForm.replacePushbuttonField(fieldName, ad.getField());
			} catch (Exception e) {
				log.info("Error: Failed to add image " + basePath + "/" + value + " to pdf: " + e.getMessage());
			}
			
			log.info("Adding image to: " + fieldName);
		} else {
	
			String imageUrl = serverRoot + value;

			List<FieldPosition> posList = pdfForm.getFieldPositions(fieldName);
			if(posList == null) {
				log.info("Field not found for: " + fieldName);
			} else {
				Rectangle targetPosition = posList.get(0).position;
				int page = pdfForm.getFieldPositions(fieldName).get(0).page;
			    Anchor url = new Anchor("\uf08e", symbols_font);
			    url.setReference(imageUrl);
			    ColumnText data = new ColumnText(stamper.getOverContent(page));
			
			    data.setSimpleColumn(url, targetPosition.getLeft(), targetPosition.getBottom(), targetPosition.getRight(), targetPosition.getTop(), 
			    		(targetPosition.getHeight() + symbols_font.getSize()) / 2, Element.ALIGN_CENTER);
			    data.go();
			}
		}
	}
	
	public static void addMapImageTemplate(AcroFields pdfForm, PushbuttonField ad, String fieldName, Image img, boolean stretch) throws IOException, DocumentException {
		
		if(ad != null) {
			ad.setLayout(PushbuttonField.LAYOUT_ICON_ONLY);
			ad.setProportionalIcon(!stretch);
			try {
				ad.setImage(img);
			} catch (Exception e) {
				log.info("Error: Failed to add image to pdf: " + e.getMessage());
			}
			pdfForm.replacePushbuttonField(fieldName, ad.getField());
			log.info("Adding image to: " + fieldName);
		} 
	}
	
	/*
	 * Convert geospatial data into an map image
	 */
	public static Image getMapImage(Connection sd, 
			String map, 
			String account,
			PdfMapValues mapValues, 
			String location, 
			String zoom,
			String mapbox_key,
			int sId,
			String user,
			String markerColor,
			String basePath) throws BadElementException, MalformedURLException, IOException, SQLException {
		
		Image img = null;
		
		StringBuffer url = new StringBuffer();
		boolean getMap = false;
		url.append("https://api.mapbox.com/styles/v1/");
		if(account != null) {
			url.append(account).append("/");	// Mapbox username that owns the style
		} else {
			url.append("mapbox").append("/");	// Mapbox username that owns the style
		}
		
		if(map != null && !map.equals("none")) {
			url.append(map);
		} else {
			url.append("streets-v11");	// default map
		}
		url.append("/static/");
		
		if((mapValues.hasGeometry() || mapValues.hasLine())) {
			
			url.append("geojson(")
				.append(URLEncoder.encode(createGeoJsonMapValue(mapValues, markerColor), "UTF-8"))
				.append(")/");
			if(zoom != null && zoom.trim().length() > 0) {
				String centroidValue = mapValues.geometry;
				if(centroidValue == null) {
					centroidValue = mapValues.startGeometry;
				}
				url.append(GeneralUtilityMethods.getGeoJsonCentroid(centroidValue) + "," + zoom);
			} else if(location != null) {
				url.append(location);
			} else {
				url.append("auto");
			}
			url.append("/");
			getMap = true;
		} else {
			// Attempt to get default map boundary from appearance
			if(location != null) {
				url.append(location);
				url.append("/");
				getMap = true;
			}					
		}
		
		if(getMap && mapbox_key == null) {
			log.info("Mapbox key not specified.  PDF Map not created");
		} else if(getMap) {;
			url.append("500x300?access_token=");
			url.append(mapbox_key);
			try {
				log.info("Mapbox API call: " + url);
				
				/*
				 * There is a problem with passing a URL to the IText getInstance function as
				 * it will cause two mapbox requests to be recorded resulting in additional charges
				 * Instead download the imag first then add it to the PDF as a file
				 */
				URL mapboxUrl = new URL(url.toString());
				BufferedImage tempImg = ImageIO.read(mapboxUrl);
				File file = new File(basePath + "/temp/pdfmap_" + UUID.randomUUID() + ".png");
				ImageIO.write(tempImg, "png", file);			       
				img = Image.getInstance(file.getAbsolutePath());
			    
				lm.writeLog(sd, sId, user, LogManager.MAPBOX_REQUEST, map, 0, null);
			} catch (Exception e) {
				log.log(Level.SEVERE, "Exception", e);
			}
		} 
		
		return img;
	}
	
	/*
	 * Convert geospatial data into an abstract image
	 */
	public static Image getLineImage(Connection sd, 
			PdfMapValues mapValues,
			PdfTrafficLightValues tlValues,
			int sId,
			String user,
			DisplayItem di,
			String basePath,
			Float width,
			Float height) throws BadElementException, MalformedURLException, IOException, SQLException, TranscoderException {
		
		Image img = null;
	
		int margin = 10;	
		String fontSize = "8";
		
        // Add the faults
		PreparedStatement pstmt = null;;
		OutputStream ostream = null;
		DOMImplementation impl = SVGDOMImplementation.getDOMImplementation();
		
		try {
			
			String svgNS = SVGDOMImplementation.SVG_NAMESPACE_URI;
			Document doc = impl.createDocument(svgNS, "svg", null);

			// Get the root element (the 'svg' element).
			org.w3c.dom.Element svgRoot = doc.getDocumentElement();
			
			// Set the width and height attributes on the root 'svg' element.
			svgRoot.setAttributeNS(null, "width", String.valueOf(width));
			svgRoot.setAttributeNS(null, "height",  String.valueOf(height));
			
			org.w3c.dom.Element mainLine = doc.createElementNS(svgNS, "line");
			mainLine.setAttribute("id", "mainLine");
			mainLine.setAttribute("x1",String.valueOf(margin));
			mainLine.setAttribute("y1",String.valueOf(height / 2));
			mainLine.setAttribute("x2",String.valueOf(width - margin));
			mainLine.setAttribute("y2",String.valueOf((height / 2)));
			mainLine.setAttribute("stroke", "black");			
			svgRoot.appendChild(mainLine);
			
			// Add start and end of line
			org.w3c.dom.Text p1t = doc.createTextNode("1");
			org.w3c.dom.Element p1te = doc.createElementNS(svgNS,"text");
			p1te.setAttributeNS(null,"x", String.valueOf(margin - 5));   // Position should be half the width of the text    
			p1te.setAttributeNS(null,"y", String.valueOf((height / 2) - 2)); 
			p1te.setAttributeNS(null,"font-size",fontSize);		
			p1te.setAttribute("stroke", "red");	
			p1te.appendChild(p1t);
			svgRoot.appendChild(p1te);
			
			org.w3c.dom.Text p2t = doc.createTextNode("2");
			org.w3c.dom.Element p2te = doc.createElementNS(svgNS,"text");
			p2te.setAttributeNS(null,"x", String.valueOf(width - margin + 2));   // Position should be half the width of the text    
			p2te.setAttributeNS(null,"y", String.valueOf((height / 2) - 2)); 
			p2te.setAttributeNS(null,"font-size",fontSize);		
			p2te.setAttribute("stroke", "red");	
			p2te.appendChild(p2t);
			svgRoot.appendChild(p2te);
			
	        // Add the faults
	        if(mapValues.hasMarkers()) {
				Float lineDistance = getDistanceAlongLine(sd, mapValues, -1);
				for(int i = 0; i < mapValues.orderedMarkers.size(); i++) {
					addMarkerSvgImage(doc, svgRoot, svgNS, sd, mapValues, lineDistance, i, height, width, margin, fontSize);
				}
	        }
	        
	        // Add traffic lights
	        if(tlValues != null) {
	        	for(int i = 0; i < tlValues.lightValues.size(); i++) {
	        		addLightSvgImage(doc, svgRoot, svgNS, sd, tlValues.lightValues.get(i), i, height, width, margin, fontSize);
	        	}
	        }
			
	        /*
	         * Convert the SVG into an image
	         */
			PNGTranscoder t = new PNGTranscoder();
			
			// set the transcoding hints
			t.addTranscodingHint(PNGTranscoder.KEY_WIDTH, new Float(1000));
			t.addTranscodingHint(PNGTranscoder.KEY_ALLOWED_SCRIPT_TYPES, "*");
			t.addTranscodingHint(PNGTranscoder.KEY_CONSTRAIN_SCRIPT_ORIGIN, new Boolean(true));
			t.addTranscodingHint(PNGTranscoder.KEY_EXECUTE_ONLOAD, new Boolean(true));
			t.addTranscodingHint(PNGTranscoder.KEY_BACKGROUND_COLOR, Color.white);

			// create the transcoder input
			 TranscoderInput input = new TranscoderInput(doc);
			
			File file = new File(basePath + "/temp/pdfimage_" + UUID.randomUUID() + ".png");
			ostream = new FileOutputStream(file);
			TranscoderOutput output = new TranscoderOutput(ostream);
			t.transcode(input, output);
			ostream.flush();	
			
			img = Image.getInstance(file.getAbsolutePath());
		} finally {
			 if(pstmt != null) try{pstmt.close();} catch(Exception e) {}
			 if(ostream != null)  try{ostream.close();} catch(Exception e) {}
		}
		
		return img;
	}
	
	/*
	 * Add a marker to an SVG image
	 */
	private static void addMarkerSvgImage(Document doc, org.w3c.dom.Element svgRoot, String svgNS, Connection sd, PdfMapValues mapValues, Float lineDistance, int idx, 
			Float height, Float width, int margin, String fontSize) throws SQLException {
		
	    DecimalFormat decFormat = new DecimalFormat("0.00");
		
		Float distanceFromP1 = getDistanceAlongLine(sd, mapValues, idx);
		Float offset = distanceFromP1 * (width - (2 * margin)) / lineDistance;
		
		org.w3c.dom.Element tick1 = doc.createElementNS(svgNS, "line");
		tick1.setAttribute("id", "m" + idx + "_1");
		tick1.setAttribute("x1",String.valueOf(margin + offset));
		tick1.setAttribute("y1",String.valueOf(height / 2));
		tick1.setAttribute("x2",String.valueOf(margin + offset - 5));
		tick1.setAttribute("y2",String.valueOf((height / 2) - 5));
		tick1.setAttribute("stroke", "red");		
		svgRoot.appendChild(tick1);
		
		org.w3c.dom.Element tick2 = doc.createElementNS(svgNS, "line");
		tick2.setAttribute("id", "m" + idx + "_2");
		tick2.setAttribute("x1",String.valueOf(margin + offset));
		tick2.setAttribute("y1",String.valueOf(height / 2));
		tick2.setAttribute("x2",String.valueOf(margin + offset + 5));
		tick2.setAttribute("y2",String.valueOf((height / 2) - 5));
		tick2.setAttribute("stroke", "red");		
		svgRoot.appendChild(tick2);
		
		org.w3c.dom.Element circle1 = doc.createElementNS(svgNS, "circle");
		double cx = margin + offset;
		double cy = (height / 2) - 6;
		double radius = 2.0;
		circle1.setAttribute("id", "c" + idx + "_1");
		circle1.setAttribute("cx",String.valueOf(cx));
		circle1.setAttribute("cy",String.valueOf(cy));
		circle1.setAttribute("r","2");
		circle1.setAttributeNS(null, "style", "fill:white;");
		circle1.setAttribute("stroke", "red");		
		svgRoot.appendChild(circle1);
		
		org.w3c.dom.Element circle2 = doc.createElementNS(svgNS, "line");
		circle2.setAttribute("id", "c" + idx + "_2");
		circle2.setAttribute("x1",String.valueOf(cx - radius * Math.cos(45.0)));
		circle2.setAttribute("y1",String.valueOf(cy - radius * Math.sin(45.0)));
		circle2.setAttribute("x2",String.valueOf(cx + radius * Math.cos(45.0)));
		circle2.setAttribute("y2",String.valueOf(cy + radius * Math.sin(45.0)));
		circle2.setAttribute("stroke", "red");		
		svgRoot.appendChild(circle2);
		
		// Add lat long
		String coords = mapValues.getCoordinates(mapValues.markers.get(idx), true);
		if(coords != null) {
			String [] coordsArray = coords.split(",");
			
			if(coordsArray.length > 1) {
				org.w3c.dom.Text latNode = doc.createTextNode("lat: " + coordsArray[1]);
				org.w3c.dom.Element lat = doc.createElementNS(svgNS,"text");
				lat.setAttributeNS(null,"x", String.valueOf((cx - 30) > 0 ? cx - 30 : 0));   // Position should be half the width of the text    
				lat.setAttributeNS(null,"y", String.valueOf((height / 2) - 30)); 
				lat.setAttributeNS(null,"font-size",fontSize);			
				lat.appendChild(latNode);
				svgRoot.appendChild(lat);
			}
			
			org.w3c.dom.Text lonNode = doc.createTextNode("lon: " + coordsArray[0]);
			org.w3c.dom.Element lon = doc.createElementNS(svgNS,"text");
			lon.setAttributeNS(null,"x", String.valueOf((cx - 30) > 0 ? cx - 30 : 0));   // Position should be half the width of the text    
			lon.setAttributeNS(null,"y", String.valueOf((height / 2) - 20)); 
			lon.setAttributeNS(null,"font-size",fontSize);			
			lon.appendChild(lonNode);
			svgRoot.appendChild(lon);
		}
	  
	    if(idx == 0) {
			org.w3c.dom.Element d1 = doc.createElementNS(svgNS,"text");
			d1.setAttributeNS(null,"x", String.valueOf(margin));    
			d1.setAttributeNS(null,"y", String.valueOf((height / 2) + 12)); 
			d1.setAttributeNS(null,"font-size",fontSize);
			
			org.w3c.dom.Text tNode1 = doc.createTextNode(decFormat.format(distanceFromP1) + " m");
			d1.appendChild(tNode1);
			svgRoot.appendChild(d1);
	    }
	    
	    // Add Distance to P2
	    if(mapValues.markers.size() -1 == idx) {
	    	org.w3c.dom.Element d2 = doc.createElementNS(svgNS,"text");
			d2.setAttributeNS(null,"x", String.valueOf(width - (2 * margin) - 20));    
			d2.setAttributeNS(null,"y", String.valueOf((height / 2) + 12)); 
			d2.setAttributeNS(null,"font-size",fontSize);
			
			org.w3c.dom.Text tNode1 = doc.createTextNode(decFormat.format(lineDistance - distanceFromP1) + " m");
			d2.appendChild(tNode1);
			svgRoot.appendChild(d2);
	    }
	    
	}
	
	/*
	 * Add a traffic light
	 */
	private static void addLightSvgImage(Document doc, org.w3c.dom.Element svgRoot, String svgNS, Connection sd, ArrayList<String> colors, int idx, 
			Float height, Float width, int margin, String fontSize) throws SQLException {
		
	   
		float offset = (float) 10.0;
		double radius = 2.0;
		
		for(int i = 0; i < colors.size(); i++) {
		
			org.w3c.dom.Element circle1 = doc.createElementNS(svgNS, "circle");
			double cx = margin + offset + 2 * radius * i;
			double cy = (height / 2) - 6;
			
			circle1.setAttribute("id", "c" + idx + "_1");
			circle1.setAttribute("cx",String.valueOf(cx));
			circle1.setAttribute("cy",String.valueOf(cy));
			circle1.setAttribute("r","2");
			circle1.setAttributeNS(null, "style", "fill:" + colors.get(i));
			circle1.setAttribute("stroke", "red");		
			svgRoot.appendChild(circle1);
		
	    }
	 
	    
	}
	
	private static String createGeoJsonMapValue(PdfMapValues mapValues, String markerColor) {
		
		// GeoJson data - add styling
		StringBuffer out = new StringBuffer("");
		out.append("{\"type\":\"FeatureCollection\",\"features\":[");
		
		// Add the Geom if it is not null
		boolean addedGeom = false;
		if(mapValues.geometry != null) {
			if(markerColor == null) {
				markerColor = "f00";
			}
			out.append(addGeoJsonFeature(mapValues.geometry, markerColor, null));		
			addedGeom=true;
		}
		// Add the start Geo Point if it is not null
		if(mapValues.startGeometry != null) {
			if(addedGeom) {
				out.append(",");
			}
			out.append(addGeoJsonFeature(mapValues.startGeometry, "0f0", "harbor"));	
			addedGeom=true;
		}
		if(mapValues.hasLine()) {
			if(addedGeom) {			// line
				out.append(",");
			}
			out.append(addGeoJsonFeature(mapValues.getLineGeometryWithMarkers(-1), "00f", null));
				
			out.append(",");
			out.append(addGeoJsonFeature(mapValues.startLine, "f0f", "1"));
			out.append(",");
			out.append(addGeoJsonFeature(mapValues.endLine, "f0f", "2"));
			
			if(mapValues.hasMarkers()) {
				for(DistanceMarker marker : mapValues.orderedMarkers) {
					out.append(",");
					out.append(addGeoJsonFeature(marker.marker, "0ff", "roadblock"));
				}
			}
		}
		out.append("]}");
		
		return out.toString();
	}
	
	private static String addGeoJsonFeature(String coords, String markerColor, String icon) {
		
		StringBuffer out = new StringBuffer("{\"type\":\"Feature\",\"geometry\":");
		out.append(coords);
		out.append(",\"properties\":{");
		
		// properties
		out.append("\"marker-color\":\"#").append(markerColor).append("\"");		// Add marker color
		out.append(",");
		out.append("\"stroke\":\"#").append(markerColor).append("\"");				// Add stroke
		if(!coords.toLowerCase().contains("linestring")) {
			out.append(",");
			out.append("\"fill\":\"#").append(markerColor).append("\"");				// Add fill, unless its a line
		}
		if(icon != null) {
			out.append(",");
			out.append("\"marker-symbol\":\"").append(icon).append("\"");				// Add fill
		}
		
		out.append("}}");
		return out.toString();
	}
	
	/*
	 * Put the markers in the order that they should appear in the line
	 */
	public static void sequenceMarkers(PreparedStatement pstmt, PdfMapValues mapValues) throws SQLException {
		
		if(mapValues.hasMarkers() && mapValues.startLine != null && mapValues.orderedMarkers == null) {
			/*
			 * Get the distance of each marker from the first point, the markers will be put in order of increasing distance
			 */
			mapValues.orderedMarkers = new ArrayList<DistanceMarker> ();
			for(String marker : mapValues.markers) {
				Float distance = getDistance(pstmt, mapValues, mapValues.startLine, marker);
				mapValues.orderedMarkers.add(new DistanceMarker(distance, marker));
				//System.out.println("Marker: " + marker + " Distance: " + distance);
			}
			
			/*
			 * Sort the ordered markers
			 */
			Collections.sort(mapValues.orderedMarkers, new Comparator<DistanceMarker>() {
			    public int compare( DistanceMarker a, DistanceMarker b ) {
			    	return Float.compare(a.distance, b.distance);
			    }
			});
			
			//for(DistanceMarker dMarker : mapValues.orderedMarkers) {
			//	System.out.println("Distance Marker: " + dMarker.marker + " Distance: " + dMarker.distance);
			//}
			
		}
		
		
	}
	
	/*
	 * Get the distance in meters between two points
	 * Assume they are reasonably close together so use 
	 */
	private static Float getDistance(PreparedStatement pstmt, PdfMapValues mapValues, String p1, String p2) throws SQLException {
		
		Float distance = (float) -1.0;
		if(p1 != null && p2 != null) {
			String[] coords1 = mapValues.getCoordinates(p1, true).split(",");
			String[] coords2 = mapValues.getCoordinates(p2, true).split(",");
			
			if(coords1.length > 1 && coords2.length > 1) {
				
				pstmt.setString(1, "SRID=4326;POINT(" + GeneralUtilityMethods.getDouble(coords1[0]) + " " + GeneralUtilityMethods.getDouble(coords1[1]) + ")");
				pstmt.setString(2, "SRID=4326;POINT(" + GeneralUtilityMethods.getDouble(coords2[0]) + " " + GeneralUtilityMethods.getDouble(coords2[1]) + ")");
				
				log.info(pstmt.toString());
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()) {
					distance = rs.getFloat(1);
				}
			}
		}

		
		return distance;
	}
	
	/*
	 * Get the distance in meters along a linestring
	 * Pass in the index of the ordered marker to use as the end point
	 * If the index is -1 then do all points including the second pit
	 */
	private static Float getDistanceAlongLine(Connection sd, PdfMapValues mapValues, int idx) throws SQLException {
		
		Float distance = (float) -1.0;
		StringBuilder sb = new StringBuilder("select ST_Length(ST_GeomFromGeoJSON('");
		sb.append(mapValues.getLineGeometryWithMarkers(idx));
		sb.append("')::geography)");
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sb.toString());
				
			log.info(pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				distance = rs.getFloat(1);
			}
		} finally {
			 if(pstmt != null) try{pstmt.close();} catch(Exception e) {}
		}
		
		return distance;
	}
	
	// Uses code from https://stackoverflow.com/questions/20614350/compress-pdf-with-large-images-via-java
	public static void resizePdf(String src, OutputStream os) throws IOException, DocumentException {
	    
	    // Read the file
	    PdfReader reader = new PdfReader(src);
	    int n = reader.getXrefSize();
	    PdfObject object;
	    PRStream stream;

	    // Look for image and manipulate image stream
	    for (int i = 0; i < n; i++) {
	        object = reader.getPdfObject(i);
	        if (object == null || !object.isStream())
	            continue;
	        stream = (PRStream)object;
	       // if (value.equals(stream.get(key))) {
	        PdfObject pdfsubtype = stream.get(PdfName.SUBTYPE);
	        if (pdfsubtype != null && pdfsubtype.toString().equals(PdfName.IMAGE.toString())) {
	            PdfImageObject image = new PdfImageObject(stream);
	            BufferedImage bi = image.getBufferedImage();
	            if (bi == null) continue;
	            int width = bi.getWidth();
	            int height = bi.getHeight();
	            
	            /*
	             * Calculate amount of compression
	             */
	    	    float factor = 1.0f;
	            log.info("compressing.  width: " + width + " height: " + height);
	            if(width > 2000 && height > 2000) {
	            	factor = 0.25f;
	            } else if(width > 1000 && height > 1000) {
	            	factor = 0.5f;
	            } else if(width > 500 && height > 500) {
	            	factor = 0.8f;
	            }
	            width = (int) (bi.getWidth() * factor);
	            height = (int) (bi.getHeight() * factor);
	            
	            BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
	            AffineTransform at = AffineTransform.getScaleInstance(factor, factor);
	            Graphics2D g = img.createGraphics();
	            g.drawRenderedImage(bi, at);
	            ByteArrayOutputStream imgBytes = new ByteArrayOutputStream();
	            ImageIO.write(img, "JPG", imgBytes);
	            stream.clear();
	            stream.setData(imgBytes.toByteArray(), false, PRStream.BEST_COMPRESSION);
	            stream.put(PdfName.TYPE, PdfName.XOBJECT);
	            stream.put(PdfName.SUBTYPE, PdfName.IMAGE);
	            //stream.put(key, value);
	            stream.put(PdfName.FILTER, PdfName.DCTDECODE);
	            stream.put(PdfName.WIDTH, new PdfNumber(width));
	            stream.put(PdfName.HEIGHT, new PdfNumber(height));
	            stream.put(PdfName.BITSPERCOMPONENT, new PdfNumber(8));
	            stream.put(PdfName.COLORSPACE, PdfName.DEVICERGB);
	        }
	    }
	    // Save altered PDF
	    PdfStamper stamper = new PdfStamper(reader, os);
	    stamper.close();
	    reader.close();
	    
	}
	
	/*
	 * https://stackoverflow.com/questions/2464166/how-can-i-remove-blank-page-from-pdf-in-itext/3309453
	 */
	public static void removeBlankPages(String pdfSourceFile, String pdfDestinationFile) throws IOException, DocumentException
	{

		// step 1: create new reader
		PdfReader r = new PdfReader(pdfSourceFile);
		RandomAccessFileOrArray raf = new RandomAccessFileOrArray(pdfSourceFile);
		com.itextpdf.text.Document document = new com.itextpdf.text.Document(r.getPageSizeWithRotation(1));
		// step 2: create a writer that listens to the document
		PdfCopy writer = new PdfCopy(document, new FileOutputStream(pdfDestinationFile));
		// step 3: we open the document
		document.open();
		// step 4: we add content
		PdfImportedPage page = null;


		//loop through each page and if there is no text on the page then delete it
		for (int i=1;i<=r.getNumberOfPages();i++)
		{
			//get the page content
			ByteArrayOutputStream bs = new ByteArrayOutputStream();
			//write the content to an output stream
			
			String text = PdfTextExtractor.getTextFromPage(r, i, new LocationTextExtractionStrategy());
			
			//add the page to the new pdf
			if (text != null && text.length() > 0) {
				page = writer.getImportedPage(r, i);
				writer.addPage(page);
			}
			bs.close();
		}
		//close everything
		document.close();
		writer.close();
		raf.close();
		r.close();

	}
	
}
