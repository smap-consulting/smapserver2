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
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
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
import org.smap.sdal.model.Form;
import org.smap.sdal.model.LineMap;
import org.smap.sdal.model.MarkerLocation;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.PdfMapValues;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Result;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TrafficLightBulb;
import org.smap.sdal.model.TrafficLightQuestions;
import org.smap.sdal.model.TrafficLightValues;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;

import com.github.binodnme.dateconverter.converter.DateConverter;
import com.github.binodnme.dateconverter.utils.DateBS;
import com.itextpdf.text.Anchor;
import com.itextpdf.text.BadElementException;
import com.itextpdf.text.BaseColor;
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
	
	public static Image getMapImage(Connection sd, 
			String mapSource,
			String map, 
			String account,
			PdfMapValues mapValues, 
			String location, 
			String zoom,
			String mapbox_key,
			String google_key,
			int sId,
			String user,
			String markerColor,
			String basePath) throws Exception {
		
		if(mapSource.equals("mapbox")) {
			 return PdfUtilities.getMapImageMapbox(sd, map, 
					account, 
					mapValues,
					location, zoom, mapbox_key,
					sId,
					user,
					markerColor,
					basePath);
		} else if(mapSource.equals("google")) {
			 return PdfUtilities.getMapImageGoogle(sd, map, 
						account, 
						mapValues,
						location, zoom, 
						google_key,
						sId,
						user,
						markerColor,
						basePath);
		} else {
			throw new Exception("Mapsource not specified");
		}
	}
	
	/*
	 * Convert geospatial data into a mapbox map image
	 */
	private static Image getMapImageMapbox(Connection sd, 
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
				 * Instead download the image first then add it to the PDF as a file
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
	 * Convert geospatial data into a Google map image
	 */
	private static Image getMapImageGoogle(Connection sd, 
			String map, 
			String account,
			PdfMapValues mapValues, 
			String location, 
			String zoom,
			String google_key,
			int sId,
			String user,
			String markerColor,
			String basePath) throws BadElementException, MalformedURLException, IOException, SQLException {
		
		Image img = null;
		
		StringBuffer url = new StringBuffer();
		boolean hasParam = false;
		url.append("https://maps.googleapis.com/maps/api/staticmap");
		
		if((mapValues.hasGeometry())) {
			
			if(!hasParam) {
				url.append("?");
			} else {
				url.append("&");
			}
			//url.append("center=59.914063,10.737874");
			url.append(createMapValueGoogle(mapValues, markerColor));
			
			if(zoom != null && zoom.trim().length() > 0) {
				url.append("&zoom=" + zoom);
			} else {
				url.append("&zoom=12");
			}
		} 
		
		if(google_key == null) {
			log.info("Google key not specified.  PDF Map not created");
		} else {;
			url.append("&size=400x400&key=");
			url.append(google_key);
			try {
				log.info("Google API call: " + url);
				
				URL googleUrl = new URL(url.toString());
				BufferedImage tempImg = ImageIO.read(googleUrl);
				File file = new File(basePath + "/temp/pdfmap_" + UUID.randomUUID() + ".png");
				ImageIO.write(tempImg, "png", file);			       
				img = Image.getInstance(file.getAbsolutePath());
			    
				lm.writeLog(sd, sId, user, LogManager.GOOGLE_REQUEST, map, 0, null);
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
			TrafficLightValues tlValues,
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
				Float lineDistance = getLineDistance(sd, mapValues, -1);
				int index = 0;
				for(int i = 0; i < mapValues.orderedMarkers.size(); i++) {
					if(mapValues.geoCompound) {
						// Only call for fault types
						HashMap<String, String> properties = mapValues.orderedMarkers.get(i).properties;
						String type = properties.get("type");
						if(type == null || !type.equals("fault")) {
							continue;
						}
					}
					addMarkerSvgImage(doc, svgRoot, svgNS, sd, mapValues, lineDistance, i, index++, height, width, margin, fontSize);
				}
	        }
	        
	        // Add traffic lights
	        if(tlValues != null) {
	        	for(int i = 0; i < tlValues.lights.size(); i++) {
	        		addLightSvgImage(doc, svgRoot, svgNS, sd, 
	        				tlValues.lights.get(i),
	        				i, tlValues.lights.size(), height, width, margin, fontSize);
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
	private static void addMarkerSvgImage(Document doc, org.w3c.dom.Element svgRoot, String svgNS, Connection sd, PdfMapValues mapValues, Float lineDistance, 
			int markerIdx,
			int selectedMarkerIdx, 
			Float height, Float width, int margin, String fontSize) throws SQLException {
		
	    DecimalFormat decFormat = new DecimalFormat("0.00");
		
		Float distanceFromP1 = getLineDistance(sd, mapValues, markerIdx);
		Float offset = distanceFromP1 * (width - (2 * margin)) / lineDistance;
		
		org.w3c.dom.Element tick1 = doc.createElementNS(svgNS, "line");
		tick1.setAttribute("id", "m" + selectedMarkerIdx + "_1");
		tick1.setAttribute("x1",String.valueOf(margin + offset));
		tick1.setAttribute("y1",String.valueOf(height / 2));
		tick1.setAttribute("x2",String.valueOf(margin + offset - 5));
		tick1.setAttribute("y2",String.valueOf((height / 2) - 5));
		tick1.setAttribute("stroke", "red");		
		svgRoot.appendChild(tick1);
		
		org.w3c.dom.Element tick2 = doc.createElementNS(svgNS, "line");
		tick2.setAttribute("id", "m" + selectedMarkerIdx + "_2");
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
		double halfTextWidth = (selectedMarkerIdx == 0) ? 30 : 20;
		
		circle1.setAttribute("id", "c" + selectedMarkerIdx + "_1");
		circle1.setAttribute("cx",String.valueOf(cx));
		circle1.setAttribute("cy",String.valueOf(cy));
		circle1.setAttribute("r","2");
		circle1.setAttributeNS(null, "style", "fill:white;");
		circle1.setAttribute("stroke", "red");		
		svgRoot.appendChild(circle1);
		
		org.w3c.dom.Element circle2 = doc.createElementNS(svgNS, "line");
		circle2.setAttribute("id", "c" + selectedMarkerIdx + "_2");
		circle2.setAttribute("x1",String.valueOf(cx - radius * Math.cos(45.0)));
		circle2.setAttribute("y1",String.valueOf(cy - radius * Math.sin(45.0)));
		circle2.setAttribute("x2",String.valueOf(cx + radius * Math.cos(45.0)));
		circle2.setAttribute("y2",String.valueOf(cy + radius * Math.sin(45.0)));
		circle2.setAttribute("stroke", "red");		
		svgRoot.appendChild(circle2);
		
		// Add lat long
		String coords = mapValues.getCoordinates(mapValues.orderedMarkers.get(markerIdx).markerLocation, true);
		if(coords != null) {
			String [] coordsArray = coords.split(",");
			
			if(coordsArray.length > 1) {
				org.w3c.dom.Text latNode = doc.createTextNode((selectedMarkerIdx == 0 ? "lat: " : "") + coordsArray[1]);
				org.w3c.dom.Element lat = doc.createElementNS(svgNS,"text");
				lat.setAttributeNS(null,"x", String.valueOf((cx - halfTextWidth) > 0 ? cx - halfTextWidth : 0));   // Position should be half the width of the text    
				lat.setAttributeNS(null,"y", String.valueOf((height / 2) - 30)); 
				lat.setAttributeNS(null,"font-size",fontSize);			
				lat.appendChild(latNode);
				svgRoot.appendChild(lat);
			}
			
			org.w3c.dom.Text lonNode = doc.createTextNode((selectedMarkerIdx == 0 ? "lon: " : "") + coordsArray[0]);
			org.w3c.dom.Element lon = doc.createElementNS(svgNS,"text");
			lon.setAttributeNS(null,"x", String.valueOf((cx - halfTextWidth) > 0 ? cx - halfTextWidth : 0));   // Position should be half the width of the text    
			lon.setAttributeNS(null,"y", String.valueOf((height / 2) - 20)); 
			lon.setAttributeNS(null,"font-size",fontSize);			
			lon.appendChild(lonNode);
			svgRoot.appendChild(lon);
		}
	  
	    if(selectedMarkerIdx == 0) {
			org.w3c.dom.Element d1 = doc.createElementNS(svgNS,"text");
			d1.setAttributeNS(null,"x", String.valueOf(margin));    
			d1.setAttributeNS(null,"y", String.valueOf((height / 2) + 12)); 
			d1.setAttributeNS(null,"font-size",fontSize);
			
			org.w3c.dom.Text tNode1 = doc.createTextNode(decFormat.format(distanceFromP1) + " m");
			d1.appendChild(tNode1);
			svgRoot.appendChild(d1);
	    }
	    
	    // Add Distance to P2
	    if((!mapValues.geoCompound && mapValues.orderedMarkers.size() - 1 == markerIdx) ||
	    		(mapValues.geoCompound && mapValues.lastFaultIdx == markerIdx)) {
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
	private static void addLightSvgImage(Document doc, org.w3c.dom.Element svgRoot, String svgNS, Connection sd, 
			ArrayList<TrafficLightBulb> bulbs, 
			int idx, 
			int lightCount,
			Float height, Float width, int margin, String fontSize) throws SQLException {
		
	   
		float offset = (float) 10.0;
		float tlOffset; 
		int radius = 5;
		if(idx == 0 && lightCount > 1) {	// Position at beginning
			tlOffset = (float) 0.0;
		} else if(idx == 0 && lightCount == 1) {	// Position in the middle
			tlOffset = width / 2 - bulbs.size() * radius - margin - offset;
		} else if(idx == lightCount - 1) {			// Position at end
			tlOffset = width - margin - 2 * offset - 2 * bulbs.size() * radius;
		} else {
			tlOffset = width / (lightCount - idx) - bulbs.size() * radius - margin - offset;
		}
				
		
		for(int i = 0; i < bulbs.size(); i++) {
		
			
			org.w3c.dom.Element circle1 = doc.createElementNS(svgNS, "circle");
			double cx = margin + offset + tlOffset + 2 * radius * i;
			double cy = height - 30;
			
			circle1.setAttribute("id", "c" + idx + "_1");
			circle1.setAttribute("cx",String.valueOf(cx));
			circle1.setAttribute("cy",String.valueOf(cy));
			circle1.setAttribute("r", String.valueOf(radius));
			
			String color = bulbs.get(i).color;
			if(color.trim().length() == 0) {
				color = "white";
			}
			circle1.setAttribute("fill", color);	
			circle1.setAttribute("stroke", "black");		
			svgRoot.appendChild(circle1);
			
			String cross = bulbs.get(i).cross.toLowerCase();
			if(cross.equals("yes") || cross.equals("true") || cross.equals("1")) {
				org.w3c.dom.Element l1 = doc.createElementNS(svgNS, "line");
				l1.setAttribute("id", "c" + idx + "_2");
				l1.setAttribute("x1",String.valueOf(cx - radius * Math.cos(45.0)));
				l1.setAttribute("y1",String.valueOf(cy - radius * Math.sin(45.0)));
				l1.setAttribute("x2",String.valueOf(cx + radius * Math.cos(45.0)));
				l1.setAttribute("y2",String.valueOf(cy + radius * Math.sin(45.0)));
				l1.setAttribute("stroke", "black");		
				svgRoot.appendChild(l1);
				
				org.w3c.dom.Element l2 = doc.createElementNS(svgNS, "line");
				l2.setAttribute("id", "c" + idx + "_3");
				l2.setAttribute("x1",String.valueOf(cx + radius * Math.cos(45.0)));
				l2.setAttribute("y1",String.valueOf(cy - radius * Math.sin(45.0)));
				l2.setAttribute("x2",String.valueOf(cx - radius * Math.cos(45.0)));
				l2.setAttribute("y2",String.valueOf(cy + radius * Math.sin(45.0)));
				l2.setAttribute("stroke", "black");		
				svgRoot.appendChild(l2);
			}
			
			String label = bulbs.get(i).label;
			if(label.length() > 0) {
				org.w3c.dom.Element d1 = doc.createElementNS(svgNS,"text");
				d1.setAttributeNS(null,"x", String.valueOf(cx - radius + (label.length() == 1 ? radius - 2 : 0)));    
				d1.setAttributeNS(null,"y", String.valueOf(cy + 12 + radius)); 
				d1.setAttributeNS(null,"font-size",fontSize);
				
				org.w3c.dom.Text tNode1 = doc.createTextNode(label);
				d1.appendChild(tNode1);
				svgRoot.appendChild(d1);
			}
		
	    }
	 
	    
	}

	/*
	 * Add markers to google static map
	 */
	private static String createMapValueGoogle(PdfMapValues mapValues, String markerColor) {
		
		// GeoJson data - add styling
		StringBuffer out = new StringBuffer("");
		
		// Add the Geom if it is not null
		boolean addedGeom = false;
		if(mapValues.geometry != null && mapValues.geometry.trim().length() > 0) {
			if(markerColor == null) {
				markerColor = "f00";
			}

			if(addedGeom) {
				out.append("&");
			}
			out.append("center=")
			.append(GeneralUtilityMethods.getLatLngfromGeoJson(mapValues.geometry));
			addedGeom=true;
			
			if(addedGeom) {
				out.append("&");
			}
			out.append("markers=")
					.append(GeneralUtilityMethods.getLatLngfromGeoJson(mapValues.geometry));		
			
		}

		
		return out.toString();
	}

	private static String createGeoJsonMapValue(PdfMapValues mapValues, String markerColor) {
		
		// GeoJson data - add styling
		StringBuffer out = new StringBuffer("");
		out.append("{\"type\":\"FeatureCollection\",\"features\":[");
		
		// Add the Geom if it is not null
		boolean addedGeom = false;
		if(mapValues.geometry != null && mapValues.geometry.trim().length() > 0) {
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
			} else {
				out.append(addGeoJsonFeature(mapValues.getLineGeometryWithMarkers(-1), "00f", null));	// only add the line from the markers if a geometry is not available
				out.append(",");
			}
			
			out.append(addGeoJsonFeature(mapValues.startLine, "f0f", "1"));
			out.append(",");
			out.append(addGeoJsonFeature(mapValues.endLine, "f0f", "2"));
			
			if(mapValues.hasMarkers()) {
				for(DistanceMarker marker : mapValues.orderedMarkers) {
					out.append(",");
					out.append(addGeoJsonFeature(marker.markerLocation, "0ff", "roadblock"));
				}
			}
		}
		if(mapValues.geoCompound) {
			// Add the distance markers submitted from the compound widget
			int pitCount = 1;
			for(DistanceMarker marker : mapValues.orderedMarkers) {
				out.append(",");
				if(marker.properties.get("type").equals("fault")) {
					out.append(addGeoJsonFeature(marker.markerLocation, "0ff", "roadblock"));
				} else {
					out.append(addGeoJsonFeature(marker.markerLocation, "f0f", String.valueOf(pitCount++)));
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
	 * Get the line distance
	 */
	private static Float getLineDistance(Connection sd, PdfMapValues mapValues, int markerIdx) throws SQLException {
		Float distance = (float) -1.0;
		
		if(mapValues.geoCompound) {
			int idx1 = mapValues.idxStart;
			int idx2 = mapValues.idxEnd;
			if(markerIdx >= 0) {
				idx2 = mapValues.idxMarkers.get(markerIdx);
			}
			distance = getDistanceBetweenPoints(sd, mapValues, idx1, idx2);		// Geo compound
		} else {
			distance = getDistanceAlongLine(sd, mapValues, markerIdx);				// Constructed geometry
		}
		return distance;
	}
	
	/*
	 * Get coordinates of the marker with the specified type
	 * if the count is set to 1 the first occurence will be returned else if 2 the end occurence etc
	 */
	public static String getMarkerCoordinates(PdfMapValues mapValues, String type, int count) throws SQLException {

		count = count - 1;
		int pointIdx;
		String value = null;
		
		if(mapValues.geoCompound && mapValues.orderedMarkers != null) {
			int markerIdx = -1;
			for(int i = 0; i < mapValues.orderedMarkers.size(); i++) {
				DistanceMarker marker = mapValues.orderedMarkers.get(i);
				String mType = marker.properties.get("type");
				if(mType != null && type.equals(mType)) {
					if(count-- <= 0) {
						markerIdx = i;
						break;
					}
				}
			}
			
			if(markerIdx >= 0) {
				pointIdx = mapValues.idxMarkers.get(markerIdx);
				value = mapValues.getPointCoordinates(pointIdx);
			}

		} 
		return value;
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
	
	/*
	 * Get the distance in meters along a linestring
	 * Passes in the indexes of the two points that form a sub section of the line
	 */
	private static Float getDistanceBetweenPoints(Connection sd, PdfMapValues mapValues, int idx1, int idx2) throws SQLException {
		
		Float distance = (float) -1.0;
		
		StringBuilder sb = new StringBuilder("select ST_Length(ST_GeomFromGeoJSON('");
	
		sb.append(mapValues.getLineGeometryBetweenPoints(idx1, idx2));
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
	
	public static Question getQuestionFromResult(Connection sd, Survey survey, Result r, Form form) throws SQLException {

		Question question = null;
		if(r.qIdx >= 0) {
			question = form.questions.get(r.qIdx);
		} if(r.qIdx <= MetaItem.INITIAL_ID) {
			question = GeneralUtilityMethods.getPreloadAsQuestion(sd, survey.id, r.qIdx);	// A preload
		} else if(r.qIdx == -1) {
			question = new Question();													// Server generated
			question.name = r.name;
			question.type = r.type;
		}
		return question;
	}
	
	public static String getDateValue(DisplayItem di, String tz, String inValue, String type) throws ParseException {
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DateFormat dfDateOnly = new SimpleDateFormat("yyyy-MM-dd");
		
		String value = "";
		if(inValue != null) {
			Date date;
			String utcValue = inValue;
			if(type.equals("dateTime") || type.equals("timestamp")) {
				df.setTimeZone(TimeZone.getTimeZone("UTC"));
				date = df.parse(inValue);
				df.setTimeZone(TimeZone.getTimeZone(tz));
				value = df.format(date);
			} else {
				dfDateOnly.setTimeZone(TimeZone.getTimeZone("UTC"));
				date = dfDateOnly.parse(inValue);
				dfDateOnly.setTimeZone(TimeZone.getTimeZone(tz));
				value = dfDateOnly.format(date);
			}
			
			// If Bikram Sambat date output is required convert  
			if(di.bs) {
	
				Date nepalDate;
				
				log.info("utc value: " + utcValue);
				
				
				if(type.equals("dateTime") || type.equals("timestamp")) {
					df.setTimeZone(TimeZone.getTimeZone("UTC"));
					date = df.parse(utcValue);
					df.setTimeZone(TimeZone.getTimeZone(tz));
					value = df.format(date);
					log.info("xxxxxxxxx: " + value);
					df.setTimeZone(TimeZone.getTimeZone("UTC"));
					nepalDate = df.parse(value);
				} else {	
					dfDateOnly.setTimeZone(TimeZone.getTimeZone("UTC"));
					date = dfDateOnly.parse(utcValue);
					date.setHours(12);
					nepalDate = date;
				} 		
					
				log.info("Value: " + value);
				
				StringBuilder bsValue = new StringBuilder("");
				DateBS dateBS = DateConverter.convertADToBS(nepalDate);  //returns corresponding DateBS
				
				bsValue.append(dateBS.getYear())
				.append("/")
				.append(dateBS.getMonth() + 1)
				.append("/")
				.append(dateBS.getDay());
				
				if(type.equals("dateTime") || type.equals("timestamp")) {
					String [] components = value.split(" ");
					if(components.length > 1) {
						bsValue.append(" ")
						.append(components[1]);
					}				
				} 
	
				value = bsValue.toString();
			}
		}
		return value;
	}
	
	/*
	 * Set the attributes for this question from keys set in the appearance column
	 */
	public static void setQuestionFormats(String appearance, DisplayItem di) throws Exception {

		if(appearance != null) {
			String [] appValues = appearance.split(" ");
			for(int i = 0; i < appValues.length; i++) {
				String app = appValues[i].trim().toLowerCase();
				if(app.startsWith("pdflabelbg")) {
					setColor(app, di, true);
				} else if(app.startsWith("pdfvaluebg")) {
					setColor(app, di, false);
				} else if(app.startsWith("pdfmarkercolor")) {
					di.markerColor = getRGBColor(app);
				} else if(app.startsWith("pdflabelw")) {
					setWidths(app, di);
				} else if(app.startsWith("pdfheight")) {
					setHeight(app, di);
				} else if(app.startsWith("pdfspace")) {
					setSpace(app, di);
				} else if(app.equals("pdflabelcaps")) {
					di.labelcaps = true;
				} else if(app.equals("pdfbs")) {
					di.bs = true;
				} else if(app.equals("pdflabelbold")) {
					di.labelbold = true;
				} else if(app.startsWith("pdfmapsource_")) {			// map source
					String mapSource = getAppValue(app);
					di.mapSource = getAppValue(app);
				} else if(app.startsWith("pdfmap_")) {			// mapbox style map id
					String map = getAppValue(app);
					if(!map.equals("custom")) {
						di.map = map;
						di.account = "mapbox";
					}
				} else if(app.startsWith("pdflinemap") || app.startsWith("pdflineimage") || app.startsWith("pdflinelocation")) {		// Multiple points to be joined into a map or image
					di.linemap = new LineMap(getAppValueArray(app));
					if(app.startsWith("pdflinemap")) {
						di.linemap.type = "map";
					} else if(app.startsWith("pdflinelocation")) { 
						di.linemap.type = "location";
						di.markerLocation = new MarkerLocation(getAppValueArray(app));
					} else {
						di.linemap.type = "image";
					}
				} else if(app.startsWith("pdftl")) {		// Multiple points to be joined into a map or image
					if(di.trafficLight == null) {
						di.trafficLight = new TrafficLightQuestions();
					}
					di.trafficLight.addApp(getAppValueArray(app));
				} else if(app.startsWith("pdfaccount")) {			// mapbox account
					di.account = getAppValue(app);
				} else if(app.startsWith("pdflocation")) {
					di.location = getAppValue(app);			// lon,lat,zoom
				} else if(app.startsWith("pdfbarcode")) {
					di.isBarcode = true;		
				} else if(app.equals("pdfstretch")) {
					di.stretch = true;		
				} else if(app.startsWith("pdfzoom")) {
					di.zoom = getAppValue(app);		
				} else if(app.startsWith("pdfround")) {
					try {
						di.round = Integer.valueOf(getAppValue(app));	
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					}
				} else if(app.startsWith("pdfhyperlink")) {
					di.isHyperlink = true;		
				} else if(app.equals("signature")) {
					di.isSignature = true;		
				} else if(app.equals("pdfhiderepeatinglabels")) {
					di.hideRepeatingLabels = true;		
				} else if(app.equals("thousands-sep")) {
					di.tsep = true;		
				} else if(app.equals("pdfshowimage")) {
					di.showImage = true;		
				}
			}
		}
	}
	
	/*
	 * Get the color values for a single appearance value
	 * Format is:  xxxx_0Xrr_0Xgg_0xbb
	 */
	private static void setColor(String aValue, DisplayItem di, boolean isLabel) {

		BaseColor color = null;

		String [] parts = aValue.split("_");
		if(parts.length >= 4) {
			if(parts[1].startsWith("0x")) {
				color = new BaseColor(Integer.decode(parts[1]), 
						Integer.decode(parts[2]),
						Integer.decode(parts[3]));
			} else {
				color = new BaseColor(Integer.decode("0x" + parts[1]), 
						Integer.decode("0x" + parts[2]),
						Integer.decode("0x" + parts[3]));
			}
		}

		if(isLabel) {
			di.labelbg = color;
		} else {
			di.valuebg = color;
		}

	}
	
	/*
	 * Get the color values for a single appearance value
	 * Output is just the RGB value
	 * Format is:  xxxx_0Xrr_0Xgg_0xbb
	 */
	private static String getRGBColor(String aValue) {

		String rgbValue = "";

		String [] parts = aValue.split("_");
		if(parts.length >= 4) {
			rgbValue = parts[1] + parts[2] + parts[3];
		}
		return rgbValue;

	}
	
	private static String getAppValue(String aValue) {
		String [] parts = aValue.split("_");
		if(parts.length >= 2) {
			return parts[1];   		
		}
		else return null;
	}
	
	private static String[] getAppValueArray(String aValue) {
		return aValue.split("_");
	}
	
	/*
	 * Set the widths of the label and the value
	 * Appearance is:  pdflabelw_## where ## is a number from 0 to 10
	 */
	private static void setWidths(String aValue, DisplayItem di) {

		String [] parts = aValue.split("_");
		if(parts.length >= 2) {
			di.widthLabel = Integer.valueOf(parts[1]);   		
		}

		// Do bounds checking
		if(di.widthLabel < 0 || di.widthLabel > 10) {
			di.widthLabel = 5;		
		}
	}
	
	/*
	 * Set the height of the value
	 * Appearance is:  pdfheight_## where ## is the height
	 */
	private static void setHeight(String aValue, DisplayItem di) {

		String [] parts = aValue.split("_");
		if(parts.length >= 2) {
			di.valueHeight = Double.valueOf(parts[1]);   		
		}

	}

	/*
	 * Set space before this item
	 * Appearance is:  pdfheight_## where ## is the height
	 */
	private static void setSpace(String aValue, DisplayItem di) {

		String [] parts = aValue.split("_");
		if(parts.length >= 2) {
			di.space = Integer.valueOf(parts[1]);   		
		}

	}
	
	/*
	 * Extract the compound map values from the display item specification
	 */
	public static PdfMapValues getMapValues(Survey survey, DisplayItem di) {
		PdfMapValues mapValues = new PdfMapValues();
		
		// Geo compound
		lookupGeoCompoundValueInSurvey(di.linemap.geoCompoundQuestion, survey.instance.results, mapValues);
		
		if(mapValues.geoCompound) {
			
			mapValues.idxStart = -1;
			mapValues.idxEnd = -1;
			mapValues.idxMarkers = new ArrayList<>();
			if(mapValues.orderedMarkers != null) {
				int markerIdx = 0;
				for(DistanceMarker marker: mapValues.orderedMarkers) {
					String indexString = marker.properties.get("index");
					if(indexString != null) {
						int pointIdx = Integer.valueOf(indexString);
						String type = marker.properties.get("type");
						if(type != null) {
							if(type.equals("pit")) {
								if(mapValues.idxStart == -1) {
									mapValues.idxStart = pointIdx;
								} else {
									mapValues.idxEnd = pointIdx;
								}
								mapValues.idxMarkers.add(pointIdx);
							} else {
								mapValues.lastFaultIdx = markerIdx;
								mapValues.idxMarkers.add(pointIdx);
							}
						}
					}
					markerIdx++;
				}
			}
			
		} else if(!mapValues.geoCompound) {
			// Start point
			ArrayList<String> startValues = lookupInSurvey(di.linemap.startPoint, survey.instance.results);
			if(startValues.size() > 0) {
				mapValues.startLine = startValues.get(0);
			}
	
			// End point
			ArrayList<String> endValues = lookupInSurvey(di.linemap.endPoint, survey.instance.results);
			if(endValues.size() > 0) {
				mapValues.endLine = endValues.get(0);
			}
			
			if(di.linemap.markers.size() > 0) {
				mapValues.markers = new ArrayList<String> ();
				for(String markerName : di.linemap.markers) {
					mapValues.markers.addAll(lookupInSurvey(markerName, survey.instance.results));
				}		
			}
		}
		
		return mapValues;
	}
	
	/*
	 * Get the data from a referenced geo-compound widget
	 * Only return the first found - geo-compounds in repeats are undefined
	 */
	public static void lookupGeoCompoundValueInSurvey(String qname, ArrayList<ArrayList<Result>> records, 
			PdfMapValues mapValues) {

		if(qname != null && records != null && records.size() > 0) {
			for(ArrayList<Result> r : records) {
				for(Result result : r) {
					if(result.subForm == null && result.name.equals(qname) && result.type.equals("geocompound")) {
						mapValues.geoCompound = true;
						mapValues.geometry = result.value;
						mapValues.orderedMarkers = result.markers;
						break;
					} else if(result.subForm != null) {
						lookupGeoCompoundValueInSurvey(qname, result.subForm, mapValues);
					}
				}		
			}
		}
		return;
	}
	
	/*
	 * Get an array of values for the specified question in the survey
	 * There will only be more than one value if the question is in a repeat
	 */
	public static ArrayList<String> lookupInSurvey(String qname, ArrayList<ArrayList<Result>> records) {
		ArrayList<String> values = new ArrayList<>();
		if(qname != null && records != null && records.size() > 0) {
			for(ArrayList<Result> r : records) {
				for(Result result : r) {
					if(result.subForm == null && result.name.equals(qname)) {
						values.add(result.value != null ? result.value : "");
					} else if(result.subForm != null) {
						values.addAll(lookupInSurvey(qname, result.subForm));
					}
				}		
			}
		}
		return values;
	}
}
