package org.smap.sdal.Utilities;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.apache.batik.dom.svg.SVGDOMImplementation;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.PDFTableManager;
import org.smap.sdal.model.PdfMapValues;
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
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PushbuttonField;

public class PdfUtilities {

	private static Logger log =
			 Logger.getLogger(PDFTableManager.class.getName());
	
	private static LogManager lm = new LogManager();		// Application log
	
	public static void addImageTemplate(AcroFields pdfForm, String fieldName, String basePath, 
			String value, String serverRoot, PdfStamper stamper, Font symbols_font) throws IOException, DocumentException {
		PushbuttonField ad = pdfForm.getNewPushbuttonFromField(fieldName);
		if(ad != null) {
			ad.setLayout(PushbuttonField.LAYOUT_ICON_ONLY);
			ad.setProportionalIcon(true);
			try {
				File f = new File(basePath + "/" + value);
				if(f.exists()) {
					ad.setImage(Image.getInstance(basePath + "/" + value));
				} else {
					// mus be on s3
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
	
	public static void addMapImageTemplate(AcroFields pdfForm, PushbuttonField ad, String fieldName, Image img) throws IOException, DocumentException {
		
		if(ad != null) {
			ad.setLayout(PushbuttonField.LAYOUT_ICON_ONLY);
			ad.setProportionalIcon(true);
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
			int sId,
			String user,
			String markerColor,
			String basePath) throws BadElementException, MalformedURLException, IOException, SQLException, TranscoderException {
		
		Image img = null;
		int width = 200;
		int height = 100;
		int margin = 10;
		

        // Add the faults
		String sql = "SELECT ST_Distance(gg1, gg2) As spheroid_dist "
				+ "FROM (SELECT "
				+ "?::geography as gg1,"
				+ "?::geography as gg2"
				+ ") As foo";
		PreparedStatement pstmt = null;;
		
		//Graphics2D g2d = null;
		OutputStream ostream = null;
		DOMImplementation impl = SVGDOMImplementation.getDOMImplementation();
		
		try {
			BufferedImage tempImg = new BufferedImage(width, height,
	                BufferedImage.TYPE_INT_ARGB);
			
			String svgNS = SVGDOMImplementation.SVG_NAMESPACE_URI;
			Document doc = impl.createDocument(svgNS, "svg", null);

			// Get the root element (the 'svg' element).
			org.w3c.dom.Element svgRoot = doc.getDocumentElement();
			
			// Set the width and height attributes on the root 'svg' element.
			svgRoot.setAttributeNS(null, "width", "400");
			svgRoot.setAttributeNS(null, "height", "450");
			
			// Create the rectangle.
			org.w3c.dom.Element rectangle = doc.createElementNS(svgNS, "rect");
			rectangle.setAttributeNS(null, "x", "10");
			rectangle.setAttributeNS(null, "y", "20");
			rectangle.setAttributeNS(null, "width", "100");
			rectangle.setAttributeNS(null, "height", "50");
			rectangle.setAttributeNS(null, "fill", "red");
			
			// Attach the rectangle to the root 'svg' element.
			svgRoot.appendChild(rectangle);
			
			PNGTranscoder t = new PNGTranscoder();
			
			// set the transcoding hints
			t.addTranscodingHint(PNGTranscoder.KEY_WIDTH, new Float(1000));
			t.addTranscodingHint(PNGTranscoder.KEY_ALLOWED_SCRIPT_TYPES, "*");
			t.addTranscodingHint(PNGTranscoder.KEY_CONSTRAIN_SCRIPT_ORIGIN, new Boolean(true));
			t.addTranscodingHint(PNGTranscoder.KEY_EXECUTE_ONLOAD, new Boolean(true));

			// create the transcoder input
			 TranscoderInput input = new TranscoderInput(doc);
			
			/*
			g2d = (Graphics2D) tempImg.getGraphics();
		    java.awt.Font font = new java.awt.Font("SansSerif", java.awt.Font.PLAIN, 8);
		    g2d.setFont(font);
			
			// Add the line
	        g2d.setColor(Color.BLACK);
	        g2d.drawLine(margin, height / 2, width - margin, height / 2);
	        
	        // Add the faults
	        if(mapValues.markers.size() > 0) {
		        pstmt = sd.prepareStatement(sql);	// Prepared statement to get distances
				int lineDistance = getDistance(pstmt, mapValues, mapValues.startLine, mapValues.endLine);
				System.out.println("Distance: " + lineDistance);
				for(int i = 0; i < mapValues.markers.size(); i++) {
					addMarkerImage(g2d, pstmt, mapValues, lineDistance, i, height, width, margin);
				}
	        }
			*/
			
			File file = new File(basePath + "/temp/pdfimage_" + UUID.randomUUID() + ".png");
			ostream = new FileOutputStream(file);
			TranscoderOutput output = new TranscoderOutput(ostream);
			t.transcode(input, output);
			ostream.flush();
	        
			//ImageIO.write(tempImg, "png", file);		
			
			img = Image.getInstance(file.getAbsolutePath());
		} finally {
			 //if(g2d != null) try{g2d.dispose();} catch(Exception e) {}
			 if(pstmt != null) try{pstmt.close();} catch(Exception e) {}
			 if(ostream != null)  try{ostream.close();} catch(Exception e) {}
		}
		
		return img;
	}
	
	private static void addMarkerImage(Graphics2D g2d, PreparedStatement pstmt, PdfMapValues mapValues, int lineDistance, int idx, 
			int height, int width, int margin) throws SQLException {
		int distanceFromP1 = getDistance(pstmt, mapValues, mapValues.startLine, mapValues.markers.get(idx));
		int offset = distanceFromP1 * (width - (2 * margin)) / lineDistance;
		g2d.setColor(Color.RED);
	    g2d.drawLine(margin + offset, height / 2, margin + offset - 5, (height / 2) - 5);
	    g2d.drawLine(margin + offset, height / 2, margin + offset + 5, (height / 2) - 5);
	    
	    g2d.drawOval(margin + offset - 3, (height / 2) - 12, 6, 6);
	    g2d.drawLine(margin + offset - 2, (height / 2) - 5, margin + offset + 2, (height / 2) - 3);
	    // Add distance to P1
	    g2d.setColor(Color.BLACK);
	    if(idx == 0) {
	    	g2d.drawString(distanceFromP1 + "m", margin + 10, (height / 2) + 10);
	    }
	    // Add Distance to P2
	    if(mapValues.markers.size() -1 == idx) {
	    	g2d.drawString((lineDistance - distanceFromP1) + "m", width - (2 * margin) - 10, (height / 2) + 10);
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
			out.append(addGeoJsonFeature(mapValues.getLineGeometry(), "00f", null));
				
			out.append(",");
			out.append(addGeoJsonFeature(mapValues.startLine, "f0f", "1"));
			out.append(",");
			out.append(addGeoJsonFeature(mapValues.endLine, "f0f", "2"));
			
			if(mapValues.hasMarkers()) {
				for(String marker : mapValues.markers) {
					out.append(",");
					out.append(addGeoJsonFeature(marker, "0ff", "roadblock"));
				}
			}
		}
		out.append("]}");
		
		System.out.println(out.toString());
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
		System.out.println(out.toString());
		return out.toString();
	}
	
	/*
	 * Get the distance in meters between two points
	 * Assume they are reasonably close together so use 
	 */
	private static int getDistance(PreparedStatement pstmt, PdfMapValues mapValues, String p1, String p2) throws SQLException {
		
		int distance = -1;
		String[] coords1 = mapValues.getCoordinates(p1, true).split(",");
		String[] coords2 = mapValues.getCoordinates(p2, true).split(",");
		
		if(coords1.length > 1 && coords2.length > 1) {
			
			pstmt.setString(1, "SRID=4326;POINT(" + GeneralUtilityMethods.getDouble(coords1[1]) + " " + GeneralUtilityMethods.getDouble(coords1[1]) + ")");
			pstmt.setString(2, "SRID=4326;POINT(" + GeneralUtilityMethods.getDouble(coords2[1]) + " " + GeneralUtilityMethods.getDouble(coords2[1]) + ")");
			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				distance = rs.getInt(1);
			}
		}

		
		return distance;
	}
}
