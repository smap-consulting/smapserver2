package org.smap.sdal.Utilities;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
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
import javax.imageio.ImageReader;

import com.twelvemonkeys.imageio.plugins.webp.WebPImageReaderSpi;

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
import com.itextpdf.io.image.ImageData;
import com.itextpdf.io.image.ImageDataFactory;
import com.itextpdf.kernel.colors.ColorConstants;
import com.itextpdf.kernel.colors.DeviceRgb;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.geom.Rectangle;
import com.itextpdf.kernel.pdf.PdfArray;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfName;
import com.itextpdf.kernel.pdf.PdfPage;
import com.itextpdf.kernel.pdf.PdfNumber;
import com.itextpdf.kernel.pdf.PdfObject;
import com.itextpdf.kernel.pdf.PdfReader;
import com.itextpdf.kernel.pdf.PdfStream;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.kernel.pdf.action.PdfAction;
import com.itextpdf.kernel.pdf.annot.PdfLinkAnnotation;
import com.itextpdf.kernel.pdf.annot.PdfWidgetAnnotation;
import com.itextpdf.kernel.pdf.canvas.PdfCanvas;
import com.itextpdf.kernel.pdf.canvas.parser.PdfTextExtractor;
import com.itextpdf.kernel.pdf.canvas.parser.listener.LocationTextExtractionStrategy;
import com.itextpdf.kernel.pdf.xobject.PdfFormXObject;
import com.itextpdf.kernel.pdf.xobject.PdfImageXObject;
import com.itextpdf.forms.PdfAcroForm;
import com.itextpdf.forms.fields.PdfButtonFormField;
import com.itextpdf.forms.fields.PdfFormField;
import com.itextpdf.layout.Canvas;
import com.itextpdf.layout.element.Image;
import com.itextpdf.layout.element.Link;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.properties.Leading;
import com.itextpdf.layout.properties.Property;
import com.itextpdf.layout.properties.TextAlignment;
import com.itextpdf.layout.layout.LayoutArea;
import com.itextpdf.layout.layout.LayoutContext;
import com.itextpdf.layout.layout.LayoutResult;
import com.itextpdf.layout.renderer.IRenderer;

public class PdfUtilities {

	private static Logger log =
			 Logger.getLogger(PDFTableManager.class.getName());
	
	private static LogManager lm = new LogManager();		// Application log

	public static final float FONT_SIZE = 12;			// iText's default font size
	public static final float URL_FONT_SIZE = 8;			// Preferred size for a media url in a template field
	public static final float MIN_URL_FONT_SIZE = 5;		// Smaller than this is not worth reading
	public static final float LINE_SPACING = 1.2f;		// Line spacing as a multiple of the font size

	/*
	 * Set the spacing between the lines of a paragraph
	 *
	 * iText 8 defaults to a leading of 2.2 times the font size which leaves large gaps
	 *  between the wrapped lines of a value.  iText 5 table cells used a leading of exactly
	 *  the font size.  Set LINE_SPACING to 1.0 to go back to that
	 *
	 * The leading is fixed rather than multiplied as a multiplied leading is applied to the
	 *  ascender and descender of the font, which vary widely between the fonts used here.
	 *  Hence the font size is set at the same time so that the two stay together
	 *
	 * Labels are not affected.  They are converted from html and carry their own line height
	 */
	public static void setLineSpacing(com.itextpdf.layout.Document document) {
		document.setFontSize(FONT_SIZE);
		document.setProperty(Property.LEADING, new Leading(Leading.FIXED, FONT_SIZE * LINE_SPACING));
	}

	/*
	 * Create iText image data from a file or URL
	 * Formats that iText cannot read directly, such as webp, are converted to png first
	 */
	public static ImageData createImageData(File f) throws IOException {
		try {
			return ImageDataFactory.create(f.getAbsolutePath());
		} catch (com.itextpdf.io.exceptions.IOException e) {
			return convertToPng(Files.readAllBytes(f.toPath()), f.getName());
		}
	}

	public static ImageData createImageData(String name) throws IOException {
		try {
			return ImageDataFactory.create(name);
		} catch (com.itextpdf.io.exceptions.IOException e) {
			File f = new File(name);
			return convertToPng(f.exists() ? Files.readAllBytes(f.toPath()) : readBytes(new URL(name)), name);
		}
	}

	public static ImageData createImageData(byte [] bytes, String name) throws IOException {
		try {
			return ImageDataFactory.create(bytes);
		} catch (com.itextpdf.io.exceptions.IOException e) {
			return convertToPng(bytes, name);
		}
	}

	public static ImageData createImageData(URL url) throws IOException {
		try {
			return ImageDataFactory.create(url);
		} catch (com.itextpdf.io.exceptions.IOException e) {
			return convertToPng(readBytes(url), url.toString());
		}
	}

	private static byte[] readBytes(URL url) throws IOException {
		try (InputStream is = url.openStream()) {
			return is.readAllBytes();
		}
	}

	private static ImageData convertToPng(byte[] bytes, String name) throws IOException {
		BufferedImage bi = ImageIO.read(new ByteArrayInputStream(bytes));
		if(bi == null && isWebp(bytes)) {
			/*
			 * ImageIO did not find the webp plugin.  This happens in a servlet container
			 * where the plugin registry can be initialised before the webapp classloader
			 * is scanned, so use the webp reader directly
			 */
			ImageReader reader = new WebPImageReaderSpi().createReaderInstance(null);
			try {
				reader.setInput(ImageIO.createImageInputStream(new ByteArrayInputStream(bytes)));
				bi = reader.read(0, null);
			} finally {
				reader.dispose();
			}
		}
		if(bi == null) {
			throw new IOException("Unsupported image format: " + name);
		}
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		ImageIO.write(bi, "png", os);
		return ImageDataFactory.create(os.toByteArray());
	}

	/*
	 * webp files start with the RIFF container header followed by the WEBP fourcc
	 */
	private static boolean isWebp(byte[] b) {
		return b.length > 12
				&& b[0] == 'R' && b[1] == 'I' && b[2] == 'F' && b[3] == 'F'
				&& b[8] == 'W' && b[9] == 'E' && b[10] == 'B' && b[11] == 'P';
	}

	public static void addImageTemplate(PdfAcroForm pdfForm, String fieldName, String basePath,
			String value, String serverRoot, PdfDocument pdfDoc, PdfFont symbols_font,
			boolean stretch) throws IOException {

		PdfFormField field = pdfForm.getField(fieldName);
		if(field instanceof PdfButtonFormField) {
			PdfButtonFormField ad = (PdfButtonFormField) field;
			try {
				File f = new File(basePath + "/" + value);
				if(f.exists()) {
					ad.setImage(basePath + "/" + value);
				} else {
					// must be on s3
					ad.setImage(serverRoot + "/" + value);
				}
			} catch (Exception e) {
				log.fine("Error: Failed to add image " + basePath + "/" + value + " to pdf: " + e.getMessage());
			}

			log.fine("Adding image to: " + fieldName);
		} else if(field != null) {

			String imageUrl = serverRoot + value;

			PdfWidgetAnnotation widget = field.getWidgets().isEmpty() ? null : field.getWidgets().get(0);
			if(widget == null) {
				log.fine("Field not found for: " + fieldName);
			} else {
				Rectangle targetPosition = widget.getRectangle().toRectangle();
			    Link url = new Link("\uf08e", PdfAction.createURI(imageUrl));
			    float fontSize = 12;
			    Canvas data = new Canvas(new PdfCanvas(widget.getPage()), targetPosition);
			    data.showTextAligned(
			    		new Paragraph(url).setFont(symbols_font).setFontSize(fontSize),
			    		(targetPosition.getLeft() + targetPosition.getRight()) / 2,
			    		targetPosition.getBottom() + (targetPosition.getHeight() - fontSize) / 2,
			    		TextAlignment.CENTER);
			    data.close();
			}
		}
	}

	/*
	 * Add a video, audio or other media file to a template field
	 *
	 * The media itself cannot be embedded in the pdf so show the thumbnail that was created
	 *  when the attachment was uploaded, falling back to the media url when there is no
	 *  thumbnail, and make the field area a link through to the media on the server
	 *
	 * iText's setImage() must not be used for these files.  It base64 encodes the file and sets
	 *  that as the field value, then, when the encoded data cannot be read as an image, it
	 *  shows the encoded data as the label of the field
	 */
	public static void addMediaTemplate(PdfAcroForm pdfForm, String fieldName, String basePath,
			String value, String serverRoot, PdfDocument pdfDoc, PdfFont font, PdfFont symbols_font) {

		PdfFormField field = pdfForm.getField(fieldName);
		if(field == null || field.getWidgets().isEmpty()) {
			log.fine("Field not found for: " + fieldName);
			return;
		}

		PdfWidgetAnnotation widget = field.getWidgets().get(0);
		PdfPage page = widget.getPage();
		Rectangle rect = widget.getRectangle().toRectangle();
		String mediaUrl = serverRoot + value;

		ImageData thumbnail = getMediaThumbnail(basePath, value, serverRoot);
		boolean added = false;

		if(thumbnail != null && field instanceof PdfButtonFormField) {
			try {
				PdfImageXObject imgXObj = new PdfImageXObject(thumbnail);
				PdfFormXObject formXObj = new PdfFormXObject(new Rectangle(0, 0, imgXObj.getWidth(), imgXObj.getHeight()));
				new PdfCanvas(formXObj, pdfDoc).addXObjectAt(imgXObj, 0, 0);
				((PdfButtonFormField) field).setImageAsForm(formXObj);
				added = true;
			} catch (Exception e) {
				log.fine("Error: Failed to add media thumbnail to " + fieldName + ": " + e.getMessage());
			}
		}

		if(!added && page != null) {
			/*
			 * Either the field is not a push button or there is no thumbnail.  Draw into the
			 *  field area and then remove the field so that flattening the form does not
			 *  cover the drawing
			 */
			Canvas canvas = new Canvas(new PdfCanvas(page.newContentStreamAfter(), page.getResources(), pdfDoc), rect);
			try {
				if(thumbnail != null) {
					Image img = new Image(thumbnail);
					img.scaleToFit(rect.getWidth(), rect.getHeight());
					canvas.add(img);
				} else {
					/*
					 * Show the url if the whole of it can be made to fit, and a link marker if
					 * it cannot.
					 *
					 * A template field for an attachment is often only wide enough for an icon,
					 * because an icon is what was drawn here before these fields showed a url at
					 * all.  Given a ninety character url and thirty points to put it in, the
					 * layout writes the lines that fit and silently drops the rest, which came
					 * out as "https:/" in the field and nothing anywhere else.
					 *
					 * Try smaller sizes before giving up: a field with room for the url over two
					 * or three lines is common, and shrinking a point or two is far better than
					 * replacing the url with an icon.  Ask the layout engine rather than working
					 * it out from font metrics, since it is the same code that does the drawing
					 * and it knows where a url can be broken.
					 */
					Paragraph urlPara = null;
					float urlHeight = 0;
					for(float fontSize = URL_FONT_SIZE; fontSize >= MIN_URL_FONT_SIZE; fontSize -= 1) {
						Paragraph candidate = new Paragraph(mediaUrl)
								.setFont(font)
								.setFontSize(fontSize)
								.setFixedLeading(fontSize * LINE_SPACING)
								.setFontColor(ColorConstants.BLUE)
								.setTextAlignment(TextAlignment.CENTER);
						LayoutResult result = layout(candidate, canvas, pdfDoc, page, rect);
						if(result != null && result.getStatus() == LayoutResult.FULL) {
							urlPara = candidate;
							urlHeight = result.getOccupiedArea().getBBox().getHeight();
							break;
						}
					}

					if(urlPara != null) {
						/*
						 * A canvas lays out from the top of its area, so a url that does not
						 * fill the field sits against the top of it.  Centre it in the field,
						 * which is where the template author drew the box for it.
						 */
						canvas.add(urlPara.setMarginTop(Math.max(0, (rect.getHeight() - urlHeight) / 2)));
					} else {
						/*
						 * No size shows the whole url, so mark the field as a link instead of
						 * writing part of one.  The whole rectangle is a link annotation below,
						 * so the marker only has to say that there is something here.
						 *
						 * Place it by its baseline as addImageTemplate does rather than adding it
						 * to the canvas: the glyph needs more room than its font size and a
						 * paragraph is laid out from the top of the field, so an added one sits
						 * high and, in a field only as tall as the glyph, crosses the top of it.
						 */
						float iconSize = Math.min(12, rect.getHeight());
						canvas.showTextAligned(
								new Paragraph("\uf08e")
										.setFont(symbols_font)
										.setFontSize(iconSize)
										.setFixedLeading(iconSize)
										.setFontColor(ColorConstants.BLUE),
								(rect.getLeft() + rect.getRight()) / 2,
								rect.getBottom() + (rect.getHeight() - iconSize) / 2,
								TextAlignment.CENTER);
					}
				}
			} catch (Exception e) {
				log.fine("Error: Failed to add media " + value + " to " + fieldName + ": " + e.getMessage());
			} finally {
				canvas.close();
			}

			try {
				pdfForm.removeField(fieldName);
			} catch (Exception e) {
				log.fine("Error removing field: " + fieldName + ": " + e.getMessage());
			}
		}

		/*
		 * Add the link to the page rather than to the field.  A link set on the field would be
		 *  lost when the form is flattened
		 */
		if(page != null) {
			try {
				PdfLinkAnnotation link = new PdfLinkAnnotation(rect);
				link.setAction(PdfAction.createURI(mediaUrl));
				link.setBorder(new PdfArray(new float [] {0, 0, 0}));
				page.addAnnotation(link);
			} catch (Exception e) {
				log.fine("Error: Failed to add media link for " + fieldName + ": " + e.getMessage());
			}
		}
	}

	/*
	 * Lay the paragraph out in the rectangle without drawing it, to find out whether all of it
	 * fits and how much room it takes.  A status of anything less than FULL means the layout
	 * would silently drop the rest, which is how a url became "https:/".
	 *
	 * Returns null when the layout could not be done, which the caller reads as not fitting.
	 */
	private static LayoutResult layout(Paragraph para, Canvas canvas, PdfDocument pdfDoc,
			PdfPage page, Rectangle rect) {
		try {
			IRenderer renderer = para.createRendererSubTree().setParent(canvas.getRenderer());
			return renderer.layout(
					new LayoutContext(new LayoutArea(pdfDoc.getPageNumber(page), rect)));
		} catch (Exception e) {
			// Could not tell, so assume not and show the marker rather than part of a url
			log.fine("Could not measure media url for a template field: " + e.getMessage());
			return null;
		}
	}

	/*
	 * Get the thumbnail that was created for a media file when it was uploaded
	 * Returns null if there is no thumbnail, which is the case for audio and for any other
	 *  media that no thumbnail can be generated from
	 */
	private static ImageData getMediaThumbnail(String basePath, String value, String serverRoot) {

		ImageData thumbnail = null;

		try {
			byte [] bytes = AttachmentStore.getThumbnailBytes(basePath, value, serverRoot, false);
			if(bytes != null) {
				thumbnail = createImageData(bytes, value);
			}
		} catch (Exception e) {
			log.fine("Error: Failed to read thumbnail for " + value + ": " + e.getMessage());
		}

		return thumbnail;
	}

	public static void addMapImageTemplate(PdfAcroForm pdfForm, String fieldName, ImageData img, boolean stretch,
			PdfDocument pdfDoc) {

		PdfFormField field = pdfForm.getField(fieldName);
		if(field instanceof PdfButtonFormField && img != null) {
			PdfButtonFormField ad = (PdfButtonFormField) field;
			try {
				// Build a form XObject holding the (in-memory) image and set it as the button appearance
				PdfImageXObject imgXObj = new PdfImageXObject(img);
				PdfFormXObject formXObj = new PdfFormXObject(new Rectangle(0, 0, imgXObj.getWidth(), imgXObj.getHeight()));
				new PdfCanvas(formXObj, pdfDoc).addXObjectAt(imgXObj, 0, 0);
				ad.setImageAsForm(formXObj);
			} catch (Exception e) {
				log.fine("Error: Failed to add image to pdf: " + e.getMessage());
			}
			log.fine("Adding image to: " + fieldName);
		}
	}
	
	public static ImageData getMapImage(
			Connection sd,
			String mapSource,
			String map, 
			String account,
			PdfMapValues mapValues, 
			String location, 
			String zoom,
			String mapbox_key,
			String google_key,
			String maptiler_key,
			int sId,
			String user,
			String markerColor,
			String basePath) throws Exception {
		
		if(mapSource == null || mapSource.equals("default")) {
			mapSource = GeneralUtilityMethods.getDefaultMapSource(sd, sId);
		}
		
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
		} else if(mapSource.equals("maptiler")) {
			 return PdfUtilities.getMapImageMapTiler(sd, map, 
						mapValues,
						location, zoom, 
						maptiler_key,
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
	private static ImageData getMapImageMapbox(Connection sd,
			String map,
			String account,
			PdfMapValues mapValues,
			String location,
			String zoom,
			String mapbox_key,
			int sId,
			String user,
			String markerColor,
			String basePath) throws MalformedURLException, IOException, SQLException {

		ImageData img = null;
		
		StringBuilder url = new StringBuilder();
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
			log.fine("Mapbox key not specified.  PDF Map not created");
		} else if(getMap) {;
			url.append("500x300?access_token=");
			url.append(mapbox_key);
			try {
				log.fine("Mapbox API call: " + url);
				
				/*
				 * There is a problem with passing a URL to the IText getInstance function as
				 * it will cause two mapbox requests to be recorded resulting in additional charges
				 * Instead download the image first then add it to the PDF as a file
				 */
				URL mapboxUrl = new URL(url.toString());
				BufferedImage tempImg = ImageIO.read(mapboxUrl);
				File file = new File(basePath + "/temp/pdfmap_" + UUID.randomUUID() + ".png");
				ImageIO.write(tempImg, "png", file);
				img = ImageDataFactory.create(file.getAbsolutePath());

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
	private static ImageData getMapImageGoogle(Connection sd,
			String map,
			String account,
			PdfMapValues mapValues,
			String location,
			String zoom,
			String google_key,
			int sId,
			String user,
			String markerColor,
			String basePath) throws MalformedURLException, IOException, SQLException {

		ImageData img = null;
		
		StringBuilder url = new StringBuilder();
		boolean hasParam = false;
		url.append("https://maps.googleapis.com/maps/api/staticmap");
		
		if((mapValues.hasGeometry())) {
			
			if(!hasParam) {
				url.append("?");
			} else {
				url.append("&");
			}
			url.append(createMapValueGoogle(mapValues, markerColor));
			
			if(zoom != null && zoom.trim().length() > 0) {
				url.append("&zoom=" + zoom);
			} else {
				url.append("&zoom=12");
			}
		} 
		
		if(google_key == null) {
			log.fine("Google key not specified.  PDF Map not created");
		} else {;
			url.append("&size=400x400&key=");
			url.append(google_key);
			try {
				log.fine("Google API call: " + url);
				
				URL googleUrl = new URL(url.toString());
				BufferedImage tempImg = ImageIO.read(googleUrl);
				File file = new File(basePath + "/temp/pdfmap_" + UUID.randomUUID() + ".png");
				ImageIO.write(tempImg, "png", file);
				img = ImageDataFactory.create(file.getAbsolutePath());

				lm.writeLog(sd, sId, user, LogManager.GOOGLE_REQUEST, map, 0, null);
			} catch (Exception e) {
				lm.writeLog(sd, sId, user, LogManager.ERROR, "Could not get google map image. You may need to enable billing on your google maps API at https://console.cloud.google.com/project/_/billing/enable", 0, null);
				log.log(Level.SEVERE, "Exception", e);
			}
		} 
		
		return img;
	}
	
	/*
	 * Convert geospatial data into a mapbox map image
	 */
	private static ImageData getMapImageMapTiler(Connection sd,
			String map,
			PdfMapValues mapValues,
			String location,
			String zoom,
			String maptiler_key,
			int sId,
			String user,
			String markerColor,
			String basePath) throws MalformedURLException, IOException, SQLException {

		ImageData img = null;
		
		StringBuilder url = new StringBuilder();
		String lonLat = null;
		boolean getMap = false;
		
		url.append(" https://api.maptiler.com/maps/");
		
		if(map != null && !map.equals("none")) {
			url.append(map);
		} else {
			url.append("streets");	// default map
		}
		url.append("/static/");
		
		if(zoom == null || zoom.trim().length() == 0) {
			zoom = "16";
		}
		if((mapValues.hasGeometry() || mapValues.hasLine())) {
			
			String centroidValue = mapValues.geometry;
			if(centroidValue == null) {
				centroidValue = mapValues.startGeometry;
			}
			lonLat = GeneralUtilityMethods.getGeoJsonCentroid(centroidValue);
			url.append(lonLat + "," + zoom);
			
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
		
		url.append("500x300.png");
		
		if(getMap && maptiler_key == null) {
			log.fine("Maptiler key not specified.  PDF Map not created");
		} else if(getMap) {
			url.append("?key=").append(maptiler_key);
			
			/*
			 * Add marker
			 */
			if(lonLat != null) {
				if(markerColor == null) {
					markerColor = "red";
				} else {
					markerColor = "%" + markerColor;
				}
				url.append("&markers=")
					.append(lonLat + "," + markerColor);
			}
			
			try {
				log.fine("Maptiler API call: " + url.toString());
				
				/*
				 * There is a problem with passing a URL to the IText getInstance function as
				 * it will cause two mapbox requests to be recorded resulting in additional charges
				 * However maptiler required it or the request will be rejected
				 */
				URL mapUrl = new URL(url.toString());
				img = ImageDataFactory.create(mapUrl);
				lm.writeLog(sd, sId, user, LogManager.MAPTILER_REQUEST, map, 0, null);
			} catch (Exception e) {
				log.log(Level.SEVERE, "Exception", e);
			}
		} 
		
		return img;
	}

	/*
	 * Convert geospatial data into an abstract image
	 */
	public static ImageData getLineImage(Connection sd,
			PdfMapValues mapValues,
			TrafficLightValues tlValues,
			int sId,
			String user,
			DisplayItem di,
			String basePath,
			Float width,
			Float height) throws MalformedURLException, IOException, SQLException, TranscoderException {

		ImageData img = null;
	
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
			t.addTranscodingHint(PNGTranscoder.KEY_WIDTH, Float.valueOf(1000.0f));
			t.addTranscodingHint(PNGTranscoder.KEY_ALLOWED_SCRIPT_TYPES, "*");
			t.addTranscodingHint(PNGTranscoder.KEY_CONSTRAIN_SCRIPT_ORIGIN, Boolean.valueOf(true));
			t.addTranscodingHint(PNGTranscoder.KEY_EXECUTE_ONLOAD, Boolean.valueOf(true));
			t.addTranscodingHint(PNGTranscoder.KEY_BACKGROUND_COLOR, Color.white);

			// create the transcoder input
			 TranscoderInput input = new TranscoderInput(doc);
			
			File file = new File(basePath + "/temp/pdfimage_" + UUID.randomUUID() + ".png");
			ostream = new FileOutputStream(file);
			TranscoderOutput output = new TranscoderOutput(ostream);
			t.transcode(input, output);
			ostream.flush();

			img = ImageDataFactory.create(file.getAbsolutePath());
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
			}
			
			/*
			 * Sort the ordered markers
			 */
			Collections.sort(mapValues.orderedMarkers, new Comparator<DistanceMarker>() {
			    public int compare( DistanceMarker a, DistanceMarker b ) {
			    	return Float.compare(a.distance, b.distance);
			    }
			});
			
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
				
				log.fine(pstmt.toString());
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
				
			log.fine(pstmt.toString());
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
				
			log.fine(pstmt.toString());
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
	public static void resizePdf(String src, OutputStream os) throws IOException {

	    // Read the file (reader + writer to the output stream in one document)
	    PdfDocument pdfDoc = new PdfDocument(new PdfReader(src), new PdfWriter(os));
	    int n = pdfDoc.getNumberOfPdfObjects();

	    // Look for image and manipulate image stream
	    for (int i = 1; i < n; i++) {
	        PdfObject object = pdfDoc.getPdfObject(i);
	        if (object == null || !object.isStream())
	            continue;
	        PdfStream stream = (PdfStream) object;
	        PdfObject pdfsubtype = stream.get(PdfName.Subtype);
	        if (pdfsubtype != null && pdfsubtype.equals(PdfName.Image)) {
	            BufferedImage bi;
	            try {
	                bi = new PdfImageXObject(stream).getBufferedImage();
	            } catch (Exception e) {
	                continue;
	            }
	            if (bi == null) continue;
	            int width = bi.getWidth();
	            int height = bi.getHeight();

	            /*
	             * Calculate amount of compression
	             */
	    	    float factor = 1.0f;
	            log.fine("compressing.  width: " + width + " height: " + height);
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
	            stream.setData(imgBytes.toByteArray(), false);		// false: do not flate-compress, bytes are DCT (JPEG)
	            stream.put(PdfName.Type, PdfName.XObject);
	            stream.put(PdfName.Subtype, PdfName.Image);
	            stream.put(PdfName.Filter, PdfName.DCTDecode);
	            stream.put(PdfName.Width, new PdfNumber(width));
	            stream.put(PdfName.Height, new PdfNumber(height));
	            stream.put(PdfName.BitsPerComponent, new PdfNumber(8));
	            stream.put(PdfName.ColorSpace, PdfName.DeviceRGB);
	        }
	    }
	    // Save altered PDF
	    pdfDoc.close();

	}
	
	/*
	 * https://stackoverflow.com/questions/2464166/how-can-i-remove-blank-page-from-pdf-in-itext/3309453
	 */
	public static void removeBlankPages(String pdfSourceFile, String pdfDestinationFile) throws IOException
	{

		// Open source and destination documents
		PdfDocument src = new PdfDocument(new PdfReader(pdfSourceFile));
		PdfDocument dest = new PdfDocument(new PdfWriter(pdfDestinationFile));

		//loop through each page and only copy pages that have text on them
		int num = src.getNumberOfPages();
		for (int i = 1; i <= num; i++)
		{
			String text = PdfTextExtractor.getTextFromPage(src.getPage(i), new LocationTextExtractionStrategy());

			//add the page to the new pdf
			if (text != null && text.length() > 0) {
				src.copyPagesTo(i, i, dest);
			}
		}
		//close everything
		dest.close();
		src.close();

	}
	
	public static Question getQuestionFromResult(Connection sd, Survey survey, Result r, Form form) throws SQLException {

		Question question = null;
		if(r.qIdx >= 0) {
			question = form.questions.get(r.qIdx);
		} if(r.qIdx <= MetaItem.INITIAL_ID) {
			question = GeneralUtilityMethods.getPreloadAsQuestion(sd, survey.surveyData.id, r.qIdx);	// A preload
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
				dfDateOnly.setTimeZone(TimeZone.getTimeZone("UTC"));	// Dates are always in UTC
				value = dfDateOnly.format(date);
			}
			
			// If Bikram Sambat date output is required convert  
			if(di.bs) {
	
				Date nepalDate;
				
				log.fine("utc value: " + utcValue);
				
				
				if(type.equals("dateTime") || type.equals("timestamp")) {
					df.setTimeZone(TimeZone.getTimeZone("UTC"));
					date = df.parse(utcValue);
					df.setTimeZone(TimeZone.getTimeZone(tz));
					value = df.format(date);
					log.fine("xxxxxxxxx: " + value);
					df.setTimeZone(TimeZone.getTimeZone("UTC"));
					nepalDate = df.parse(value);
				} else {	
					dfDateOnly.setTimeZone(TimeZone.getTimeZone("UTC"));
					date = dfDateOnly.parse(utcValue);
					date.setHours(12);
					nepalDate = date;
				} 		
					
				log.fine("Value: " + value);
				
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

		com.itextpdf.kernel.colors.Color color = null;

		String [] parts = aValue.split("_");
		if(parts.length >= 4) {
			if(parts[1].startsWith("0x")) {
				color = new DeviceRgb(Integer.decode(parts[1]),
						Integer.decode(parts[2]),
						Integer.decode(parts[3]));
			} else {
				color = new DeviceRgb(Integer.decode("0x" + parts[1]),
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
		lookupGeoCompoundValueInSurvey(di.linemap.geoCompoundQuestion, survey.surveyData.instance.results, mapValues);
		
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
			ArrayList<String> startValues = lookupInSurvey(di.linemap.startPoint, survey.surveyData.instance.results);
			if(startValues.size() > 0) {
				mapValues.startLine = startValues.get(0);
			}
	
			// End point
			ArrayList<String> endValues = lookupInSurvey(di.linemap.endPoint, survey.surveyData.instance.results);
			if(endValues.size() > 0) {
				mapValues.endLine = endValues.get(0);
			}
			
			if(di.linemap.markers.size() > 0) {
				mapValues.markers = new ArrayList<String> ();
				for(String markerName : di.linemap.markers) {
					mapValues.markers.addAll(lookupInSurvey(markerName, survey.surveyData.instance.results));
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
