package org.smap.sdal.managers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.PdfPageSizer;
import org.smap.sdal.Utilities.PdfUtilities;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.DisplayItem;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.PdfMapValues;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Result;
import org.smap.sdal.model.Row;
import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TrafficLightBulb;
import org.smap.sdal.model.TrafficLightValues;
import org.smap.sdal.model.User;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.MetadataException;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.itextpdf.text.Anchor;
import com.itextpdf.text.BadElementException;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.FontFactory;
import com.itextpdf.text.Image;
import com.itextpdf.text.List;
import com.itextpdf.text.ListItem;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.AcroFields;
import com.itextpdf.text.pdf.BarcodeQRCode;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.PushbuttonField;
import com.itextpdf.tool.xml.ElementList;
import com.itextpdf.tool.xml.XMLWorker;
import com.itextpdf.tool.xml.XMLWorkerFontProvider;
import com.itextpdf.tool.xml.XMLWorkerHelper;
import com.itextpdf.tool.xml.css.CssFile;
import com.itextpdf.tool.xml.css.StyleAttrCSSResolver;
import com.itextpdf.tool.xml.html.CssAppliers;
import com.itextpdf.tool.xml.html.CssAppliersImpl;
import com.itextpdf.tool.xml.html.Tags;
import com.itextpdf.tool.xml.parser.XMLParser;
import com.itextpdf.tool.xml.pipeline.css.CSSResolver;
import com.itextpdf.tool.xml.pipeline.css.CssResolverPipeline;
import com.itextpdf.tool.xml.pipeline.end.ElementHandlerPipeline;
import com.itextpdf.tool.xml.pipeline.html.HtmlPipeline;
import com.itextpdf.tool.xml.pipeline.html.HtmlPipelineContext;

/*****************************************************************************

This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

 ******************************************************************************/

public class PDFSurveyManager {

	private static Logger log =
			Logger.getLogger(PDFSurveyManager.class.getName());

	LogManager lm = new LogManager();		// Application log

	public static Font Symbols = null;
	public static Font defaultFont = null;
	public static Font defaultFontBold = null;
	public static Font defaultFontLink = null;
	public static Font arabicFont = null;
	public static Font bengaliFont = null;
	public static Font bengaliFontBold = null;
	public static Font devanagariFont = null;
	public static Font devanagariFontBold = null;
	private static final String DEFAULT_CSS = "/resources/css/default_pdf.css";
	private static int NUMBER_TABLE_COLS = 10;
	private static int NUMBER_QUESTION_COLS = 10;

	// Global values set in constructor
	private ResourceBundle localisation;
	private ChoiceManager choiceManager = null;
	private Survey survey;
	private Connection sd;
	private Connection cResults;
	private String user;
	private String tz;

	// Other global values
	int languageIdx = 0;

	private class Parser {
		XMLParser xmlParser = null;
		ElementList elements = null;
	}

	int marginLeft = 50;
	int marginRight = 50;
	int marginTop_1 = 130;
	int marginBottom_1 = 100;
	int marginTop_2 = 50;
	int marginBottom_2 = 100;

	boolean mExcludeEmpty = false;
	String mBasePath;
	String mHyperlinkPrefix;
	String mAttachmentPrefix;

	private class StringElement {
		public boolean htmlToken;
		public String text;
		public int index;
	}

	private class GlobalVariables {																// Level descended in form hierarchy
		//HashMap<String, Integer> count = new HashMap<String, Integer> ();		// Record number at a location given by depth_length as a string
		int [] cols = {NUMBER_QUESTION_COLS};	// Current Array of columns
		boolean hasAppendix = false;
		String mapbox_key;
		String google_key;
		String maptiler_key;

		// Map of questions that need to have the results of another question appended to their results in a pdf report
		HashMap <String, ArrayList<String>> addToList = new HashMap <String, ArrayList<String>>();
	}

	public PDFSurveyManager(ResourceBundle l, Connection sd, Connection cResults, Survey s, String u, String tz) {
		localisation = l;
		choiceManager = new ChoiceManager(l, tz);
		this.sd = sd;
		this.cResults = cResults;
		survey  = s;
		user = u;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	/*
	 * Call this function to create a PDF
	 * Return a suggested name for the PDF file derived from the results
	 */
	public String createPdf(
			OutputStream outputStream,
			String basePath, 
			String attachmentPrefix,
			String hyperlinkPrefix,
			String remoteUser,
			String language, 
			int pdfTemplateId,
			boolean generateBlank,
			String filename,
			boolean landscape,					// Set true if landscape
			HttpServletResponse response) throws Exception {

 		if(language != null) {
			language = language.replace("'", "''");	// Escape apostrophes
		} else {
			language = "none";
		}

		mExcludeEmpty = survey.surveyData.exclude_empty;
		mBasePath = basePath;
		mAttachmentPrefix = attachmentPrefix;
		mHyperlinkPrefix = hyperlinkPrefix;

		User user = null;

		ServerManager serverManager = new ServerManager();
		ServerData serverData = serverManager.getServer(sd, localisation);

		UserManager um = new UserManager(localisation);
		int [] repIndexes = new int[20];		// Assume repeats don't go deeper than 20 levels

		Document document = null;
		PdfWriter writer = null;
		PdfReader reader = null;
		PdfStamper stamper = null;

		try {

			// Get fonts and embed them
			String os = System.getProperty("os.name");
			log.info("Operating System:" + os);

			if(os.startsWith("Mac")) {
				FontFactory.register("/Library/Fonts/fontawesome-webfont.ttf", "Symbols");
				//FontFactory.register("/Library/Fonts/Arial Unicode.ttf", "default");
				FontFactory.register("/Library/Fonts/NotoNaskhArabic-Regular.ttf", "arabic");
				FontFactory.register("/Library/Fonts/NotoSans-Regular.ttf", "notosans");
				FontFactory.register("/Library/Fonts/NotoSans-Bold.ttf", "notosansbold");
				FontFactory.register("/Library/Fonts/NotoSansBengali-Regular.ttf", "bengali");
				FontFactory.register("/Library/Fonts/NotoSansBengali-Bold.ttf", "bengalibold");
				FontFactory.register("/Library/Fonts/NotoSansDevanagari-Light.ttf", "devanagari");
				FontFactory.register("/Library/Fonts/NotoSansDevanagari-Bold.ttf", "devanagaribold");
			} else if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") > 0) {
				// Linux / Unix
				FontFactory.register("/usr/share/fonts/truetype/fontawesome-webfont.ttf", "Symbols");
				FontFactory.register("/usr/share/fonts/truetype/NotoNaskhArabic-Regular.ttf", "arabic");
				FontFactory.register("/usr/share/fonts/truetype/NotoSans-Regular.ttf", "notosans");
				FontFactory.register("/usr/share/fonts/truetype/NotoSans-Bold.ttf", "notosansbold");
				FontFactory.register("/usr/share/fonts/truetype/NotoSansBengali-Regular.ttf", "bengali");
				FontFactory.register("/usr/share/fonts/truetype/NotoSansBengali-Bold.ttf", "bengalibold");
				FontFactory.register("/usr/share/fonts/truetype/NotoSansDevanagari-Light.ttf", "devanagari");
				FontFactory.register("/usr/share/fonts/truetype/NotoSansDevanagari-Bold.ttf", "devanagaribold");
			}

			Symbols = FontFactory.getFont("Symbols", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 12); 
			defaultFontLink = FontFactory.getFont("Symbols", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 12); 
			defaultFont = FontFactory.getFont("notosans", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 10); 
			defaultFontBold = FontFactory.getFont("notosansbold", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 10); 
			arabicFont = FontFactory.getFont("arabic", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 10); 
			bengaliFont = FontFactory.getFont("bengali", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 10); 
			bengaliFontBold = FontFactory.getFont("bengalibold", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 10); 
			devanagariFont = FontFactory.getFont("devanagari", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 10); 
			devanagariFontBold = FontFactory.getFont("devanagaribold", BaseFont.IDENTITY_H, 
					BaseFont.EMBEDDED, 10); 

			defaultFontLink.setColor(BaseColor.BLUE);

			/*
			 * Get the results and details of the user that submitted the survey
			 */

			log.info("User Ident who submitted the survey: " + survey.surveyData.instance.user);
			String userName = survey.surveyData.instance.user;
			if(userName == null) {
				userName = remoteUser;
			}
			if(userName != null) {
				user = um.getByIdent(sd, userName);
			}

			// If a filename was not specified then get one from the survey data
			// This filename is returned to the calling program so that it can be used as a permanent name for the temporary file created here
			// If the PDF is to be returned in an http response then the header is set now before writing to the output stream
			log.info("Filename passed to createPDF is: " + filename);
			if(filename == null) {
				filename = survey.getInstanceName();
				filename = GeneralUtilityMethods.convertDisplayNameToFileName(filename, false);
				filename += ".pdf";

			} else {
				if(!filename.endsWith(".pdf")) {
					filename += ".pdf";
				}
			}

			// If the PDF is to be returned in an http response then set the file name now
			if(response != null) {
				log.info("Setting filename to: " + filename);
				GeneralUtilityMethods.setFilenameInResponse(filename, response);
			}

			/*
			 * Get a template for the PDF report if it exists
			 * The template name will be the same as the XLS form name but with an extension of pdf
			 */
			File templateFile = GeneralUtilityMethods.getPdfTemplate(sd, mBasePath, survey.surveyData.displayName, survey.surveyData.p_id, pdfTemplateId, survey.surveyData.ident);

			/*
			 * Get dependencies between Display Items, for example if a question result should be added to another
			 *  question's results
			 */
			GlobalVariables gv = new GlobalVariables();
			if(!generateBlank) {
				for(int i = 0; i < survey.surveyData.instance.results.size(); i++) {
					getDependencies(gv, survey.surveyData.instance.results.get(i), survey, i);	
				}
			}
			gv.mapbox_key = serverData.mapbox_default;
			gv.google_key = serverData.google_key;
			gv.maptiler_key = serverData.maptiler_key;
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, remoteUser);

			languageIdx = GeneralUtilityMethods.getLanguageIdx(survey, language);
			if(templateFile != null && templateFile.exists()) {

				log.info("PDF Template Exists: " + templateFile.getAbsolutePath());
				String templateName = templateFile.getAbsolutePath();

				reader = new PdfReader(templateName);
				stamper = new PdfStamper(reader, outputStream);

				for(int i = 0; i < survey.surveyData.instance.results.size(); i++) {
					fillTemplate(gv, stamper.getAcroFields(), survey.surveyData.instance.results.get(i), 
							null, i, stamper, oId);
				}
				if(user != null) {
					fillTemplateUserDetails(stamper.getAcroFields(), user, mBasePath);
				}
				stamper.setFormFlattening(true);
				stamper.flush();
				stamper.close();
				stamper = null;

			} else {
				log.info("++++No template exists creating a pdf file programmatically");

				/*
				 * Create a PDF without the stationary
				 * If we need to add a letter head then create document in two passes, the second pass adds the letter head
				 * Else just create the document directly in a single pass
				 */
				Parser parser = getXMLParser();

				// Step 1 - Create the underlying document as a byte array
				if(landscape) {
					document = new Document(PageSize.A4.rotate());
				} else {
					document = new Document(PageSize.A4);
				}

				// Get the title
				String title = survey.getInstanceName();
				if(title.equals("survey")) {
					title = survey.surveyData.displayName;
				}

				// Determine the number of rows in the title and adjust the document margins accordingly
				Font titleFont = new Font();
				int fontHeight = 18;
				titleFont.setSize(fontHeight);
				float width = titleFont.getCalculatedBaseFont(true).getWidthPoint(title, titleFont.getCalculatedSize());
				Rectangle pageRect = document.getPageSize();
				// Calculate rows of title and substract 1 as maginTop_1 assumes 1 row already
				int rows = Math.round((width / (pageRect.getWidth() - marginLeft - marginRight)) + 1) - 1;

				document.setMargins(marginLeft, marginRight, marginTop_1 + rows * fontHeight, marginBottom_1);
				writer = PdfWriter.getInstance(document, outputStream);
				writer.setInitialLeading(12);	

				writer.setPageEvent(new PdfPageSizer(title, 
						user, mBasePath, null,
						marginLeft, marginRight, marginTop_2, marginBottom_2,
						survey.surveyData.ident, survey.surveyData.default_logo)); 
				document.open();

				// If this form has data maintain a list of parent records to lookup ${values}
				ArrayList<ArrayList<Result>> parentRecords = null;
				if(!generateBlank) {
					parentRecords = new ArrayList<ArrayList<Result>> ();
				}

				for(int i = 0; i < survey.surveyData.instance.results.size(); i++) {
					processForm(
							parser, 
							document, 
							survey.surveyData.instance.results.get(i), 
							generateBlank,
							0,
							i,
							repIndexes,
							gv,
							false,
							parentRecords,
							remoteUser,
							oId,
							true		// show sub form index
							);		
				}

				fillNonTemplateUserDetails(document, user, basePath, survey.getInstanceMeta().hrk);

				// Add appendix
				if(gv.hasAppendix) {
					document.newPage();
					document.add(new Paragraph("Appendix", defaultFontBold));

					for(int i = 0; i < survey.surveyData.instance.results.size(); i++) {
						processForm(
								parser, 
								document, 
								survey.surveyData.instance.results.get(i), 
								generateBlank,
								0,
								i,
								repIndexes,
								gv,
								true, 
								parentRecords,
								remoteUser,
								oId,
								false);		
					}
				}

			}

		} finally {
			if(document != null) try {document.close();} catch (Exception e) {};
			if(writer != null) try {writer.close();} catch (Exception e) {};
			if(stamper != null) try {stamper.close();} catch (Exception e) {};
			if(reader != null) try {reader.close();} catch (Exception e) {};
		}

		return filename;

	}

	/*
	 * Get dependencies between question
	 */
	private void getDependencies(GlobalVariables gv,
			ArrayList<Result> record,
			org.smap.sdal.model.Survey survey,
			int recNumber) {

		for(int j = 0; j < record.size(); j++) {
			Result r = record.get(j);
			if(r.type.equals("form")) {
				for(int k = 0; k < r.subForm.size(); k++) {
					getDependencies(gv, r.subForm.get(k), survey, k);
				}
			} else {

				if(r.appearance != null && r.appearance.contains("pdfaddto")) {
					String name = getReferencedQuestion(r.appearance);
					if(name != null) {
						String refKey = r.fIdx + "_" + recNumber + "_" + name; 
						ArrayList<String> deps = gv.addToList.get(refKey);

						if(deps == null) {
							deps = new ArrayList<String> ();
							gv.addToList.put(refKey, deps);
						}
						deps.add(r.value);
					}
				}
			}
		}
	}

	private String getReferencedQuestion(String app) {
		String name = null;

		String [] appValues = app.split(" ");
		for(int i = 0; i < appValues.length; i++) {
			if(appValues[i].startsWith("pdfaddto")) {
				int idx = appValues[i].indexOf('_');
				if(idx > -1) {
					name = appValues[i].substring(idx + 1);
				}
				break;
			}
		}

		return name;
	}


	/*
	 * Fill the template with data from the survey
	 */
	private void fillTemplate(
			GlobalVariables gv,
			AcroFields pdfForm, 
			ArrayList<Result> record, 
			String formName,
			int repeatIndex,
			PdfStamper stamper,
			int oId) throws Exception {


		for(Result r : record) {

			String value = "";
			boolean hideLabel = false;
			String fieldName = getFieldName(formName, repeatIndex, r.name);
			String fieldNameQR = getFieldName(formName, repeatIndex, r.name + "_qr");

			DisplayItem di = new DisplayItem();
			try {
				Form form = survey.surveyData.forms.get(r.fIdx);
				Question question = PdfUtilities.getQuestionFromResult(sd, survey, r, form);
				PdfUtilities.setQuestionFormats(question.appearance, di);
			} catch (Exception e) {
				// If we can't get the question details for this data then that is ok
			}

			/*
			 * Set the value based on the result
			 * Process subforms if this is a repeating group
			 */
			if(r.type.equals("form")) {
				for(int k = 0; k < r.subForm.size(); k++) {
					fillTemplate(gv, pdfForm, r.subForm.get(k),fieldName, k, stamper, oId);
				} 
				
				/*
				 * Remove unused repeat fields
				 */
				if(r.subForm.size() > 0) {
					removeRepeatFields(stamper, pdfForm, r.subForm.get(0), r.subForm.size(), fieldName);
				}
				
			} else if(r.type.equals("select1")) {

				Form form = survey.surveyData.forms.get(r.fIdx);
				Question question = form.questions.get(r.qIdx);

				ArrayList<String> matches = new ArrayList<String> ();
				matches.add(r.value);
				value = choiceManager.getLabel(sd, cResults, user, oId, survey.surveyData.id, question.id, question.l_id, 
						question.external_choices, question.external_table, 
						survey.surveyData.languages.get(languageIdx).name, languageIdx, matches, survey.surveyData.ident, di.showImage);

			} else if(r.type.equals("select")) {

				String nameValue = r.value;
				if(nameValue != null) {
					String vArray [] = nameValue.split(" ");
					ArrayList<String> matches = new ArrayList<String> ();
					if(vArray != null) {
						for(String v : vArray) {
							if(v.trim().length() > 0) {
								matches.add(v);
							}
						}
					}
					Form form = survey.surveyData.forms.get(r.fIdx);
					Question question = form.questions.get(r.qIdx);
					value = choiceManager.getLabel(sd, cResults, user, oId, survey.surveyData.id, question.id, question.l_id,  question.external_choices, 
							question.external_table, 
							survey.surveyData.languages.get(languageIdx).name, languageIdx, matches, survey.surveyData.ident, false);
				}

			} else if(r.type.equals("dateTime") || r.type.equals("timestamp") || r.type.equals("date")) {

				value = PdfUtilities.getDateValue(di, tz, r.value, r.type);
				

			} else if(di.tsep && (r.type.equals("int") || (r.type.equals("string") && r.value != null && !r.value.contains(".")))) {
				long iValue = 0;
				try {
					if(r.value.trim().length() > 0) {	// Zero length strings should map to 0
						iValue = Long.parseLong(r.value.replace(",", ""));
					}
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
				value = String.format("%,d", iValue);
				
			} else if(di.tsep && (r.type.equals("decimal") || (r.type.equals("string") && r.value != null && r.value.contains(".")))) {
				Double dValue = 0.0;
				try {
					r.value = r.value.replace(",", "");
					if(di.round >= 0) {
						r.value = GeneralUtilityMethods.round(r.value, di.round);	
					}
					dValue = Double.parseDouble(r.value);
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
				DecimalFormat formatter = (DecimalFormat) DecimalFormat.getInstance(localisation.getLocale());
				value = formatter.format(dValue);

			} else if(r.type.equals("decimal") && di.round >= 0 && r.value != null && r.value.trim().length() > 0) {
				value = GeneralUtilityMethods.round(r.value, di.round);	
				
			} else {
				value = r.value;
			}

			/*
			 * Add the value to the form
			 * Alternatively remove the fieldName if the value is empty.
			 * pdf_fields have empty values as the real value is a composite of other questions so allow these
			 */
			if(value == null || value.trim().equals("") && (!(r.type.equals("pdf_field") && di.linemap != null))) {
				try {
					pdfForm.removeField(fieldName);
				} catch (Exception e) {
					log.info("Error removing field: " + fieldName + ": " + e.getMessage());
				}

			} else if(r.type.equals("geopoint") || r.type.equals("geoshape") || r.type.equals("geotrace") || r.type.startsWith("geopolygon_") 
					|| r.type.startsWith("geolinestring_") || r.type.equals("geocompound")) {

				PushbuttonField ad = pdfForm.getNewPushbuttonFromField(fieldName);
				if(ad != null) {
					
					PdfMapValues mapValues = new PdfMapValues();
					if(r.type.equals("geocompound")) {
						mapValues.orderedMarkers = r.markers;	
						mapValues.geoCompound = true;
					} 
					mapValues.geometry = r.value;
					
					Image img = PdfUtilities.getMapImage(sd, di.mapSource, di.map, di.account, mapValues, 
							di.location, di.zoom,gv.mapbox_key, gv.google_key, gv.maptiler_key,
							survey.surveyData.id,
							user,
							di.markerColor,
							mBasePath);
					PdfUtilities.addMapImageTemplate(pdfForm, ad, fieldName, img, di.stretch);
				} else {
					log.info("No field for image (Mapbox not called: " + fieldName);
				}

			} else if(r.type.equals("pdf_field") && di.linemap != null) {

				PdfMapValues mapValues = PdfUtilities.getMapValues(survey, di);
				
				if(di.linemap.type.equals("location") && di.markerLocation != null) {
					
					String location = PdfUtilities.getMarkerCoordinates(mapValues, di.markerLocation.type, di.markerLocation.count);
					pdfForm.setField(fieldName, location);
					
				} else {
					Float width = (float) 200.0;
					Float height = (float) 100.0;
	
					// If a push button field is used set the image size from that field
					PushbuttonField ad = pdfForm.getNewPushbuttonFromField(fieldName);
					if(ad != null) {
						Rectangle rect = ad.getBox();
						width = rect.getWidth();
						height = rect.getHeight();
					}
					
					TrafficLightValues tlValues = getTrafficLightValues(di);
					PreparedStatement pstmt = null;
					try {
						pstmt = mapValues.getDistancePreparedStatement(sd);	// Prepared statement to get distances
						PdfUtilities.sequenceMarkers(pstmt, mapValues);		// Put markers in sequence increasing from start
					} finally {
						 if(pstmt != null) try{pstmt.close();} catch(Exception e) {}
					}
					Image img = null;
					if(di.linemap.type.equals("map")) {
						img = PdfUtilities.getMapImage(sd, di.mapSource, di.map, 
								di.account, 
								mapValues,
								di.location, di.zoom, gv.mapbox_key,gv.google_key, gv.maptiler_key,
								survey.surveyData.id,
								user,
								di.markerColor,
								mBasePath);
					} else {
						img = PdfUtilities.getLineImage(sd, 
								mapValues,
								tlValues,
								survey.surveyData.id,
								user,
								di,
								mBasePath,
								width,
								height);
					}
	
					PdfUtilities.addMapImageTemplate(pdfForm, ad, fieldName, img, di.stretch);
				}


			} else if(r.type.equals("image") || r.type.equals("video") || r.type.equals("audio")  || r.type.equals("file")) {
				PdfUtilities.addImageTemplate(pdfForm, fieldName, mBasePath, value, mAttachmentPrefix, stamper, defaultFontLink, di.stretch);

			} else if(r.type.equals("select1") && di.showImage) {
				String filePath = UtilityMethodsEmail.getMediaPath(survey.surveyData.ident, value, mBasePath, oId, survey.surveyData.id);
				if(filePath != null) {
					// remove base path from file path as it will be added in again
					String remnantPath = filePath.substring(mBasePath.length());
					PdfUtilities.addImageTemplate(pdfForm, fieldName, mBasePath, remnantPath, mAttachmentPrefix, stamper, defaultFontLink, di.stretch);
				}
			} else {				
				if(hideLabel) {
					try {
						pdfForm.removeField(fieldName);
					} catch (Exception e) {
						log.info("Error removing field: " + fieldName + ": " + e.getMessage());
					}
				} else {
					if(di.isBarcode) {
						PushbuttonField ad = pdfForm.getNewPushbuttonFromField(fieldName);
						if(ad != null) {
							BarcodeQRCode qrcode = new BarcodeQRCode(value.trim(), 1, 1, null);
							Image qrcodeImage = qrcode.getImage();
							qrcodeImage.setAbsolutePosition(10,500);
							qrcodeImage.scalePercent(200);
							PdfUtilities.addMapImageTemplate(pdfForm, ad, fieldName, qrcodeImage, di.stretch);
						}
					} else {
						pdfForm.setField(fieldName, value);

					}
				}	
			} 

			/*
			 * Add any QR code values to fields that have been identified using the QR suffix
			 */
			if(fieldNameQR != null && value != null && value.trim().length() > 0) {
				PushbuttonField ad = pdfForm.getNewPushbuttonFromField(fieldName);
				if(ad != null) {
					BarcodeQRCode qrcode = new BarcodeQRCode(value.trim(), 1, 1, null);
					Image qrcodeImage = qrcode.getImage();
					qrcodeImage.setAbsolutePosition(10,500);
					qrcodeImage.scalePercent(200);
					PdfUtilities.addMapImageTemplate(pdfForm, ad, fieldNameQR, qrcodeImage, di.stretch);
				}
			}
		}
	}

	/*
	 * Remove repeating fields which have not been populated
	 */
	private void removeRepeatFields(
			PdfStamper stamper,
			AcroFields pdfForm, 
			ArrayList<Result> record, 
			int size,
			String formName
			) throws Exception {

		for(Result r : record) {
			int repeatIndex = size;
			boolean exists = true;
			while(exists) {
				String fieldName = getFieldName(formName, repeatIndex++, r.name);
				PushbuttonField current = pdfForm.getNewPushbuttonFromField(fieldName);
				if(current != null) {
					PushbuttonField ad = new PushbuttonField(stamper.getWriter(), current.getBox(), null);
					pdfForm.replacePushbuttonField(fieldName, ad.getField());
				}
				exists = pdfForm.removeField(fieldName);
			}
		}
	}
	
	
	private String getFieldName(String formName, int index, String qName) {
		String name = null;

		if(formName == null || formName.equals("")) {
			name = qName;
		} else {
			name = formName + "[" + index + "]." + qName;
		}
		return name;
	}

	private class UserSettings {
		String title;
		String license;
	}

	/*
	 * Fill the template with data from the survey
	 */
	private static void fillTemplateUserDetails(AcroFields pdfForm, User user, String basePath) throws IOException, DocumentException {
		try {

			pdfForm.setField("user_name", user.name);
			pdfForm.setField("user_company", user.company_name);

			/*
			 * As interim use a hard coded class to hold the data
			 */
			String settings = user.settings;
			Type type = new TypeToken<UserSettings>(){}.getType();
			Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
			UserSettings us = gson.fromJson(settings, type);

			if(us != null) {
				pdfForm.setField("user_title", us.title);
				pdfForm.setField("user_license", us.license);

				PushbuttonField ad = pdfForm.getNewPushbuttonFromField("user_signature");
				if(ad != null) {
					ad.setLayout(PushbuttonField.LAYOUT_ICON_ONLY);
					ad.setProportionalIcon(true);
					String filename = null;
					try {
						filename = basePath + "/media/users/" + user.id + "/sig/"  + user.signature;
						ad.setImage(Image.getInstance(filename));
					} catch (Exception e) {
						log.info("Error: Failed to add signature " + filename + " to pdf");
					}
					pdfForm.replacePushbuttonField("user_signature", ad.getField());
				} else {
					//log.info("Picture field: user_signature not found");
				}
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Error filling template", e);
		}
	}



	/*
	 * Get an XML Parser
	 */
	private Parser getXMLParser() {

		Parser parser = new Parser();

		// CSS
		CSSResolver cssResolver = new StyleAttrCSSResolver();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(mBasePath + "_bin" + DEFAULT_CSS);
			CssFile cssFile = XMLWorkerHelper.getCSS(fis);
			cssResolver.addCss(cssFile);
		} catch(Exception e) {
			log.log(Level.SEVERE, "Failed to get CSS file", e);
			cssResolver = XMLWorkerHelper.getInstance().getDefaultCssResolver(true);
		} finally {
			try {fis.close();} catch (Exception e) {}
		}



		// Pipelines
		parser.elements = new ElementList();
		ElementHandlerPipeline end = new ElementHandlerPipeline(parser.elements, null);

		String os = System.getProperty("os.name");
		log.info("Operating System:" + os);


		XMLWorkerFontProvider fontProvider = new XMLWorkerFontProvider();

		if(os.startsWith("Mac")) {
			fontProvider.register("/Library/Fonts/NotoSansBengali-Regular.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/Library/Fonts/NotoNaskhArabic-Regular.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/Library/Fonts/NotoSansBengali-Bold.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/Library/Fonts/NotoSans-Regular.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/Library/Fonts/NotoSans-Bold.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/Library/Fonts/NotoSansDevanagari-Light.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/Library/Fonts/NotoSansDevanagari-Bold.ttf", BaseFont.IDENTITY_H);


		} else if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") > 0) {
			// Linux / Unix
			fontProvider.register("/usr/share/fonts/truetype/NotoSansBengali-Regular.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/usr/share/fonts/truetype/NotoNaskhArabic-Regular.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/usr/share/fonts/truetype/NotoNaskhArabic-Regular.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/usr/share/fonts/truetype/NotoSans-Regular.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/usr/share/fonts/truetype/NotoSans-Bold.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/usr/share/fonts/truetype/NotoSansDevanagari-Light.ttf", BaseFont.IDENTITY_H);
			fontProvider.register("/usr/share/fonts/truetype/NotoSansDevanagari-Bold.ttf", BaseFont.IDENTITY_H);
		}

		/* debug
		System.out.println("Fonts present in " + fontProvider.getClass().getName());
	    	Set<String> registeredFonts = fontProvider.getRegisteredFonts();
	        for (String font : registeredFonts)
	        	System.out.println(font);
		 */

		CssAppliers cssAppliers = new CssAppliersImpl(fontProvider);

		// HTML
		HtmlPipelineContext htmlContext = new HtmlPipelineContext(cssAppliers);
		htmlContext.setTagFactory(Tags.getHtmlTagProcessorFactory());
		htmlContext.autoBookmark(false);
		HtmlPipeline html = new HtmlPipeline(htmlContext, end);
		CssResolverPipeline css = new CssResolverPipeline(cssResolver, html);

		XMLWorker worker = new XMLWorker(css, true);  
		parser.xmlParser = new XMLParser(worker);

		return parser;

	}

	/*
	 * Process the form
	 * Attempt to follow the standard set by enketo for the layout of forms so that the same layout directives
	 *  can be applied to showing the form on the screen and generating the PDF
	 */
	private void processForm(
			Parser parser,
			Document document,  
			ArrayList<Result> record,
			boolean generateBlank,
			int depth,
			int length,
			int[] repIndexes,
			GlobalVariables gv,
			boolean appendix,
			ArrayList<ArrayList<Result>> parentRecords,
			String remoteUser,
			int oId,
			boolean showSubFormIndex) throws Exception {

		// Check that the depth of repeats hasn't exceeded the maximum
		if(depth > repIndexes.length - 1) {
			depth = repIndexes.length - 1;	
		}

		/*
		 * Get the start geopoint value if it has been set
		 */
		String startGeopointValue = null;
		boolean hideStartGeopoint = false;
		int startGeopointIndex = -1;
		for(int j = 0; j < record.size(); j++) {
			Result r = record.get(j);

			if(r.type.equals("geopoint")) {
				Form form = survey.surveyData.forms.get(r.fIdx);
				Question question = PdfUtilities.getQuestionFromResult(sd, survey, r, form);
				if(!question.visible) {
					startGeopointValue = r.value;
					startGeopointIndex = j;
					break;
				} 
			}
		}

		/*
		 * Process
		 */
		boolean firstQuestion = true;
		for(int j = 0; j < record.size(); j++) {
			Result r = record.get(j);
			if(r.type.equals("form")) {

				firstQuestion = true;			// Make sure there is a gap when we return from the sub form
				// If this is a blank template check to see the number of times we should repeat this sub form
				showSubFormIndex = showSubFormIndex(r.appearance);
				if(generateBlank) {
					int blankRepeats = getBlankRepeats(r.appearance);
					for(int k = 0; k < blankRepeats; k++) {
						repIndexes[depth] = k;
						processForm(
								parser, 
								document, 
								r.subForm.get(0), 
								generateBlank, 
								depth + 1,
								k,
								repIndexes,
								gv,
								appendix,
								null,
								remoteUser,
								oId,
								showSubFormIndex);
					}
				} else {
					for(int k = 0; k < r.subForm.size(); k++) {
						// Maintain array list of parent records in order to look up ${values}
						parentRecords.add(0, record);		// Push this record in at the beginning of the list as we want to search most recent first
						repIndexes[depth] = k;
						processForm(
								parser, 
								document, 
								r.subForm.get(k),
								generateBlank, 
								depth + 1,
								k,
								repIndexes,
								gv,
								appendix,
								parentRecords,
								remoteUser,
								oId,
								showSubFormIndex);
					} 
				}
			} else {
				// Process the question

				Form form = survey.surveyData.forms.get(r.fIdx);
				Question question = PdfUtilities.getQuestionFromResult(sd, survey, r, form);

				if(question != null) {
					if(includeResult(r, question, appendix, gv, generateBlank, startGeopointIndex,
							hideStartGeopoint, j)) {
						if(question.type.equals("begin group")) {
							if(question.isNewPage()) {
								document.newPage();
							}
						}
						if(question.type.equals("end group")) {
							//ignore
						} else {

							Row row = prepareRow(record, survey, j, gv, length, appendix, 
									parentRecords, 
									generateBlank,
									startGeopointIndex,
									hideStartGeopoint);

							PdfPTable newTable = processRow(
									parser, 
									row, 
									generateBlank, 
									depth, 
									repIndexes, 
									gv,
									remoteUser,
									oId,
									startGeopointValue,
									showSubFormIndex);

							newTable.setWidthPercentage(100);
							newTable.setKeepTogether(true);

							// Add a gap if this is the first question of the record
							// or the previous row was at a different depth
							if(firstQuestion) {
								newTable.setSpacingBefore(5);
							} else {
								newTable.setSpacingBefore(row.spaceBefore());
							}
							firstQuestion = false;

							// Start a new page if the first question needs to be on a new page
							if(row.items.get(0).isNewPage) {
								document.newPage();
							}
							document.add(newTable);
							j += row.items.size() - 1;	// Jump over multiple questions if more than one was added to the row
						}
					}
				} else {
					log.info("Question Idx not found: " + r.qIdx);
				}

			}
		}

		return;
	}

	/*
	 * Make a decision as to whether this result should be included in the PDF
	 */
	private boolean includeResult(Result r, org.smap.sdal.model.Question question, 
			boolean appendix,
			GlobalVariables gv,
			boolean generateBlank,
			int startGeopointIndex,
			boolean hideStartGeopoint,
			int index) {

		boolean include = true;
		boolean inMeta = question.inMeta;

		if(index == startGeopointIndex && hideStartGeopoint) {
			// Don't include if the question is a start geopoint and there is a conventional geometry object in this form
			include = false;
		} else if(!generateBlank && mExcludeEmpty && isSkipped(question, r) ) {
			// Don't include the question if it has been marked as not to be included
			include = false;
		} else if(question.appearance != null) {
			if(question.appearance.contains("pdfno")) {
				include = false;
			} else {
				boolean appendixQuestion = question.appearance.contains("pdfapp");
				if(appendixQuestion) {
					gv.hasAppendix = true;
				}
				if(appendix && !appendixQuestion || (!appendix && appendixQuestion) ) {
					include = false;
				}
			}
		} else {
			// Questions without appearance should not appear in the appendix
			if(appendix) {
				include = false;
			}
		}

		if(include) {
			if(r.name == null) {
				include = false;
			} else if(r.name.startsWith("meta") && r.type.equals("begin group")){
				include = false;
			} else if(inMeta) {
				include = false;
			} else if(r.name.startsWith("meta_group")) {
				include = false;
			} else if(r.name.equals("prikey") || r.name.equals("parkey")) {
				include = false;
			} else if(r.name.equals("user")  && r.qIdx < 0) {
				include = false;
			} else if(r.name.equals("instancename")  && r.qIdx < 0) {
				include = false;
			} else if(r.name.equals("_hrk")) {
				include = false;
			}
		}

		return include;
	}


	/*
	 * Add the table row to the document
	 */
	private PdfPTable processRow(
			Parser parser, 
			Row row, 
			boolean generateBlank,
			int depth,
			int[] repIndexes,
			GlobalVariables gv,
			String remoteUser,
			int oId,
			String startGeopointValue,
			boolean showSubFormIndex) throws BadElementException, MalformedURLException, IOException {

		// Add a column for each level of repeats so that the repeat number can be shown
		PdfPTable table = new PdfPTable((showSubFormIndex ? depth : 0) + NUMBER_TABLE_COLS);	

		// Add the cells to record repeat indexes
		if(showSubFormIndex) {
			for(int i = 0; i < depth; i++) {
				PdfPCell c = new PdfPCell();
				c.addElement(new Paragraph(String.valueOf(repIndexes[i] + 1), defaultFont));
				c.setBackgroundColor(BaseColor.LIGHT_GRAY);
				table.addCell(c);
			}
		}

		int spanCount = NUMBER_TABLE_COLS;
		int numberItems = row.items.size();
		for(DisplayItem di : row.items) {

			boolean hideLabel = false;
			if(di.hideRepeatingLabels && depth > 0 && repIndexes[depth - 1] > 0) {
				hideLabel = true;
			}
			PdfPCell cell = new PdfPCell(addDisplayItem(parser, di, 
					mBasePath, 
					mHyperlinkPrefix,
					mAttachmentPrefix, 
					generateBlank, 
					gv, remoteUser, oId, startGeopointValue, hideLabel));
			cell.setBorderColor(BaseColor.LIGHT_GRAY);

			// Make sure the last cell extends to the end of the table
			if(numberItems == 1) {
				di.width = spanCount;
			}
			cell.setColspan(di.width);
			table.addCell(cell);

			numberItems--;
			spanCount -= di.width;
		}
		return table;
	}

	/*
	 * Return true if an answer has not been supplied to a question
	 */
	private boolean isSkipped(org.smap.sdal.model.Question q, Result r) {
		boolean skipped = false;
		boolean choiceSet = false;

		/*
		if(r.choices != null) {
			for(Result c : r.choices) {
				if(c.isSet) {
					choiceSet = true;
					break;
				}
			}
		}
		 */
		if(!q.type.equals("note")) {
			skipped = ((r.value == null || r.value.trim().length() == 0) && !choiceSet);
		}
		return skipped;
	}

	/*
	 * Add a row of questions
	 * Each row is created as a table
	 * converts questions and results to display items
	 * As many display items are added as will fit in the current groupWidth
	 * If the total width of the display items does not add up to the groupWidth then the last item
	 *  will be extended so that the total is equal to the group width
	 */
	private Row prepareRow(
			ArrayList<Result> record, 
			org.smap.sdal.model.Survey survey, 
			int offset,
			GlobalVariables gv,
			int recNumber,
			boolean appendix,
			ArrayList<ArrayList<Result>> parentRecords,
			boolean generateBlank,
			int startGeopointIndex,
			boolean hideStartGeopoint) throws Exception {

		Row row = new Row();
		row.groupWidth = gv.cols.length;

		for(int i = offset; i < record.size(); i++) {
			Result r = record.get(i);

			Form form = survey.surveyData.forms.get(r.fIdx);
			Question question = PdfUtilities.getQuestionFromResult(sd, survey, r, form);

			Label label = null;
			if(question.display_name != null && question.display_name.trim().length() > 0) {
				// Use display name in preference to labels if it exists
				label = new Label();
				label.text = question.display_name;
			} else {
				// Use labels as this is the old way
				if(question.labels.size() > 0) {
					label = question.labels.get(languageIdx);
				} else {
					label = new Label();
					if(question.source != null) {
						// Probably should have a label
						log.info("Error: No label found for question: " + question.name);
					}
				}
			}

			boolean isNewPage = question.isNewPage();

			if(i == offset) {
				// First question of row - update the number of columns
				int [] updateCols = question.updateCols(gv.cols);
				if(updateCols != null) {
					gv.cols = updateCols;			// Can only update the number of columns with the first question of the row
				}

				includeQuestion(row.items, gv, i, label, question, offset, survey, r, isNewPage, 
						recNumber,
						record,
						parentRecords);
			} else if(i - offset < gv.cols.length) {
				// 2nd or later questions in the row
				int [] updateCols = question.updateCols(gv.cols);		// Returns null if the number of columns has not changed


				if(updateCols == null || isNewPage) {
					if(includeResult(r, question, appendix, gv, 
							generateBlank,
							startGeopointIndex,
							hideStartGeopoint,
							i)) {
						includeQuestion(row.items, 
								gv, 
								i, 
								label, 
								question, 
								offset, 
								survey, 
								r, 
								isNewPage, 
								recNumber,
								record,
								parentRecords);
					}
				} else {
					// If the question updated the number of columns then we will need to start a new row
					break;
				}


			} else {
				break;
			}


		}
		return row;
	}

	/*
	 * Include question in the row
	 */
	private void includeQuestion(ArrayList<DisplayItem> items, GlobalVariables gv, int colIdx, Label label, 
			org.smap.sdal.model.Question question,
			int offset,
			org.smap.sdal.model.Survey survey,
			Result r,
			boolean isNewPage,
			int recNumber,
			ArrayList<Result> record,
			ArrayList<ArrayList<Result>> parentRecords) throws Exception {

		int [] cols = gv.cols;
		DisplayItem di = new DisplayItem();
		di.width = cols[colIdx-offset];		
		if(question.type != null && question.type.equals("calculate")) {
			di.text = "";		// Hack to remove labels from calculate questions
		} else {
			di.text = label.text == null ? "" : label.text;
		}
		di.text = lookupReferenceValue(di.text, record, parentRecords);

		di.hint = label.hint ==  null ? "" : label.hint;
		di.hint = lookupReferenceValue(di.hint, record, parentRecords);

		di.type = question.type;
		di.name = question.name;
		di.value = r.value;
		di.isNewPage = isNewPage;

		PdfUtilities.setQuestionFormats(question.appearance, di);
		di.fIdx = r.fIdx;
		di.qIdx = r.qIdx;
		di.rec_number = recNumber;
		di.markers = r.markers;			// geocompound markers

		items.add(di);
	}

	/*
	 * Where a label includes a reference value such as ${name} then these need to be converted to the actual value
	 */
	public String lookupReferenceValue(String input, ArrayList<Result> record, ArrayList<ArrayList<Result>> parentRecords) {

		StringBuffer newValue = new StringBuffer("");
		String v;

		// Return if we are generating a blank template
		if(parentRecords == null) {
			return input;
		}

		Pattern pattern = Pattern.compile("\\$\\{.+?\\}");
		java.util.regex.Matcher matcher = pattern.matcher(input);
		int start = 0;
		while (matcher.find()) {

			String matched = matcher.group();
			String qname = matched.substring(2, matched.length() - 1);

			// Add any text before the match
			int startOfGroup = matcher.start();
			newValue.append(input.substring(start, startOfGroup));

			// Add the matched value after lookup
			// First check in the current record
			v = lookupInRecord(qname, record);

			// If not found try each of the parent records starting from the closest
			if(v == null) {
				for(ArrayList<Result> p : parentRecords) {
					v = lookupInRecord(qname, p);
					if(v != null) {
						break;
					}
				}
			}

			// Still null!  well maybe this ${..} pattern was just meant to be
			if(v == null) {
				v = matcher.group();
			}
			newValue.append(v);

			// Reset the start
			start = matcher.end();

		}

		// Get the remainder of the string
		if(start < input.length()) {
			newValue.append(input.substring(start));		
		}

		return newValue.toString();
	}

	/*
	 * Lookup the value of a question in a record
	 */
	private String lookupInRecord(String name, ArrayList<Result> record) {
		String value = null;

		if(name != null) {
			for(Result r : record) {
				if(r.name.equals(name)) {
					value = r.value;
					break;
				}
			}
		}

		return value;
	}
	
	/*
	 * Get an array of values for the specified group of questions in the survey
	 * There will only be more than one value if the question is in a repeat
	 * Note colors are primary and will determine the number of bulbs that are returned
	 */
	ArrayList<TrafficLightBulb> lookupGroupInSurvey(TrafficLightBulb qGroup, ArrayList<ArrayList<Result>> records) {
		ArrayList<TrafficLightBulb> values = new ArrayList<>();
		if(qGroup != null && qGroup.color != null && qGroup.color.length() > 0 && records != null && records.size() > 0) {
			
			ArrayList<String> colors = new ArrayList<>();
			for(ArrayList<Result> r : records) {
				for(Result result : r) {
					if(result.subForm == null && result.name.equals(qGroup.color)) {
						colors.add(result.value != null ? result.value : "");
					} else if(result.subForm != null) {
						colors.addAll(PdfUtilities.lookupInSurvey(qGroup.color, result.subForm));
					}
				}		
			}
			
			// Get the crosses
			ArrayList<String> crosses = new ArrayList<>();
			if(qGroup.cross != null && qGroup.cross.length() > 0) {
				for(ArrayList<Result> r : records) {
					for(Result result : r) {
						if(result.subForm == null && result.name.equals(qGroup.cross)) {
							crosses.add(result.value != null ? result.value : "");
						} else if(result.subForm != null) {
							crosses.addAll(PdfUtilities.lookupInSurvey(qGroup.cross, result.subForm));
						}
					}		
				}
			}
			
			// Get the labels
			ArrayList<String> labels = new ArrayList<>();
			if(qGroup.label != null && qGroup.label.length() > 0) {
				for(ArrayList<Result> r : records) {
					for(Result result : r) {
						if(result.subForm == null && result.name.equals(qGroup.label)) {
							labels.add(result.value != null ? result.value : "");
						} else if(result.subForm != null) {
							labels.addAll(PdfUtilities.lookupInSurvey(qGroup.label, result.subForm));
						}
					}		
				}
			}
			
			// Combine the bulb components
			for(int i = 0; i < colors.size(); i++) {
				TrafficLightBulb b = new TrafficLightBulb();
				b.color = colors.get(i);
				if(i < crosses.size()) {
					b.cross = crosses.get(i);
				} else {
					b.cross = "";
				}
				if(i < labels.size()) {
					b.label = labels.get(i);
				} else {
					b.label = "";
				}
				values.add(b);
				
			}
			
		}
		return values;
	}

	/*
	 * Get the number of blank repeats to generate
	 */
	int getBlankRepeats(String appearance) {
		int repeats = 1;

		if(appearance != null) {
			String [] appValues = appearance.split(" ");
			if(appearance != null) {
				for(int i = 0; i < appValues.length; i++) {
					if(appValues[i].startsWith("pdfrepeat")) {
						String [] parts = appValues[i].split("_");
						if(parts.length >= 2) {
							repeats = Integer.valueOf(parts[1]);
						}
						break;
					}
				}
			}
		}

		return repeats;
	}

	/*
	 * Return true if sub form index should be shown
	 */
	boolean showSubFormIndex(String appearance) {
		boolean show = true;

		if(appearance != null) {
			String [] appValues = appearance.split(" ");
			if(appearance != null) {
				for(int i = 0; i < appValues.length; i++) {
					if(appValues[i].equals("pdfhideindex")) {
						show = false;
					}
				}
			}
		}

		return show;
	}

	/*
	 * Add the question label, hint, and any media
	 */
	private PdfPTable addDisplayItem(
			Parser parser, 
			DisplayItem di, 
			String basePath,
			String hyperlinkPrefix,
			String attachmentPrefix,
			boolean generateBlank,
			GlobalVariables gv,
			String remoteUser,
			int oId,
			String startGeopointValue,
			boolean hideLabel) throws BadElementException, MalformedURLException, IOException {

		PdfPCell labelCell = new PdfPCell();
		PdfPCell valueCell = new PdfPCell();
		labelCell.setBorderColor(BaseColor.LIGHT_GRAY);
		valueCell.setBorderColor(BaseColor.LIGHT_GRAY);

		PdfPTable tItem = null;

		// Add label
		if(!hideLabel) {

			// Get text value
			String textValue = "";
			if(di.text != null && di.text.trim().length() > 0) {
				textValue = di.text;
			} else {
				textValue = di.name;
			}
			textValue = textValue.trim();

			if(di.labelcaps) {
				textValue = textValue.toUpperCase();
			}

			ArrayList<StringElement> elements = null;
			/*
			 * If the language does not use latin characters then break it up into 
			 * elements and process each spearately.  However if pure latin then the
			 * pdf parser seems to work better and does not add newlines after an html token
			 */
			if(GeneralUtilityMethods.getLanguage(textValue).equals("")) {
				elements = new ArrayList<> ();
				StringElement se = new StringElement();
				se.htmlToken = false;
				se.text = textValue;
				elements.add(se);
			} else {
				elements = getStringElements(textValue);
			}

			StringBuffer html = new StringBuffer();
			for(StringElement se : elements) {

				if(se.htmlToken) {
					html.append(se.text);
				} else {
					html.append("<span class='label ");
					if(di.labelbold) {
						html.append(" lbold");
					}

					// Add language class
					html.append(GeneralUtilityMethods.getLanguage(se.text));
					html.append("'>");

					// Add text value
					html.append(GeneralUtilityMethods.unesc(se.text));
					html.append("</span>");
				}
			}

			// Only include hints if we are generating a blank template
			if(generateBlank) {
				html.append("<span class='hint ");
				if(di.hint != null) {
					html.append(GeneralUtilityMethods.getLanguage(di.hint));
					html.append("'>");
					html.append(GeneralUtilityMethods.unesc(di.hint));
				}
				html.append("</span>");
			}

			parser.elements.clear();
			try {
				parser.xmlParser.parse(new StringReader(html.toString()));

				for(Element element : parser.elements) {
					if(textValue != null && textValue.length() > 0) {
						if(GeneralUtilityMethods.isRtlLanguage(textValue)) {
							labelCell.setRunDirection(PdfWriter.RUN_DIRECTION_RTL);
						}
					} else if(di.hint != null && di.hint.length() > 0) {
						if(GeneralUtilityMethods.isRtlLanguage(textValue)) {
							labelCell.setRunDirection(PdfWriter.RUN_DIRECTION_RTL);
						}
					}
					labelCell.addElement(element);
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Error parsing: " + html.toString() + " : " + e.getMessage(), e);
				String msg = e.getMessage();
				if(msg == null) {
					msg = "Error in PDF generation. Ignoring.";
				}
				lm.writeLog(sd, survey.getId(), remoteUser, LogManager.ERROR, msg + " for: " + html.toString(), 0, null);
				labelCell.addElement(getPara(html.toString(), di, gv, null, null));
			}
		}



		// Set the content of the value cell
		try {
			updateValueCell(parser, remoteUser, valueCell, di, 
					generateBlank, basePath, 
					hyperlinkPrefix, 
					attachmentPrefix,
					gv, 
					oId,
					startGeopointValue);
		} catch (Exception e) {
			log.info("Error updating value cell, continuing: " + basePath + " : " + di.value);
			log.log(Level.SEVERE, "Exception", e);
		}

		int widthValue = 5;
		if(di.widthLabel == 10) {
			widthValue = 1;	// Label and value in 1 column
			di.widthLabel = 1;
			tItem = new PdfPTable(1); 
		} else {
			// Label and value in 2 columns
			widthValue = 10 - di.widthLabel;
			tItem = new PdfPTable(10);
		}
		// Format label cell
		labelCell.setColspan(di.widthLabel);
		if(di.labelbg != null) {
			labelCell.setBackgroundColor(di.labelbg);
		}

		// Format value cell
		valueCell.setColspan(widthValue);
		if(di.valueHeight > -1.0) {
			valueCell.setMinimumHeight((float)di.valueHeight);
		}
		if(di.valuebg != null) {
			valueCell.setBackgroundColor(di.valuebg);
		}

		tItem.addCell(labelCell);
		tItem.addCell(valueCell);
		return tItem;
	}

	/*
	 * Set the contents of the value cell
	 */
	private void updateValueCell(
			Parser parser,
			String remoteUser,
			PdfPCell valueCell, 
			DisplayItem di, 
			boolean generateBlank, 
			String basePath,
			String hyperlinkPrefix,
			String attachmentPrefix,
			GlobalVariables gv,
			int oId,
			String startGeopointValue
			) throws Exception {


		// Questions that append their values to this question
		ArrayList<String> deps = gv.addToList.get(di.fIdx + "_" + di.rec_number + "_" + di.name);


		if(di.type.startsWith("select")) {
			processSelect(parser, remoteUser, valueCell, di, generateBlank, gv, oId);
		} else if (di.type.equals("image")) {
			if(di.value != null && !di.value.trim().equals("") && !di.value.trim().equals("Unknown")) {
				if(di.isHyperlink) {
					Anchor anchor = new Anchor(hyperlinkPrefix + di.value);
					anchor.setReference(hyperlinkPrefix + di.value);

					valueCell.addElement(getPara("", di, gv, deps, anchor));
				} else {
					try {
						File f = new File(basePath + "/" + di.value);
						if(!f.exists()) {
							f = new File(attachmentPrefix + di.value);
						}
						if(f.exists()) {
							float angle = getImageRotation(new FileInputStream(f));
							
							Image img = Image.getInstance(f.getAbsolutePath()); 
							img.setRotationDegrees(angle);
							valueCell.addElement(img);
						}
						
						
					} catch(Exception e) {
						log.info("Error: image " + basePath + "/" + di.value + " not added: " + e.getMessage());
						log.log(Level.SEVERE, "Adding image to pdf", e);
					}
				}

			} else {
				// TODO add empty image
			}

		} else if (di.type.equals("video") || di.type.equals("audio") || di.type.equals("file")) {
			if(di.value != null && !di.value.trim().equals("") && !di.value.trim().equals("Unknown")) {
				Anchor anchor = new Anchor(hyperlinkPrefix + di.value);
				anchor.setReference(hyperlinkPrefix + di.value);

				valueCell.addElement(getPara("", di, gv, deps, anchor));

			} else {
				// TODO add empty image
			}

		} else if(di.type.equals("geopoint") || di.type.equals("geoshape") || di.type.equals("geotrace") 
				|| di.type.equals("geocompound") || di.type.startsWith("geopolygon_") || di.type.startsWith("geolinestring_")) {

			PdfMapValues mapValues = new PdfMapValues();		
			if(di.type.equals("geocompound")) {
				mapValues.orderedMarkers = di.markers;	
				mapValues.geoCompound = true;
			} 
			
			mapValues.geometry = di.value;
			mapValues.startGeometry = startGeopointValue;
			
			Image img = PdfUtilities.getMapImage(sd, di.mapSource, di.map, 
					di.account, 
					mapValues,
					di.location, di.zoom, 
					gv.mapbox_key,
					gv.google_key,
					gv.maptiler_key,
					survey.surveyData.id,
					user,
					di.markerColor,
					basePath);

			if(img != null) {
				valueCell.addElement(img);
			} else {
				valueCell.addElement(getPara(" ", di, gv, deps, null));
			}

		} else if(di.type.equals("pdf_field") && di.linemap != null) { 
			
			PdfMapValues mapValues = PdfUtilities.getMapValues(survey, di);	
			
			if(di.linemap.type.equals("location") && di.markerLocation != null) {
				
				String location = PdfUtilities.getMarkerCoordinates(mapValues, di.markerLocation.type, di.markerLocation.count);
				valueCell.addElement(getPara(location, di, gv, deps, null));
				
			} else {
			
				PreparedStatement pstmt = null;			
				TrafficLightValues tlValues = getTrafficLightValues(di);
				try {
					pstmt = mapValues.getDistancePreparedStatement(sd);	// Prepared statement to get distances
					PdfUtilities.sequenceMarkers(pstmt, mapValues);		// Put markers in sequence increasing from start
				} finally {
					 if(pstmt != null) try{pstmt.close();} catch(Exception e) {}
				}
				
				Image img = null;
				Float width = (float) 200.0;
				Float height = (float) 100.0;
				if(di.linemap.type.equals("map")) {
					img = PdfUtilities.getMapImage(sd, di.mapSource, di.map, 
							di.account, 
							mapValues,
							di.location, di.zoom, gv.mapbox_key,gv.google_key,gv.maptiler_key,
							survey.surveyData.id,
							user,
							di.markerColor,
							basePath);
				} else {
					img = PdfUtilities.getLineImage(sd, 
							mapValues,
							tlValues,
							survey.surveyData.id,
							user,
							di,
							basePath,
							width,
							height);
				}
				
				if(img != null) {
					valueCell.addElement(img);
				} else {
					valueCell.addElement(getPara(" ", di, gv, deps, null));
				}
			}

		} else if(di.isBarcode) { 
			BarcodeQRCode qrcode = new BarcodeQRCode(di.value.trim(), 1, 1, null);
			Image qrcodeImage = qrcode.getImage();
			qrcodeImage.setAbsolutePosition(10,500);
			qrcodeImage.scalePercent(200);

			valueCell.addElement((qrcodeImage));

		} else if(di.tsep && (di.type.equals("int") || (di.type.equals("string") && di.value != null && !di.value.contains(".")))) {
			long iValue = 0;
			try {
				iValue = Long.parseLong(di.value.replace(",", ""));
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			DecimalFormat formatter = (DecimalFormat) DecimalFormat.getInstance(localisation.getLocale());
			String value = formatter.format(iValue);
			valueCell.addElement(getPara(value, di, gv, deps, null));
		} else if(di.tsep && (di.type.equals("decimal") || (di.type.equals("string") && di.value != null && di.value.contains(".")))) {
			Double dValue = 0.0;
			try {
				di.value = di.value.replace(",", "");
				if(di.round >= 0) {
					di.value = GeneralUtilityMethods.round(di.value, di.round);	
				}
				dValue = Double.parseDouble(di.value);
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			
			DecimalFormat formatter = (DecimalFormat) DecimalFormat.getInstance(localisation.getLocale());
			String value = formatter.format(dValue);
			

			
			valueCell.addElement(getPara(value, di, gv, deps, null));
		} else if(di.type.equals("decimal") && di.round >= 0 && di.value != null && di.value.trim().length() > 0) {
			String value = GeneralUtilityMethods.round(di.value, di.round);	
			valueCell.addElement(getPara(value, di, gv, deps, null));
		} else {
			String value = null;

			if(di.value == null || di.value.trim().length() == 0) {
				value = " ";	// Need a space to show a blank row
			} else {
				if(di.value != null && di.value.length() > 0) {
					if(GeneralUtilityMethods.isRtlLanguage(di.value)) {
						valueCell.setRunDirection(PdfWriter.RUN_DIRECTION_RTL);
					}
				}

				if(di.type.equals("dateTime") || di.type.equals("timestamp") || di.type.equals("date")) {		// Set date time to local time				
					
					value = PdfUtilities.getDateValue(di, tz, di.value, di.type);
					

				} else {
					value = di.value;
				}

				
			}
			valueCell.addElement(getPara(value, di, gv, deps, null));
		}
	}
	
	/*
	 * Get the image rotation
	 */
	float getImageRotation(InputStream imageBytes) throws MetadataException, ImageProcessingException, IOException {
		Metadata metadata = ImageMetadataReader.readMetadata(imageBytes);
		ExifIFD0Directory exifIFD0Directory = metadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
		int orientation = exifIFD0Directory.getInt(ExifIFD0Directory.TAG_ORIENTATION);

		float angle = 0;
		switch (orientation)
		{
		case 1:
		case 2:
		    angle = 0; break;
		case 3:
		case 4:
		    angle = (float) 180; break;
		case 5:
		case 6:
		    angle = (float) -90; break;
		case 7:
		case 8:
		    angle = (float) 90; break;
		}
		return angle;
	}
	
	/*
	 * Extract the compound traffic light values from the traffic light questions
	 */
	private TrafficLightValues getTrafficLightValues(DisplayItem di) {
		TrafficLightValues tlValues = new TrafficLightValues();
		
		if(di.trafficLight != null && di.trafficLight.lights != null && di.trafficLight.lights.size() > 0) {
			
			tlValues.lights = new ArrayList<> ();			
			
			for(ArrayList<TrafficLightBulb> singleLight : di.trafficLight.lights) {	
				ArrayList<TrafficLightBulb> values = new ArrayList<> ();
				for(TrafficLightBulb bulb : singleLight) {
					values.addAll(lookupGroupInSurvey(bulb, survey.surveyData.instance.results));
				}
				tlValues.lights.add(values);
			}	
			
		}
		
		return tlValues;
	}
	
	private Paragraph getPara(String value, DisplayItem di, GlobalVariables gv, ArrayList<String> deps, Anchor anchor) {

		boolean hasContent = false;
		Font f = null;
		boolean isRtl = false;
		String lang = "";

		Paragraph para = new Paragraph("", defaultFont);

		if(value != null && value.trim().length() > 0) {
			lang = GeneralUtilityMethods.getLanguage(value);
			f = getFont(lang);
			isRtl = isRtl(lang);
			para.add(new Chunk(GeneralUtilityMethods.unesc(value), f));
			hasContent = true;
		}

		// Add dependencies

		if(deps != null) {
			for(String n : deps) {
				if(n != null && n.trim().length() > 0) {
					if(hasContent) {
						para.add(new Chunk(",", defaultFont));
					}

					lang = GeneralUtilityMethods.getLanguage(n);
					f = getFont(lang);
					if(!isRtl) {		// Don't override RTL if it has already been set
						isRtl = isRtl(lang);
					}
					para.add(new Chunk(n, f));
				}

			}
		}
		if(anchor != null) {
			para.setFont(defaultFontLink);
			para.add(anchor);
			para.setFont(defaultFontLink);
		}
		return para;
	}

	/*
	 * HTML equivalent of getPara
	 */
	private String getHtml(String value, DisplayItem di, GlobalVariables gv, ArrayList<String> deps) {

		boolean hasContent = false;

		StringBuffer html = new StringBuffer();
		html.append("<span>");
		if(value != null && value.trim().length() > 0) {
			html.append(value);
			hasContent = true;
		}

		// Add dependencies

		if(deps != null) {
			for(String n : deps) {
				if(n != null && n.trim().length() > 0) {
					if(hasContent) {
						html.append(",");
					}			
					html.append(n);
				}

			}
		}
		html.append("</span>");

		return html.toString();
	}


	private Font getFont(String lang) {
		Font f = defaultFont;

		if(lang.length() > 0) {
			if(lang.equals("arabic")) {
				f = arabicFont;
			} else if(lang.equals("bengali")) {
				f = bengaliFont;
			} else if(lang.equals("devanagari")) {
				f = devanagariFont;
			}	
		} 

		return f;
	}

	private boolean isRtl(String lang) {
		boolean isRtl = false;

		if(lang.length() > 0) {
			if(lang.equals("arabic")) {
				isRtl = true;
			} 	
		} 
		return isRtl;
	}

	private void processSelect(Parser parser, String remoteUser, PdfPCell cell, DisplayItem di,
			boolean generateBlank,
			GlobalVariables gv, int oId) throws Exception {

		Font f = null;
		boolean isRtl = false;

		// If generating blank template
		List list = new List();
		list.setAutoindent(false);
		list.setSymbolIndent(24);

		String lang;

		boolean isSelectMultiple = di.type.equals("select") ? true : false;

		// Questions that append their values to this question
		ArrayList<String> deps = gv.addToList.get(di.fIdx + "_" + di.rec_number + "_" + di.name);

		/*
		 * Add the value of this question unless
		 *   The form is not blank and the value is "other" and their are 1 or more dependent questions
		 *   In this case we assume that its only the values of the dependent questions that are needed
		 */

		if(generateBlank) {
			Form form = survey.surveyData.forms.get(di.fIdx);
			Question question = form.questions.get(di.qIdx);
			OptionList ol = survey.surveyData.optionLists.get(question.list_name);
			for(Option o : ol.options) {

				String text = null;
				if(o.display_name != null && o.display_name.trim().length() > 0) {
					text = o.display_name;
				} else {
					text = o.labels.get(languageIdx).text;
				}
				lang = GeneralUtilityMethods.getLanguage(text);
				f = getFont(lang);
				isRtl = isRtl(lang);

				ListItem item = new ListItem(GeneralUtilityMethods.unesc(text), f);

				if(isSelectMultiple) {	
					item.setListSymbol(new Chunk("\uf096", Symbols)); 
					list.add(item);
				} else {
					item.setListSymbol(new Chunk("\uf10c", Symbols)); 
					list.add(item);
				}
			}

			if(isRtl) {
				cell.setRunDirection(PdfWriter.RUN_DIRECTION_RTL);
			}
			cell.addElement(list);

		} else {
			if(deps == null || (di.value != null && !di.value.trim().toLowerCase().equals("other"))) {

				String value = di.value;
				
				if(di.type.equals("select1")) {

					Form form = survey.surveyData.forms.get(di.fIdx);
					Question question = form.questions.get(di.qIdx);

					ArrayList<String> matches = new ArrayList<String> ();
					matches.add(di.value);
					value = choiceManager.getLabel(sd, cResults, user, oId, survey.surveyData.id, question.id, question.l_id, 
							question.external_choices, question.external_table, 
							survey.surveyData.languages.get(languageIdx).name, languageIdx, matches, survey.surveyData.ident, di.showImage);
				} else if(di.type.equals("select")) {
					String nameValue = value;
					if(nameValue != null) {
						String vArray [] = nameValue.split(" ");
						ArrayList<String> matches = new ArrayList<String> ();
						if(vArray != null) {
							for(String v : vArray) {
								matches.add(v);
							}
						}
						Form form = survey.surveyData.forms.get(di.fIdx);
						Question question = form.questions.get(di.qIdx);
						value = choiceManager.getLabel(sd, cResults, user, oId, survey.surveyData.id, question.id, 
								question.l_id, question.external_choices, question.external_table, 
								survey.surveyData.languages.get(languageIdx).name, languageIdx, matches, survey.surveyData.ident, false);		// Do not get images for multi select
					}
				}

				if(GeneralUtilityMethods.isRtlLanguage(di.value)) {
					cell.setRunDirection(PdfWriter.RUN_DIRECTION_RTL);
				}
				
				if(di.showImage) {
					String filePath = UtilityMethodsEmail.getMediaPath(survey.surveyData.ident, value, mBasePath, oId, survey.surveyData.id);
					if(filePath != null) {
						File imageFile = new File(filePath);
						Image img = null;
						if(imageFile.exists()) {
							img = Image.getInstance(filePath);
						} 
						if(img != null) {
							cell.addElement(img);
						} else {
							cell.addElement(getPara(" ", di, gv, deps, null));
						}
					}
				} else {
					parser.elements.clear();
					String html = getHtml(value, di, gv, deps);
					try {
						parser.xmlParser.parse(new StringReader(html));
					} catch (Exception e) {
						log.log(Level.SEVERE, "Error parsing: " + html.toString() + " : " + e.getMessage(), e);
						String msg = e.getMessage();
						if(msg == null) {
							msg = "Error parsing PDF. Ignoring."; 
						}
						lm.writeLog(sd, survey.getId(), remoteUser, LogManager.ERROR, msg + " for: " + html.toString(), 0, null);
						cell.addElement(getPara(html.toString(), di, gv, null, null));
					}
					for(Element element : parser.elements) {					
						cell.addElement(element);
					}
				}
			}

		}

	}

	/*
	 * Fill in user details for the output when their is no template
	 */
	private void fillNonTemplateUserDetails(Document document, User user, String basePath, String key) throws IOException, DocumentException {

		String settings = user.settings;
		Type type = new TypeToken<UserSettings>(){}.getType();
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		UserSettings us = gson.fromJson(settings, type);

		float indent = (float) 20.0;
		addValue(document, localisation.getString("pdf_completed_by") + ":", (float) 0.0);
		if(user.signature != null && user.signature.trim().length() > 0) {
			String fileName = null;
			try {
				//fileName = basePath + user.signature;

				fileName = basePath + "/media/users/" + user.id + "/sig/"  + user.signature;

				Image img = Image.getInstance(fileName);
				img.scaleToFit(200, 50);
				img.setIndentationLeft(indent);

				document.add(img);

			} catch (Exception e) {
				log.info("Error: Failed to add signature (non template) " + fileName + " to pdf: " + e.getMessage());
			}
		}
		addValue(document, user.name, indent);
		addValue(document, localisation.getString("cr_key") + ": ", (float) 0.0);
		addValue(document, key, indent);
		addValue(document, user.company_name, indent);
		if(us != null) {
			addValue(document, us.title, indent);
			addValue(document, us.license, indent);
		}

	}

	/*
	 * Format a single value into a paragraph
	 */
	private void addValue(Document document, String value, float indent) throws DocumentException {

		Font f = null;
		String lang;

		if(value != null && value.trim().length() > 0) {
			lang = GeneralUtilityMethods.getLanguage(value);
			f = getFont(lang);

			Paragraph para = new Paragraph("", f);	
			para.setIndentationLeft(indent);
			para.add(new Chunk(GeneralUtilityMethods.unesc(value), f));
			document.add(para);
		}
	}

	private ArrayList<StringElement> getStringElements(String in) {
		ArrayList<StringElement> elements = new ArrayList<> ();
		String elementString = null;
		while(in != null && in.length() > 0) {
			StringElement token = getNextToken(in);
			if(token != null) {
				elementString = in.substring(0, token.index);
			} else {
				elementString = in;
			}

			int processed = 0;
			if(elementString != null && elementString.length() > 0) {
				StringElement e = new StringElement();
				e.htmlToken = false;
				e.index = 0;
				e.text = elementString;
				elements.add(e);
				processed += elementString.length();
			}
			if(token != null) {
				elements.add(token);
				processed += token.text.length();
			}

			if(processed == 0) {
				in = null;
			} else {
				in = in.substring(processed);
			}
		}
		return elements;
	}

	private StringElement getNextToken(String in) {
		StringElement token = null;
		if(in != null && in.length() > 0) {
			int idx = in.indexOf('<');
			if(idx >= 0) {

				if(in.indexOf("<b>", idx) == idx) {
					token = new StringElement();
					token.index = idx;
					token.text = "<b>";
					token.htmlToken = true;
				} else if(in.indexOf("</b>", idx) == idx) {
					token = new StringElement();
					token.index = idx;
					token.text = "</b>";
					token.htmlToken = true;
				} else {
					token = getNextToken(in.substring(idx + 1));
				}
			}
		}
		return token;
	}

}


