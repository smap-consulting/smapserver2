package org.smap.sdal.Utilities;

import java.io.File;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.smap.sdal.managers.PDFTableManager;
import org.smap.sdal.model.User;

import com.itextpdf.io.font.constants.StandardFonts;
import com.itextpdf.io.image.ImageDataFactory;
import com.itextpdf.kernel.colors.ColorConstants;
import com.itextpdf.kernel.events.Event;
import com.itextpdf.kernel.events.IEventHandler;
import com.itextpdf.kernel.events.PdfDocumentEvent;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.geom.Rectangle;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfPage;
import com.itextpdf.kernel.pdf.canvas.PdfCanvas;
import com.itextpdf.layout.Canvas;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Image;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import com.itextpdf.layout.properties.TextAlignment;
import com.itextpdf.layout.properties.UnitValue;

/*
 * Event handler to write headers and footers on each page (iText 8).
 * Replaces the iText 5 PdfPageEventHelper.  Register with:
 *   pdfDoc.addEventHandler(PdfDocumentEvent.END_PAGE, new PdfPageSizer(...));
 *
 * Note: iText 8 uses uniform document margins (no per-page margin change as the
 * old onStartPage did).  The caller should set the top margin large enough for
 * the page-1 title/logo band; this handler only draws, it does not alter margins.
 */
public class PdfPageSizer implements IEventHandler {
	User user = null;
	String title;
	String basePath;
	int marginLeft;
	int marginRight;
	int marginTop_2;
	int marginBottom_2;
	String surveyIdent;
	String defaultLogo;
	ArrayList<String> tableHeader;

	private static Logger log =
			 Logger.getLogger(PDFTableManager.class.getName());

	private PdfFont font;

	public PdfPageSizer(String title, User user, String basePath,
			ArrayList<String> tableHeader,
			int marginLeft,
			int marginRight,
			int marginTop_2,
			int marginBottom_2,
			String surveyIdent,
			String defaultLogo) {

		this.title = title;
		this.user = user;
		this.basePath = basePath;
		this.marginLeft = marginLeft;
		this.marginRight = marginRight;
		this.marginTop_2 = marginTop_2;
		this.marginBottom_2 = marginBottom_2;
		this.tableHeader = tableHeader;
		this.surveyIdent = surveyIdent;
		this.defaultLogo = defaultLogo;

		try {
			font = PdfFontFactory.createFont(StandardFonts.HELVETICA);
		} catch (Exception e) {
			log.fine("Failed to create Helvetica font for page sizer");
		}
	}

	@Override
	public void handleEvent(Event event) {

		PdfDocumentEvent docEvent = (PdfDocumentEvent) event;
		PdfDocument pdf = docEvent.getDocument();
		PdfPage page = docEvent.getPage();
		int pageNumber = pdf.getPageNumber(page);
		Rectangle pageRect = page.getPageSize();

		PdfCanvas pdfCanvas = new PdfCanvas(page.newContentStreamAfter(), page.getResources(), pdf);
		Canvas canvas = new Canvas(pdfCanvas, pageRect);

		try {
			// Optional repeating column header (drawn as a band under the top margin)
			if (tableHeader != null) {
				Table table = new Table(UnitValue.createPercentArray(tableHeader.size())).useAllAvailableWidth();
				for (String h : tableHeader) {
					Cell cell = new Cell()
							.add(new Paragraph(GeneralUtilityMethods.unesc(h)).setFont(font).setFontSize(10))
							.setBackgroundColor(ColorConstants.LIGHT_GRAY)
							.setBorderTop(new com.itextpdf.layout.borders.SolidBorder(ColorConstants.LIGHT_GRAY, 0.5f))
							.setBorderBottom(new com.itextpdf.layout.borders.SolidBorder(ColorConstants.LIGHT_GRAY, 0.5f));
					table.addCell(cell);
				}
				table.setFixedPosition(marginLeft, pageRect.getTop() - marginTop_2 - 30,
						pageRect.getWidth() - marginLeft - marginRight);
				canvas.add(table);
			}

			// Title and logo on the first page only
			if (pageNumber == 1) {
				canvas.showTextAligned(
						new Paragraph(title == null ? "" : title).setFont(font).setFontSize(18),
						(pageRect.getLeft() + pageRect.getRight()) / 2,
						pageRect.getTop() - 100, TextAlignment.CENTER);

				if (user != null) {
					String fileName = null;
					File f = null;
					try {
						if (defaultLogo != null && !defaultLogo.equals("none")) {
							// Try survey folder
							fileName = basePath + File.separator + "media" + File.separator +
									surveyIdent + File.separator + defaultLogo;
							f = new File(fileName);

							// Try organisation folder
							if (!f.exists()) {
								fileName = basePath + File.separator + "media" + File.separator +
										"organisation" + File.separator + user.o_id + File.separator +
										defaultLogo;
							}
							f = new File(fileName);
						}
						if (f == null || !f.exists()) {
							// try banner logo
							fileName = basePath + File.separator + "media" + File.separator +
									"organisation" + File.separator + user.o_id + File.separator +
									"settings" + File.separator + "bannerLogo";
							f = new File(fileName);
						}

						if (f.exists()) {
							Image img = new Image(PdfUtilities.createImageData(f));
							img.scaleToFit(200, 50);
							float w = img.getImageScaledWidth();
							img.setFixedPosition(
									pageRect.getRight() - (marginRight + w),
									pageRect.getTop() - 75);
							canvas.add(img);
						}
					} catch (Exception e) {
						log.fine("Error: Failed to add image " + fileName + " to pdf");
					}
				}
			}

			// Footer is always written
			if (user != null) {
				float mid = (pageRect.getLeft() + pageRect.getRight()) / 2;
				showFooterLine(canvas, user.company_name, mid, pageRect.getBottom() + 80);
				showFooterLine(canvas, user.company_address, mid, pageRect.getBottom() + 65);
				showFooterLine(canvas, user.company_phone, mid, pageRect.getBottom() + 50);
				showFooterLine(canvas, user.company_email, mid, pageRect.getBottom() + 35);
			}

			// Add page number
			showFooterLine(canvas, String.format("page %d", pageNumber),
					pageRect.getRight() - 100, pageRect.getBottom() + 25);

		} finally {
			canvas.close();
		}
	}

	private void showFooterLine(Canvas canvas, String text, float x, float y) {
		if (text == null) {
			text = "";
		}
		canvas.showTextAligned(new Paragraph(text).setFont(font).setFontSize(10),
				x, y, TextAlignment.CENTER);
	}
}
