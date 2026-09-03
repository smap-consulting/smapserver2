import com.itextpdf.text.*;
import com.itextpdf.text.pdf.*;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;
import com.itextpdf.tool.xml.ElementList;
import com.itextpdf.tool.xml.XMLWorker;
import com.itextpdf.tool.xml.XMLWorkerFontProvider;
import com.itextpdf.tool.xml.XMLWorkerHelper;
import com.itextpdf.tool.xml.css.CssFile;
import com.itextpdf.tool.xml.css.StyleAttrCSSResolver;
import com.itextpdf.tool.xml.html.CssAppliersImpl;
import com.itextpdf.tool.xml.html.Tags;
import com.itextpdf.tool.xml.parser.XMLParser;
import com.itextpdf.tool.xml.pipeline.css.CSSResolver;
import com.itextpdf.tool.xml.pipeline.css.CssResolverPipeline;
import com.itextpdf.tool.xml.pipeline.end.ElementHandlerPipeline;
import com.itextpdf.tool.xml.pipeline.html.HtmlPipeline;
import com.itextpdf.tool.xml.pipeline.html.HtmlPipelineContext;
import org.smap.sdal.Utilities.PdfUtilities;

import java.io.*;
import java.nio.file.*;

/*
 * Acceptance tests for the pdf paths that can be exercised without a database.  Run them
 * with run.sh in this directory after a build of sdDAL
 *
 * Written for the revert from iText 8 to iText 5, where the reported problem was errors
 * in templated pdfs
 *
 *  - image creation, including webp, which iText cannot read itself
 *  - filling and flattening a template whose NeedAppearances is set, the way LibreOffice
 *    sets it, with and without the fix so the difference is visible
 *  - an image into a template push button, and the link glyph into a field that cannot
 *    hold an image, which is the behaviour templates are sized for
 *  - the xmlworker html pipeline against the restored default_pdf.css
 *  - resizePdf and removeBlankPages
 */
public class PdfTests {

	static int failures = 0;
	static final String GLYPH = "";			// fontawesome external link
	// Where PDFSurveyManager looks for the fonts
	static final String FONT_DIR = System.getProperty("os.name").startsWith("Mac")
			? "/Library/Fonts/" : "/usr/share/fonts/truetype/";

	public static void main(String[] args) throws Exception {
		File dir = new File(args.length > 0 ? args[0] : "target/pdf-tests");
		String webp = args.length > 1 ? args[1] : "test/pdf/sample.webp";
		String css = args.length > 2 ? args[2]
				: "../setup/deploy/version1/resources/css/default_pdf.css";
		dir.mkdirs();
		File base = new File(dir, "base");
		new File(base, "attachments").mkdirs();

		Files.copy(Paths.get(webp), new File(base, "attachments/photo.webp").toPath(),
				StandardCopyOption.REPLACE_EXISTING);
		makePng(new File(base, "attachments/sig.png"));

		System.out.println("Images");
		images(base);

		System.out.println("Template");
		File template = makeTemplate(new File(dir, "template.pdf"));
		checkFilled(fill(template, base, new File(dir, "filled.pdf"), true), true);

		System.out.println("Template without the appearance fix, to show what it does");
		checkFilled(fill(template, base, new File(dir, "unfixed.pdf"), false), false);

		System.out.println("Html labels");
		html(css);

		System.out.println("Pdf utilities");
		resize(new File(dir, "filled.pdf"), new File(dir, "resized.pdf"));
		blankPages(new File(dir, "blanks.pdf"), new File(dir, "noblanks.pdf"));

		System.out.println(failures == 0 ? "\nALL PASSED" : "\n" + failures + " FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}

	static void images(File base) throws Exception {
		Image webp = PdfUtilities.createImage(new File(base, "attachments/photo.webp"));
		check("webp file -> image", webp.getWidth() == 64, (int) webp.getWidth() + "x" + (int) webp.getHeight());

		Image png = PdfUtilities.createImage(new File(base, "attachments/sig.png").getAbsolutePath());
		check("png path -> image", png.getWidth() == 120, (int) png.getWidth() + "x" + (int) png.getHeight());

		byte [] bytes = Files.readAllBytes(new File(base, "attachments/photo.webp").toPath());
		check("webp bytes -> image", PdfUtilities.createImage(bytes, "photo.webp").getWidth() == webp.getWidth(),
				"same size as from the file");

		try {
			PdfUtilities.createImage(new File(base, "attachments/none.webp"));
			check("a missing file still throws", false, "no exception");
		} catch (IOException e) {
			check("a missing file still throws", true, e.getClass().getSimpleName());
		}
	}

	/*
	 * A template like the ones in use: text fields with a default appearance, an image push
	 * button, a signature push button, a text field where a media link goes, and
	 * NeedAppearances set the way LibreOffice sets it
	 */
	static File makeTemplate(File out) throws Exception {
		Document doc = new Document(PageSize.A4);
		PdfWriter writer = PdfWriter.getInstance(doc, new FileOutputStream(out));
		doc.open();
		doc.add(new Paragraph("Template"));

		addText(writer, "user_name", new Rectangle(40, 700, 300, 720));
		addText(writer, "q1", new Rectangle(40, 660, 300, 680));
		addText(writer, "clip", new Rectangle(40, 470, 70, 484));		// too small for a url
		addButton(writer, "photo", new Rectangle(40, 500, 240, 640));
		addButton(writer, "user_signature", new Rectangle(300, 560, 460, 640));

		writer.getAcroForm().setNeedAppearances(true);		// what LibreOffice does
		doc.close();
		check("template built", out.length() > 0, out.length() + " bytes");
		return out;
	}

	static void addText(PdfWriter writer, String name, Rectangle r) throws Exception {
		TextField tf = new TextField(writer, r, name);
		tf.setFont(BaseFont.createFont(BaseFont.HELVETICA, BaseFont.WINANSI, BaseFont.NOT_EMBEDDED));
		tf.setFontSize(10);
		writer.addAnnotation(tf.getTextField());
	}

	static void addButton(PdfWriter writer, String name, Rectangle r) throws Exception {
		PushbuttonField b = new PushbuttonField(writer, r, name);
		b.setLayout(PushbuttonField.LAYOUT_ICON_ONLY);
		writer.addAnnotation(b.getField());
	}

	/*
	 * Fill it the way PDFSurveyManager does
	 */
	static File fill(File template, File base, File out, boolean generateAppearances) throws Exception {
		PdfReader reader = new PdfReader(template.getAbsolutePath());
		PdfStamper stamper = new PdfStamper(reader, new FileOutputStream(out));
		AcroFields form = stamper.getAcroFields();

		if(generateAppearances) {
			form.setGenerateAppearances(true);		// the LibreOffice fix
			check("template has its five fields", form.getFields().size() == 5,
					form.getFields().keySet().toString());
		}

		form.setField("user_name", "Neil Penman");
		form.setField("q1", "the quick brown fox");

		Font symbols = FontFactory.getFont(FONT_DIR + "fontawesome-webfont.ttf",
				BaseFont.IDENTITY_H, BaseFont.EMBEDDED, 12);
		PdfUtilities.addImageTemplate(form, "photo", base.getAbsolutePath(),
				"attachments/photo.webp", "https://example.com", stamper, symbols, false);
		PdfUtilities.addImageTemplate(form, "user_signature", base.getAbsolutePath(),
				"attachments/sig.png", "https://example.com", stamper, symbols, false);
		PdfUtilities.addImageTemplate(form, "clip", base.getAbsolutePath(),
				"attachments/clip.mp4", "https://example.com", stamper, symbols, false);

		stamper.setFormFlattening(true);
		stamper.close();
		reader.close();
		return out;
	}

	static void checkFilled(File filled, boolean fixed) throws Exception {
		PdfReader reader = new PdfReader(filled.getAbsolutePath());
		String text = PdfTextExtractor.getTextFromPage(reader, 1).replace('\n', ' ').trim();

		if(fixed) {
			check("flattened value renders", text.contains("Neil Penman"), text);
			check("second value renders", text.contains("the quick brown fox"), "");
			check("no fields left after flatten", reader.getAcroFields().getFields().isEmpty(), "");
			check("both button images are in the page", images(reader) >= 2, images(reader) + " image xobjects");
			check("link glyph in the field that cannot hold an image", text.contains(GLYPH), "");
		} else {
			// Without setGenerateAppearances the values do not survive the flatten
			check("values are lost without the fix, as reported", !text.contains("Neil Penman"), text);
		}
		reader.close();
	}

	static int images(PdfReader reader) throws Exception {
		int found = 0;
		PdfDictionary res = reader.getPageN(1).getAsDict(PdfName.RESOURCES);
		PdfDictionary xo = res == null ? null : res.getAsDict(PdfName.XOBJECT);
		if(xo != null) {
			for(PdfName n : xo.getKeys()) {
				found += countImages(PdfReader.getPdfObject(xo.get(n)), 0);
			}
		}
		return found;
	}

	static int countImages(PdfObject o, int depth) {
		if(!(o instanceof PdfStream) || depth > 3) {
			return 0;
		}
		PdfStream s = (PdfStream) o;
		if(PdfName.IMAGE.equals(s.getAsName(PdfName.SUBTYPE))) {
			return 1;
		}
		int found = 0;
		PdfDictionary res = s.getAsDict(PdfName.RESOURCES);
		PdfDictionary xo = res == null ? null : res.getAsDict(PdfName.XOBJECT);
		if(xo != null) {
			for(PdfName n : xo.getKeys()) {
				found += countImages(PdfReader.getPdfObject(xo.get(n)), depth + 1);
			}
		}
		return found;
	}

	/*
	 * The label pipeline: the restored default_pdf.css through xmlworker, the way
	 * PDFSurveyManager builds it
	 */
	static void html(String cssPath) throws Exception {
		CSSResolver cssResolver = new StyleAttrCSSResolver();
		try (FileInputStream fis = new FileInputStream(cssPath)) {
			CssFile cssFile = XMLWorkerHelper.getCSS(fis);
			cssResolver.addCss(cssFile);
			check("default_pdf.css parses under xmlworker", cssFile != null, cssPath);
		}

		XMLWorkerFontProvider fontProvider = new XMLWorkerFontProvider();
		for(String f : new String [] {"NotoSans-Regular.ttf", "NotoSans-Bold.ttf",
				"NotoNaskhArabic-Regular.ttf", "NotoSansBengali-Regular.ttf",
				"NotoSansDevanagari-Light.ttf"}) {
			fontProvider.register(FONT_DIR + f, BaseFont.IDENTITY_H);
		}

		ElementList elements = new ElementList();
		HtmlPipelineContext hpc = new HtmlPipelineContext(new CssAppliersImpl(fontProvider));
		hpc.setAcceptUnknown(true).autoBookmark(true).setTagFactory(Tags.getHtmlTagProcessorFactory());
		new XMLParser(new XMLWorker(new CssResolverPipeline(cssResolver,
				new HtmlPipeline(hpc, new ElementHandlerPipeline(elements, null))), true))
				.parse(new StringReader("<span class='label'>How many <b>pits</b> are there?</span>"));
		check("an html label produces elements", !elements.isEmpty(), elements.size() + " elements");

		ElementList rtl = new ElementList();
		HtmlPipelineContext hpc2 = new HtmlPipelineContext(new CssAppliersImpl(fontProvider));
		hpc2.setAcceptUnknown(true).autoBookmark(true).setTagFactory(Tags.getHtmlTagProcessorFactory());
		new XMLParser(new XMLWorker(new CssResolverPipeline(cssResolver,
				new HtmlPipeline(hpc2, new ElementHandlerPipeline(rtl, null))), true))
				.parse(new StringReader("<span class='label'>مرحبا latin</span>"));
		check("an arabic and latin label produces elements", !rtl.isEmpty(), rtl.size() + " elements");
	}

	static void resize(File in, File out) throws Exception {
		try (OutputStream os = new FileOutputStream(out)) {
			PdfUtilities.resizePdf(in.getAbsolutePath(), os);
		}
		PdfReader reader = new PdfReader(out.getAbsolutePath());
		check("resizePdf keeps the page", reader.getNumberOfPages() == 1,
				in.length() + " bytes in, " + out.length() + " out");
		reader.close();
	}

	static void blankPages(File in, File out) throws Exception {
		Document doc = new Document();
		PdfWriter writer = PdfWriter.getInstance(doc, new FileOutputStream(in));
		doc.open();
		doc.add(new Paragraph("page one"));
		doc.newPage();
		writer.setPageEmpty(false);			// force a genuinely empty page out
		doc.newPage();
		doc.add(new Paragraph("page three"));
		doc.close();

		PdfReader before = new PdfReader(in.getAbsolutePath());
		int pages = before.getNumberOfPages();
		before.close();

		PdfUtilities.removeBlankPages(in.getAbsolutePath(), out.getAbsolutePath());
		PdfReader reader = new PdfReader(out.getAbsolutePath());
		check("removeBlankPages drops the empty page", pages == 3 && reader.getNumberOfPages() == 2,
				pages + " pages in, " + reader.getNumberOfPages() + " out");
		reader.close();
	}

	static void makePng(File f) throws Exception {
		java.awt.image.BufferedImage bi = new java.awt.image.BufferedImage(120, 40,
				java.awt.image.BufferedImage.TYPE_INT_RGB);
		java.awt.Graphics2D g = bi.createGraphics();
		g.setColor(java.awt.Color.WHITE);
		g.fillRect(0, 0, 120, 40);
		g.setColor(java.awt.Color.BLACK);
		g.drawString("signature", 10, 25);
		g.dispose();
		javax.imageio.ImageIO.write(bi, "png", f);
	}

	static void check(String what, boolean ok, String detail) {
		if(!ok) {
			failures++;
		}
		System.out.println((ok ? "  ok   " : "  FAIL ") + what + (detail.isEmpty() ? "" : "   [" + detail + "]"));
	}
}
