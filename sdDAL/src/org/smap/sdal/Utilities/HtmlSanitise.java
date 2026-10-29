package org.smap.sdal.Utilities;

import java.util.Arrays;
import java.util.ResourceBundle;

import org.owasp.html.CssSchema;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

public class HtmlSanitise {

	/*
	 * The markup allowed in labels and other text that cleanText is used on
	 */
	private PolicyFactory policy = new HtmlPolicyBuilder()
            .allowAttributes("src").onElements("img")
            .allowAttributes("href").onElements("a")
            .allowAttributes("color").onElements("font")
            .allowAttributes("face").onElements("font")
            .allowAttributes("style").onElements("span")
            .allowAttributes("style").onElements("div")
            .allowAttributes("class").onElements("span")
            .allowAttributes("class").onElements("div")
            .allowAttributes("data-value").onElements("span")
            .allowAttributes("data-value").onElements("div")
            .allowStandardUrlProtocols()
            .allowCommonBlockElements()
            .allowCommonInlineFormattingElements()
            .allowStyling()
            .allowElements(
            "a", "img",
            "big", "small", "b", "i", "u", "br", "em",
            "h1", "h2", "h3", "h4", "h5", "h6",
            "font", "span", "div", "p",
            "ul", "li", "ol",
            "table", "th", "td", "thead", "tbody"
            ).toFactory();

	/*
	 * The markup allowed in an email.  The label policy plus what email layouts use: whole
	 * tables, their sizing attributes, and display so a link can be styled as a button.
	 * No <style> blocks or <center>, use inline styles.
	 */
	private static final PolicyFactory emailPolicy = new HtmlPolicyBuilder()
            .allowAttributes("src", "alt", "width", "height").onElements("img")
            .allowAttributes("href", "title").onElements("a")
            .allowAttributes("color", "face").onElements("font")
            .allowAttributes("class", "data-value").onElements("span", "div")
            .allowAttributes("border", "cellpadding", "cellspacing", "width").onElements("table")
            .allowAttributes("colspan", "rowspan", "width").onElements("td", "th")
            .allowAttributes("align").onElements("p", "div", "td", "th", "table")
            .allowStandardUrlProtocols()
            .allowCommonBlockElements()
            .allowCommonInlineFormattingElements()
            .allowStyling(CssSchema.union(CssSchema.DEFAULT, CssSchema.withProperties(Arrays.asList("display"))))
            .allowElements(
            "a", "img",
            "big", "small", "b", "i", "u", "br", "em", "hr",
            "h1", "h2", "h3", "h4", "h5", "h6",
            "font", "span", "div", "p",
            "ul", "li", "ol",
            "table", "caption", "thead", "tbody", "tfoot", "tr", "th", "td"
            ).toFactory();

	/*
	 * Sanitise HTML that is going to be used as HTML, such as the body of an email.  Text in
	 * the result is escaped, "a < b" comes back as "a &lt; b", which is what an HTML page needs.
	 */
	public String sanitiseHtml(String in) {
		if(in == null) {
			return null;
		}
		return emailPolicy.sanitize(in);
	}

	/*
	 * Sanitise a label that is going to be used as HTML, such as in a web form.  Only the label
	 * markup is kept and text stays escaped, "a < b" comes back as "a &lt; b", so text that
	 * only looks like markup, such as "&lt;script&gt;", can never become a tag.
	 */
	public String sanitiseLabelHtml(String in) {
		if(in == null) {
			return null;
		}
		return policy.sanitize(in);
	}

	/*
	 * Remove unsafe markup from text that is stored or placed in a DOM text node, where the
	 * reader escapes it.  The escaping the sanitiser adds is reversed so that "a < b" and "A & B"
	 * stay as they were rather than showing as "&lt;" and "&amp;".
	 *
	 * Never use the result as HTML.  Reversing the escaping turns "&lt;script&gt;" back into
	 * "<script>", use sanitiseHtml for that.
	 */
	public String cleanText(String in) {
		String sanitised = policy.sanitize(in);
		sanitised = sanitised.replace("&amp;", "&");
		sanitised = sanitised.replace("&#39;", "'");
		sanitised = sanitised.replace("&#34;", "\"");
		sanitised = sanitised.replace("&#96;", "`");
		sanitised = sanitised.replace("&#61;", "=");
		sanitised = sanitised.replace("&#64;", "@");
		sanitised = sanitised.replace("&#43;", "+");
		sanitised = sanitised.replace("&lt;", "<");
		sanitised = sanitised.replace("&gt;", ">");

		return sanitised;
	}

	/*
	 * Lightweight function to remove invalid characters from a name
	 */
	public static String checkCleanName(String in, ResourceBundle localisation) throws ApplicationException {
		if(in != null) {
			if(in.contains("<") || in.contains(">")
					|| in.contains("https:") || in.contains("http:")) {
				String msg = localisation.getString("mf_in");
				msg = msg.replace("%s1", in);
				throw new ApplicationException(msg);
			}
			in = in.trim();
		}
		return in;

	}
}
