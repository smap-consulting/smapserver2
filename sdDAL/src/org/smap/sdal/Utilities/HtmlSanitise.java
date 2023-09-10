package org.smap.sdal.Utilities;

import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

public class HtmlSanitise {

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
	
	public String sanitiseHtml(String in) {
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
	public static String cleanName(String in) {
		String out = in.replace("<", "");
		out = out.replace(">", "");
		return out;
	}
}
