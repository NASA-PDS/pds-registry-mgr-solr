package gov.nasa.pds.search.util;

import java.net.URLDecoder;
import java.util.regex.Pattern;

public class XssUtils {

	private XssUtils() {
	}

	// Patterns for Cross-Site Scripting filter.
	private static Pattern[] xssPatterns = new Pattern[] {
			// script fragments
			Pattern.compile("<script>(.*?)</script>", Pattern.CASE_INSENSITIVE),
			// src='...'
			Pattern.compile("src[\r\n]*=[\r\n]*\\\'(.*?)\\\'",
					Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
			Pattern.compile("src[\r\n]*=[\r\n]*\\\"(.*?)\\\"",
					Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
			// lonely script tags
			Pattern.compile("</script>", Pattern.CASE_INSENSITIVE),
			Pattern.compile("<script(.*?)>", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
			// eval(...)
			Pattern.compile("eval\\((.*?)\\)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
			// expression(...)
			Pattern.compile("expression\\((.*?)\\)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
			// javascript:...
			Pattern.compile("javascript:", Pattern.CASE_INSENSITIVE),
			// vbscript:...
			Pattern.compile("vbscript:", Pattern.CASE_INSENSITIVE),
			// onload(...)=...
			Pattern.compile("onload(.*?)=", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
			// alert(...)
			Pattern.compile("alert\\((.*?)\\)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL) };

	/**
	 * This method makes up a simple anti cross-site scripting (XSS) filter written
	 * for Java web applications. What it basically does is remove all suspicious
	 * strings from request parameters before returning them to the application.
	 */
	public static String clean(String value) {
		if (value != null) {
			// Avoid null characters
			value = value.replaceAll("\0", "");

			// Remove all sections that match a pattern
			for (Pattern scriptPattern : xssPatterns) {
				value = scriptPattern.matcher(value).replaceAll("");
			}

			// After all of the above has been removed just blank out the value
			// if any of the offending characters are present that facilitate
			// Cross-Site Scripting and Blind SQL Injection.
			// We normally exclude () but they often show up in queries.
			char badChars[] = { '|', ';', '$', '@', '\'', '"', '<', '>', ',', '\\', /* CR */ '\r', /* LF */ '\n',
					/* Backspace */ '\b' };
			try {
				String decodedStr = URLDecoder.decode(value);
				for (int i = 0; i < badChars.length; i++) {
					if (decodedStr.indexOf(badChars[i]) >= 0) {
						value = "";
					}
				}
			} catch (IllegalArgumentException e) {
				value = "";
			}
		}
		return value;
	}

}
