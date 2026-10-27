package org.smap.sdal.Utilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Checks a Cloudflare Turnstile token with Cloudflare.  Used by public forms and by the
 * registration page.
 */
public class TurnstileVerifier {

	private static Logger log = Logger.getLogger(TurnstileVerifier.class.getName());

	private TurnstileVerifier() {
	}

	/*
	 * True if Cloudflare accepts the token.  False if it does not, or cannot be asked.
	 */
	public static boolean verify(String token, String secretKey, String remoteIp) {
		if(token == null || token.isEmpty() || secretKey == null || secretKey.isEmpty()) {
			return false;
		}
		try {
			URL url = new URL("https://challenges.cloudflare.com/turnstile/v0/siteverify");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setConnectTimeout(5000);
			conn.setReadTimeout(5000);
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

			String body = "secret=" + URLEncoder.encode(secretKey, StandardCharsets.UTF_8)
					+ "&response=" + URLEncoder.encode(token, StandardCharsets.UTF_8)
					+ (remoteIp != null ? "&remoteip=" + URLEncoder.encode(remoteIp, StandardCharsets.UTF_8) : "");
			try(OutputStream os = conn.getOutputStream()) {
				os.write(body.getBytes(StandardCharsets.UTF_8));
			}

			StringBuilder sb = new StringBuilder();
			try(BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
				String line;
				while((line = br.readLine()) != null) { sb.append(line); }
			}
			return sb.toString().contains("\"success\":true");
		} catch(Exception e) {
			log.log(Level.WARNING, "Turnstile API call failed", e);
			return false;
		}
	}
}
