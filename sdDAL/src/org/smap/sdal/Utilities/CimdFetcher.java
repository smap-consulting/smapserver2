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

package org.smap.sdal.Utilities;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Fetch a Client ID Metadata Document.
 *
 * Under the 2026-07-28 MCP specification a client identifies itself with an https URL and the
 * authorization server reads its metadata from there.  That means this server makes an outbound
 * request to an address chosen by whoever is authorizing, which is a server side request forgery
 * primitive: point it at 169.254.169.254 or at something on the loopback interface and the
 * response, or merely the timing of it, reports on the inside of the network.
 *
 * So every hop is checked, not just the first.  A redirect to an internal address is the obvious
 * way around a check applied only to the URL that was supplied.  The response is also bounded in
 * size and time, because a client controlled URL that never stops sending is a denial of service
 * against the authorize endpoint.
 */
public class CimdFetcher {

	private static Logger log = Logger.getLogger(CimdFetcher.class.getName());

	private static final int MAX_REDIRECTS = 3;
	private static final int MAX_BYTES = 64 * 1024;
	private static final int CONNECT_TIMEOUT_MS = 5000;
	private static final int READ_TIMEOUT_MS = 5000;

	public static class NotAllowed extends Exception {
		private static final long serialVersionUID = 1L;
		public NotAllowed(String msg) {
			super(msg);
		}
	}

	/*
	 * True if this looks like a CIMD client id rather than a registered one
	 */
	public static boolean isMetadataUrl(String clientId) {
		return clientId != null && clientId.startsWith("https://");
	}

	/*
	 * Read the document at the given https URL, following at most a few redirects and checking the
	 * address at every one.  Returns the body; the caller parses and validates the metadata.
	 */
	public static String fetch(String urlString) throws NotAllowed {

		String current = urlString;

		for(int hop = 0; hop <= MAX_REDIRECTS; hop++) {

			URL url = check(current);
			HttpURLConnection conn = null;
			try {
				conn = (HttpURLConnection) url.openConnection();
				conn.setInstanceFollowRedirects(false);		// Checked by hand, see above
				conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
				conn.setReadTimeout(READ_TIMEOUT_MS);
				conn.setRequestProperty("Accept", "application/json");
				conn.setRequestMethod("GET");

				int status = conn.getResponseCode();
				if(status == 301 || status == 302 || status == 303 || status == 307 || status == 308) {
					String location = conn.getHeaderField("Location");
					if(location == null) {
						throw new NotAllowed("Redirect with no location");
					}
					current = new URL(url, location).toString();
					continue;
				}
				if(status != 200) {
					throw new NotAllowed("Client metadata returned " + status);
				}

				return read(conn.getInputStream());

			} catch (NotAllowed e) {
				throw e;
			} catch (Exception e) {
				log.log(Level.WARNING, "Fetching client metadata from " + urlString, e);
				throw new NotAllowed("Could not read the client metadata document");
			} finally {
				if(conn != null) {
					conn.disconnect();
				}
			}
		}

		throw new NotAllowed("Too many redirects fetching client metadata");
	}

	/*
	 * https only, and never to an address inside this network.  Resolution happens here so the
	 * check is on where the request will actually go, not on how the host was spelled.
	 */
	private static URL check(String urlString) throws NotAllowed {

		URL url;
		try {
			url = new URL(urlString);
		} catch (Exception e) {
			throw new NotAllowed("Client id is not a URL");
		}

		if(!"https".equalsIgnoreCase(url.getProtocol())) {
			throw new NotAllowed("Client metadata must be served over https");
		}
		if(url.getRef() != null) {
			throw new NotAllowed("Client id must not contain a fragment");
		}

		InetAddress[] addresses;
		try {
			addresses = InetAddress.getAllByName(url.getHost());
		} catch (Exception e) {
			throw new NotAllowed("Client metadata host does not resolve");
		}

		/*
		 * Every address the name resolves to has to be acceptable.  A name that returns one public
		 * and one internal address would otherwise be a coin toss.
		 */
		for(InetAddress address : addresses) {
			if(isInternal(address)) {
				throw new NotAllowed("Client metadata host resolves to an address inside this network");
			}
		}

		return url;
	}

	private static boolean isInternal(InetAddress a) {
		return a.isAnyLocalAddress()
				|| a.isLoopbackAddress()
				|| a.isLinkLocalAddress()
				|| a.isSiteLocalAddress()
				|| a.isMulticastAddress()
				|| isUniqueLocalV6(a)
				|| isCloudMetadata(a);
	}

	/*
	 * fc00::/7, the IPv6 equivalent of a private range, which isSiteLocalAddress does not cover
	 */
	private static boolean isUniqueLocalV6(InetAddress a) {
		byte[] b = a.getAddress();
		return b.length == 16 && (b[0] & 0xfe) == 0xfc;
	}

	/*
	 * 169.254.169.254 is link local so already refused, but name it so the reason is on the record
	 */
	private static boolean isCloudMetadata(InetAddress a) {
		return "169.254.169.254".equals(a.getHostAddress());
	}

	private static String read(InputStream is) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buffer = new byte[8192];
		int total = 0;
		int n;
		while((n = is.read(buffer)) > 0) {
			total += n;
			if(total > MAX_BYTES) {
				throw new NotAllowed("Client metadata document is too large");
			}
			out.write(buffer, 0, n);
		}
		return new String(out.toByteArray(), StandardCharsets.UTF_8);
	}
}
