package org.smap.sdal.managers;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.util.EntityUtils;
import org.smap.sdal.Utilities.ApplicationException;

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

/*
 * Manage the sending of emails
 */
public class WebhookManager {

	private static Logger log =
			Logger.getLogger(WebhookManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	ResourceBundle localisation;
	
	/*
	 * The callback url is chosen by a user, and the request comes from inside this network, so it
	 * must not reach this server or the cloud metadata service.  Checked when the connection is made
	 * rather than on the url, so that a redirect, or a name that resolves differently the second
	 * time, cannot get past it.  Private network ranges are allowed for now as a self hosted server
	 * may post to another system on its own network.
	 */
	/*
	 * Webhooks are sent by the notification processor, so a callback url that never answers must
	 * not hold it up for ever
	 */
	private static final RequestConfig WEBHOOK_TIMEOUTS = RequestConfig.custom()
			.setConnectTimeout(30 * 1000)
			.setConnectionRequestTimeout(30 * 1000)
			.setSocketTimeout(60 * 1000)
			.build();
	
	private static final DnsResolver WEBHOOK_DNS = host -> {
		InetAddress[] addresses = SystemDefaultDnsResolver.INSTANCE.resolve(host);
		for(InetAddress a : addresses) {
			if(a.isLoopbackAddress() || a.isAnyLocalAddress() || a.isLinkLocalAddress() || a.isMulticastAddress()) {
				throw new UnknownHostException("Webhooks cannot call " + host + " as it is an address on this server");
			}
		}
		return addresses;
	};
	
	public WebhookManager(ResourceBundle l) {
		localisation = l;
	}
	
	// Send an email
	public void callRemoteUrl(String callbackUrl, String payload, String user, String password) throws Exception  {

		/*
		 * Parse the url rather than cutting it up so that a port is not taken as part of the host name
		 */
		URI uri = null;
		try {
			uri = URI.create(callbackUrl);
		} catch (Exception e) {
		}
		String protocol = uri == null ? null : uri.getScheme();
		if(uri == null || uri.getHost() == null || !("https".equalsIgnoreCase(protocol) || "http".equalsIgnoreCase(protocol))) {
			String msg = localisation.getString("cb_inv_url");
			throw new ApplicationException(msg + ": " + callbackUrl);
		}
		protocol = protocol.toLowerCase();
		int port = uri.getPort();
		if(port < 0) {
			port = protocol.equals("https") ? 443 : 80;
		}

		HttpHost target = new HttpHost(uri.getHost(), port, protocol);

		CredentialsProvider credsProvider = null;
		if(user != null && user.trim().length() > 0 && password != null && password.trim().length() > 0) {
			credsProvider = new BasicCredentialsProvider();
			credsProvider.setCredentials(
					new AuthScope(target.getHostName(), target.getPort()),
					new UsernamePasswordCredentials(user, password));
		}

		HttpClientContext localContext = HttpClientContext.create();
		HttpPost req = new HttpPost(uri);

		// Add body
		MultipartEntityBuilder entityBuilder =  MultipartEntityBuilder.create();
		StringBody sba = new StringBody(payload, ContentType.TEXT_PLAIN);
		entityBuilder.addPart("data", sba);
		req.setEntity(entityBuilder.build());
		log.fine("	Info: Webhook request to: " + req.getURI().toString());

		/*
		 * Close the client and the response.  Each call built its own client and left it
		 * open, which leaks its connection pool, and left the response body unread, which
		 * holds the connection out of that pool as well.
		 */
		try (CloseableHttpClient httpclient = HttpClients.custom()
				.setDefaultCredentialsProvider(credsProvider)
				.setDnsResolver(WEBHOOK_DNS)
				.setDefaultRequestConfig(WEBHOOK_TIMEOUTS)
				.build();
				CloseableHttpResponse response = httpclient.execute(target, req, localContext)) {

			int responseCode = response.getStatusLine().getStatusCode();
			String responseReason = response.getStatusLine().getReasonPhrase();
			log.fine("	Info: Webhook response: " + responseCode + " : " + responseReason);
			if(responseCode != HttpStatus.SC_OK && responseCode != HttpStatus.SC_ACCEPTED && responseCode != HttpStatus.SC_CREATED) {
				/*
				 * Name the endpoint and quote what it said.  A bare "400 : Bad Request" gives
				 * no way to tell which webhook failed, or what it objected to.
				 */
				throw new ApplicationException(responseCode + " : " + responseReason
						+ " from " + callbackUrl + describeBody(response));
			}
		}

	}

	/*
	 * The start of the response body, for the failure message.  Capped because a rejecting
	 * endpoint often answers with a full html error page.
	 */
	private String describeBody(HttpResponse response) {
		try {
			HttpEntity entity = response.getEntity();
			if(entity == null) {
				return "";
			}
			String body = EntityUtils.toString(entity);
			if(body == null || body.trim().length() == 0) {
				return "";
			}
			body = body.trim().replaceAll("\\s+", " ");
			if(body.length() > 500) {
				body = body.substring(0, 500) + "...";
			}
			return " : " + body;
		} catch (Exception e) {
			return "";		// The status code is the useful part, do not lose it to this
		}
	}
}

