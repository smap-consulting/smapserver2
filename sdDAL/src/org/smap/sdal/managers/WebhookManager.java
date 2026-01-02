package org.smap.sdal.managers;

import java.net.URI;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
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
	
	public WebhookManager(ResourceBundle l) {
		localisation = l;
	}
	
	// Send an email
	public void callRemoteUrl(String callbackUrl, String payload, String user, String password) throws Exception  {

		String host_name = null;
		String protocol = null;
		int port = 0;
		int endOfProtocol = 0;

		if(callbackUrl.startsWith("https://")) {
			endOfProtocol = 8;
			port = 443;
			protocol = "https";
		} else if(callbackUrl.startsWith("http://")) {
			endOfProtocol = 7;
			port = 80;
			protocol = "http";
		} else {
			String msg = localisation.getString("cb_inv_url");
			throw new ApplicationException(msg + ": " + callbackUrl);
		}
		int endOfHost = callbackUrl.indexOf('/', endOfProtocol);
		if(endOfHost > 0) {
			host_name = callbackUrl.substring(endOfProtocol, endOfHost);
		} else {
			host_name = callbackUrl.substring(endOfProtocol);
		}

		HttpHost target = new HttpHost(host_name, port, protocol);

		CredentialsProvider credsProvider = null;
		if(user != null && user.trim().length() > 0 && password != null && password.trim().length() > 0) {
			credsProvider = new BasicCredentialsProvider();
			credsProvider.setCredentials(
					new AuthScope(target.getHostName(), target.getPort()),
					new UsernamePasswordCredentials(user, password));
		}

		CloseableHttpClient httpclient = HttpClients.custom()
				.setDefaultCredentialsProvider(credsProvider)
				.build();

		HttpClientContext localContext = HttpClientContext.create();
		HttpPost req = new HttpPost(URI.create(callbackUrl));

		// Add body
		MultipartEntityBuilder entityBuilder =  MultipartEntityBuilder.create();
		StringBody sba = new StringBody(payload, ContentType.TEXT_PLAIN);
		entityBuilder.addPart("data", sba);
		req.setEntity(entityBuilder.build());
		log.info("	Info: Webhook request to: " + req.getURI().toString());
		log.info("	Info: Webhook content: " + payload);
		HttpResponse response = httpclient.execute(target, req, localContext);
		int responseCode = response.getStatusLine().getStatusCode();
		String responseReason = response.getStatusLine().getReasonPhrase(); 
		log.info("	Info: Webhook response: " + responseCode + " : " + responseReason);
		if(responseCode != HttpStatus.SC_OK && responseCode != HttpStatus.SC_ACCEPTED && responseCode != HttpStatus.SC_CREATED) {
			throw new ApplicationException(responseCode + " : " + responseReason);
		}

	}
}

