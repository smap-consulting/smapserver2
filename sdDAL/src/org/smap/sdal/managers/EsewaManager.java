package org.smap.sdal.managers;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

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
public class EsewaManager {

	private static Logger log =
			Logger.getLogger(EsewaManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	String hostname;
	String paymentsHost;
	String user;
	String password;

	String protocol;
	int port;
	
	CloseableHttpClient httpclient = null;
	HttpHost target = null;
	CredentialsProvider credsProvider = null;
	
	boolean test;

	public EsewaManager(boolean test, String hostname) {
		this.test = test;
		this.hostname = hostname;
		this.paymentsHost = getPaymentsHost();
		this.user = "9806800001/2/3/4/5";
		this.password = "Nepal@123";
		

		protocol = "https";
		port = 443;

		target = new HttpHost(hostname, port, protocol);

		credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(
				new AuthScope(hostname, port),
				new UsernamePasswordCredentials(user, password));	
	}
	
	public void pay(String instanceId) {
		log.info("Paying eSewa........");
		
		HttpResponse response = null;
		
		try {
			URL url = new URL("https://" + getPaymentsHost() + "/epay/main"); 
			
			
			httpclient = HttpClients.custom()
					.setDefaultCredentialsProvider(credsProvider)
					.build();
	
			HttpClientContext localContext = HttpClientContext.create();
			HttpPost req = new HttpPost(URI.create(url.toString()));
			

	        req.setEntity(new UrlEncodedFormEntity(getPayParamValues(instanceId)));
	
	        log.info("Calling esewa: " + url);
	        
			response = httpclient.execute(target, req, localContext);
			if(response != null) {
				log.info("Response Code: " + response.getStatusLine().getStatusCode());
			} else {
				log.info("Response is null");
			}
			
		} catch (Exception e) {	
			e.printStackTrace();
		} finally {
			try{httpclient.close();} catch(Exception e) {e.printStackTrace();}
		}
	}
	
	private ArrayList<BasicNameValuePair> getPayParamValues(String instanceId) {
		
		ArrayList<BasicNameValuePair> nameValuePairs = new ArrayList<BasicNameValuePair> ();
		
        nameValuePairs.add(new BasicNameValuePair("tAmt", "111"));		// TODO get amount from record   
        nameValuePairs.add(new BasicNameValuePair("amt", "100"));		// TODO get amount from record   
        nameValuePairs.add(new BasicNameValuePair("txAmt", "5"));		// TODO get amount from record   
        nameValuePairs.add(new BasicNameValuePair("psc", "2"));		// TODO get amount from record  
        nameValuePairs.add(new BasicNameValuePair("pdc", "4"));		// TODO get amount from record 
        nameValuePairs.add(new BasicNameValuePair("scd", "EPAYTEST"));		// TODO get amount from record 
        nameValuePairs.add(new BasicNameValuePair("pid", instanceId));		// Product Id
        nameValuePairs.add(new BasicNameValuePair("su", "https://" + hostname + "/esewa/success?q=su"));
        nameValuePairs.add(new BasicNameValuePair("su", "https://" + hostname + "/esewa/failed?q=fu"));
        
        return nameValuePairs;
	}
	
	public String getPaymentsHost() {
		return (test ? "uat." : "") + "esewa.com.np";
	}
	
}


