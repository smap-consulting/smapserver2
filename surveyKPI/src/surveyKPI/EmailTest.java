package surveyKPI;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.Properties;

import javax.mail.Session;
import javax.mail.Store;
import javax.mail.internet.InternetAddress;

/*
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

*/

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import org.smap.notifications.interfaces.EmitAwsSES;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.Gmail.Users;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.ListLabelsResponse;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

@Path("/emailTest")
public class EmailTest extends Application {
	
	Authorise a = null;
	
	public EmailTest() {
		a = new Authorise(null, Authorise.OWNER);
	}
	
	/*
	 * Send a test email
	 */
	@GET
	@Path("/ses")
	public String emailTestSES(@Context HttpServletRequest request) throws Exception {
		
		EmitAwsSES mgr = new EmitAwsSES("ap-southeast-2", 
				GeneralUtilityMethods.getBasePath(request));
		InternetAddress[] recipients = InternetAddress.parse("neilpenman@gmail.com");
		mgr.sendSES(recipients, "Hello There", "1", "Email Body", null, null);

		return "done";
	}

	/*
	 * Send a test email
	 */
	@GET
	@Path("/gmail")
	public String emailTestGmail(@Context HttpServletRequest request) throws Exception {
		JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
		NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		 
		String basePath = GeneralUtilityMethods.getBasePath(request);
		String keyPath = basePath + "_bin/resources/properties/google.json";
		
		// Delegate domain wide authority
		GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream(keyPath))
			    .createScoped(Collections.singleton(GmailScopes.GMAIL_MODIFY))
			    .createDelegated("app@smap.com.au");
		HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credential);
		
		Gmail service = new Gmail.Builder(HTTP_TRANSPORT, JSON_FACTORY, requestInitializer).setApplicationName("app mail").build();
		
		String user = "me";
	    ListLabelsResponse listResponse = service.users().labels().list(user).execute();
			    
		return "done";
	}

}

