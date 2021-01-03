package org.smap.sdal.managers;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.AuthenticationFailedException;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.Message.RecipientType;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.io.FileUtils;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SubscriptionStatus;

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
 * Manage Css Custom styling
 */
public class CssManager {

	private static Logger log =
			Logger.getLogger(CssManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	String basePath;

	public CssManager(String basePath) {
		this.basePath = basePath;
	}
	
	public void setServerCssFile(String name) throws IOException {
		if(name != null) {
			if(name.equals("_none")) {
				removeCustomCssFile();
			} else {
				replaceCustomCssFile(name);
			}
		}
	}
	
	public File getCssServerFolder() throws IOException {
		// Make sure the folder exists
		String folderPath = basePath + File.separator + "css";
		File folder = new File(folderPath);
		FileUtils.forceMkdir(folder);
		
		return folder;
	}
	
	private void removeCustomCssFile() throws IOException {
		File f = getCssServerFolder();
	}
	
	private void replaceCustomCssFile(String name) {
		
	}
}


