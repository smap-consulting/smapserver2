package org.smap.sdal.managers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

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
	ResourceBundle localisation;
	
	private final String CUSTOM_FILE = "custom.css";
	private final String SERVER_FOLDER = "server";
	private final String ORG_FOLDER = "org";

	public CssManager(String basePath, ResourceBundle l) {
		this.basePath = basePath;
		localisation = l;
	}
	
	public void setCurrentCssFile(String name, int orgId) throws IOException {
		if(name == null || name.equals("_none")) {
			removeCurrentCustomCssFile(orgId);
		} else {
			replaceCustomCssFile(name, orgId);
		}
	}
	
	public File getCssLoadedFolder(int orgId) throws IOException {
		
		String folderPath = basePath + File.separator + "css" + File.separator;
		
		if(orgId > 0) {
			folderPath += orgId + File.separator + ORG_FOLDER;
		} else {
			folderPath += SERVER_FOLDER;
		}
		File folder = new File(folderPath);
		FileUtils.forceMkdir(folder);
		
		return folder;
	}
	
	public File getCssFolder() throws IOException {
		// Make sure the folder exists
		String folderPath = basePath + File.separator + "css";
		File folder = new File(folderPath);
		FileUtils.forceMkdir(folder);
		
		return folder;
	}
	
	private void removeCurrentCustomCssFile(int orgId) throws IOException {
		File cssFolder = getCssFolder();
		File f;
		if(orgId > 0) {
			f = new File(cssFolder.getAbsolutePath() + File.separator + orgId + File.separator + CUSTOM_FILE);
		} else {
			f = new File(cssFolder.getAbsolutePath() + File.separator + CUSTOM_FILE);
		}
		f.delete();
		f.createNewFile();	// Create an empty file
	}
	
	private void replaceCustomCssFile(String name, int orgId) throws IOException {
		File cssFolder = getCssFolder();
		File loadedFolder = getCssLoadedFolder(orgId);
		File source = new File(loadedFolder.getAbsolutePath() + File.separator + name);
		File target;
		if(orgId > 0) {
			target = new File(cssFolder.getAbsolutePath() + File.separator + orgId + File.separator + CUSTOM_FILE);
		} else {
			target = new File(cssFolder.getAbsolutePath() + File.separator + CUSTOM_FILE);
		}
		Files.copy(Paths.get(source.getAbsolutePath()), Paths.get(target.getPath()), StandardCopyOption.REPLACE_EXISTING);		
	}
	
	public void deleteCustomCssFile(Connection sd, String user, String name, int orgId) throws IOException {
		File loadedFolder = getCssLoadedFolder(orgId);
		File f = new File(loadedFolder.getAbsolutePath() + File.separator + name);
		log.info("Deleting css file: " + f.getAbsolutePath());
		String msg = localisation.getString("c_del_css") + " " + f.getAbsolutePath();
		lm.writeLogOrganisation(sd, orgId, user, LogManager.DELETE, msg, 0);
		f.delete();
		
	}
}


