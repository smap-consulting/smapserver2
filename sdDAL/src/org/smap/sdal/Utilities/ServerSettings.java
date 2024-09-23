package org.smap.sdal.Utilities;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

public class ServerSettings {
	
	private static Logger log =
			 Logger.getLogger(ServerSettings.class.getName());
	
	private static String basePath = null;
	private static String region = null;
	private static boolean regionSet = false;
	
	private ServerSettings() {
	}
	
	public static void setBasePath(HttpServletRequest request) {
		if(basePath == null) {
			basePath = GeneralUtilityMethods.getBasePath(request);
		}
	}
	public static void setBasePath(String bp) {
		if(basePath == null) {
			basePath = bp;
		}
	}
	
	public static String getBasePath() throws ApplicationException {
		if(basePath == null) {
			log.log(Level.SEVERE, "Base path is null", new ApplicationException("null base path"));
			throw new ApplicationException("Base Path is null when trying to get basePath without request details");
		} 
		return basePath;
	}
	
	public static String getRegion() throws ApplicationException {
		
		if(!regionSet) {
			if(basePath == null) {	
				throw new ApplicationException("Base Path is null when trying to get region");
			}
			region = GeneralUtilityMethods.getSettingFromFile(basePath + "/settings/region");
			regionSet = true;
		}		
	
		return region;
	
	}

}
