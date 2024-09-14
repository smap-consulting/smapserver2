package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import org.smap.sdal.managers.LogManager;

public class LogonLimiter {
	
	private static Logger log =
			 Logger.getLogger(LogonLimiter.class.getName());
	
	private LogonLimiter() {
	}
	
	/*
	 * Create a hashmap of ip addresses and users
	 */
	private static HashMap<String, String> logons  = new HashMap<> ();
	
	
	public static void isPermitted(HttpServletRequest request, Connection sd, LogManager lm) throws ApplicationException, SQLException {
		
		if(request.getServerName().contains("fkfkjsdfkjsdjfjksj")) {	// TODO Disabled
			String user = request.getRemoteUser();
			String address = request.getHeader("X-Forwarded-For");
			if(address == null) {
				address = request.getRemoteAddr();
			}
			if(address == null) {
				address = "none";
			}
			
			String lastAddress = logons.get(user);	// Get the last known address of the user
			if(lastAddress == null) {
				logons.put(user, address);
			} else if(!address.equals(lastAddress)) {
				String msg = "User %s1 attempting to logon from more than one device: ";
				msg = msg.replace("%s1", user);
				msg = msg + lastAddress + "," + address;
				int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
				lm.writeLogOrganisation(sd, oId, request.getRemoteUser(), LogManager.SECURITY, msg, 0);

				logons.remove(user);
				throw new ApplicationException("multilogon");
			}
		}
	}
	
}
