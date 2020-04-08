package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.InitialContext;
import javax.sql.DataSource;

/*
 * Record usage of restricted resources per organisation
 */
public class UsageLimiter {
	
	private static Logger log =
			 Logger.getLogger(UsageLimiter.class.getName());
	
	private UsageLimiter() {
	}
	
	private static HashMap<Integer, HashMap<String, Integer>> usage = new HashMap<> ();
	
	public static int getUsage(Integer oId, String resource) {
		int usage = 0;
		HashMap<String, Integer> orgUsage = usage.get(oId);
		return usage;
	}
	
	public static void closeConnection(String requester, Connection c) {

		if (c != null) try { 
			c.setAutoCommit(true);
			c.close(); 
			count--;
			log.info(" $$$$ " + count + " Close SurveyDefinitions connection: " + requester);
		} catch(SQLException e) {
			log.log(Level.SEVERE,"Failed to close surveyDefinitions connection", e);
		}
	}
	

}
