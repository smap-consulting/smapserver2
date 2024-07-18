package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AdvisoryLock {

	private static Logger log =
			 Logger.getLogger(AdvisoryLock.class.getName());
	
	PreparedStatement pstmtLock;
	PreparedStatement pstmtRelease;
	
	int a;
	int b;
	public AdvisoryLock(Connection sd, int a, int b) throws SQLException {
		this.a = a;
		this.b = b;
		
		log.info("=============== Prepare lock: " + a + " : " + b);
		pstmtLock = sd.prepareStatement("SELECT pg_advisory_lock(?,?)");
		pstmtLock.setInt(1, a);
		pstmtLock.setInt(2, b);
		
		pstmtRelease = sd.prepareStatement("SELECT pg_advisory_unlock_all()");
	}
	
	public void lock(String msg) {
		try {
			log.info("=============== Execute lock: " + msg + " : " + a + " : " + b);
			pstmtLock.execute();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}
	
	public void release(String msg) {
		try {
			log.info("=============== Release lock: " + msg + " : "+ a + " : " + b);
			if(pstmtRelease != null && !pstmtRelease.isClosed()) {
				pstmtRelease.execute();
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}
	
	public void close(String msg) {
		log.info("=============== Close lock: " + msg + " : "+ a + " : " + b);
		try {if (pstmtLock != null) {pstmtLock.close();}} catch (SQLException e) {}
		try {if (pstmtRelease != null) {pstmtRelease.close();}} catch (SQLException e) {}
	}
}
