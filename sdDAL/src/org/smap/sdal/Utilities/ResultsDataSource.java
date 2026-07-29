package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class ResultsDataSource {

	private static Logger log =
			 Logger.getLogger(ResultsDataSource.class.getName());


	private ResultsDataSource() {
	}

	private static final AtomicInteger count = new AtomicInteger();

	private static volatile DataSource dataSource;

	/*
	 * The pool object lives as long as the webapp, so look it up once instead of on
	 * every connection.  Only a successful lookup is cached, so a call made before
	 * the resource is bound does not poison it.  Two threads racing here both get
	 * the same pool from JNDI, so the race is harmless.
	 */
	private static DataSource getDataSource() throws NamingException {
		DataSource ds = dataSource;
		if (ds == null) {
			ds = (DataSource) new InitialContext().lookup("java:/comp/env/jdbc/results");
			dataSource = ds;
		}
		return ds;
	}

	public static Connection getConnection(String requester) {

		try {
			Connection c = getDataSource().getConnection();
			c.setAutoCommit(true);		// Can't rely on auto commit being set to true when connection comes from pool
			int open = count.incrementAndGet();
			log.fine(" ++++ " + open + " Create Results connection: " + requester);
			return c;
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error getting results datasource", e);
			return null;
		}
	}
	
	public static void closeConnection(String requester, Connection c) {

		// Note: don't rely on isClosed() - it does not detect a connection the
		// server has already dropped (eg after an idle timeout). Just close() it,
		// which is safe even when the connection is already dead.
		if (c != null) try {
			c.close();
			int open = count.decrementAndGet();
			log.fine(" ---- " + open + " Close Results connection: " + requester);
		} catch(SQLException e) {
			// Likely a stale/already-dead pooled connection - not severe
			int open = count.decrementAndGet();
			log.fine(" ---- " + open + " Failed to close results connection (likely already dropped): " + requester);
		}
	}
}
