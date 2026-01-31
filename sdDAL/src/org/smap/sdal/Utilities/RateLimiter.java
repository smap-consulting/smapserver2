package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

public class RateLimiter {

	private static Logger log = Logger.getLogger(RateLimiter.class.getName());

	private RateLimiter() {
	}

	/*
	 * Create one bucket per organisation
	 * A shared bucket will also be created per module
	 * Hence all the rate limited services in a module for an organisation will get a
	 * single total limit
	 *
	 * Return the record limit for the server
	 */
	private static final ConcurrentHashMap<Integer, Bucket> store = new ConcurrentHashMap<>();

	private static volatile int rate = -1;
	private static volatile int limit = 0;
	private static volatile long lastConfigLoad = 0;
	private static final long CONFIG_RELOAD_INTERVAL_MS = 60000; // Reload config every 60 seconds
	private static final Object configLock = new Object();

	public static int isPermitted(Connection sd, int oId,
			HttpServletResponse response) throws ApplicationException {

		try {
			// Load config on first call or if config is stale
			long now = System.currentTimeMillis();
			if(rate == -1 || (now - lastConfigLoad) > CONFIG_RELOAD_INTERVAL_MS) {
				loadConfig(sd, now);
			}

			if(rate > 0) {
				Bucket bucket = store.computeIfAbsent(oId, k ->
					Bucket.builder()
					    .addLimit(Bandwidth.classic(rate, Refill.greedy(rate, Duration.ofMinutes(1))))
					    .build()
				);

				if (!bucket.tryConsume(1)) {
			        // limit is exceeded
					String msg = "Rate exceeded. Access to this service is rate limited to " + rate + " requests per minute.";
					throw new ApplicationException(msg);
				}
			}
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Error checking rate limit", e);
			throw new ApplicationException("Error checking rate limit: " + e.getMessage());
		}

		return limit;
	}

	private static void loadConfig(Connection sd, long now) throws SQLException {
		synchronized(configLock) {
			// Double-check after acquiring lock - avoid reload if another thread just loaded
			if(rate != -1 && (now - lastConfigLoad) <= CONFIG_RELOAD_INTERVAL_MS) {
				return;
			}

			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try {
				pstmt = sd.prepareStatement("select max_rate, api_max_records from server");
				rs = pstmt.executeQuery();
				if(rs.next()) {
					rate = rs.getInt("max_rate");
					limit = rs.getInt("api_max_records");
					lastConfigLoad = now;
					log.info("Rate limit config loaded: rate=" + rate + ", limit=" + limit);
				}
			} finally {
				try {if (rs != null) {rs.close();}} catch (SQLException e) {}
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			}
		}
	}

	/**
	 * Force config reload on next call and clear all buckets
	 */
	public static void reset() {
		synchronized(configLock) {
			log.info("Rate limiter reset - forcing reload and clearing all buckets");
			lastConfigLoad = 0; // Force reload on next call
			store.clear();
		}
	}

}
