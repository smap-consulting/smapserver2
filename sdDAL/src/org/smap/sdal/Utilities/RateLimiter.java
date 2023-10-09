package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

public class RateLimiter {
	
	private static Logger log =
			 Logger.getLogger(RateLimiter.class.getName());
	
	private RateLimiter() {
	}
	
	/*
	 * Create one bucket per organisation
	 * A shared bucket will also be created per module
	 * Hence all the rate limited services in a module for an organisation will get a 
	 * single total limit
	 */
	private static HashMap<Integer, Bucket> store  = new HashMap<> ();
	
	private static int rate = -1;
	
	public static void isPermitted(Connection sd, int oId, 
			HttpServletResponse response,
			ResourceBundle localisation) throws ApplicationException {
		
		PreparedStatement pstmt = null;
		
		try {
			/*
			 * Get the rate from the database if we don't already have it
			 */
			if(rate == -1) {
				pstmt = sd.prepareStatement("select max_rate from server");
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()) {
					rate = rs.getInt("max_rate");
				}
			}
			
			if(rate > 0) {
				Bucket bucket = store.get(oId);
				if(bucket == null) {
					bucket = Bucket.builder()
					    .addLimit(Bandwidth.classic(rate, Refill.greedy(rate, Duration.ofMinutes(1))))
					    .build();	// TODO add a burst limit for 1 second
					store.put(oId,  bucket);
				}
				
				if (!bucket.tryConsume(1)) {
			        // limit is exceeded
					String msg = localisation.getString("rl_api");
					msg = msg.replace("%s1", String.valueOf(rate));
					throw new ApplicationException(msg);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}
		
		return;
	
	}
	
	public static void setRates(int r) {
		rate = r;
		store = new HashMap<> ();	// reset the cache
	}

}
