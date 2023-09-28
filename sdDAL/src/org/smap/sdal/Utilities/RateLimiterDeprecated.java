package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.model.RateLimitInfo;

public class RateLimiterDeprecated {
	
	private static Logger log =
			 Logger.getLogger(RateLimiterDeprecated.class.getName());
	
	private RateLimiterDeprecated() {
	}
	
	private static HashMap<Integer, HashMap<String, Long>> store  = new HashMap<> ();
	
	public static RateLimitInfo isPermitted(Connection sd, int oId, String action) {

		RateLimitInfo info =  new RateLimitInfo();
		info.permitted = true;
		
		try {
			info.gap = getGapRequired(sd, oId);

			if(info.gap > 0) {
				// Get store of requests for this organisation
				HashMap<String, Long> oStore = store.get(oId);
				if(oStore == null) {
					oStore = new HashMap<>();
					store.put(oId, oStore);
				}
				
				// Get the last timestamp for this action
				Long ts = oStore.get(action);
				if(ts == null) {
					ts = new Long(0);
					oStore.put(action, ts);
				}
				
				// Check to see if the last timestamp is less than (the gap) milli seconds away
				Long now = new Long(System.currentTimeMillis());
				try {
					info.milliSecsElapsed = now - ts;
				} catch (Exception e) {
					info.milliSecsElapsed = Integer.MAX_VALUE;	// Some large number so that the request will be accepted				
				}
				if(info.milliSecsElapsed < info.gap) {
					info.permitted = false;
					log.info("Rate limit exceeded: " + oId + " " + action);
				} else {
					oStore.put(action, now);  // update the time of the last successful usage
				}
				
			}
		
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error checking limit", e);
		}
		return info;
	}
	
	private static int getGapRequired(Connection sd, int o_id) throws SQLException {
		int gap = 0;
		String sql = "select api_rate_limit from organisation where id = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				gap = rs.getInt(1);
			}
		} finally {
			if (pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
		
		return gap;
	}

}
