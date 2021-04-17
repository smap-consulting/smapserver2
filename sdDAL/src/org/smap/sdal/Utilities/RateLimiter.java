package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.model.RateLimitInfo;

public class RateLimiter {
	
	private static Logger log =
			 Logger.getLogger(RateLimiter.class.getName());
	
	private RateLimiter() {
	}
	
	private static HashMap<Integer, HashMap<String, Long>> store  = new HashMap<> ();
	
	public static RateLimitInfo isPermitted(Connection sd, int oId, String action) {

		RateLimitInfo info =  new RateLimitInfo();
		info.permitted = true;
		
		try {
			info.gap = getGapRequired(sd, oId, action);

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
				
				// Check to see if the last timestamp is less than (the gap) seconds away
				Long now = new Long(System.currentTimeMillis());
				try {
					info.secsElapsed = (int) ((now - ts) / 1000);
				} catch (Exception e) {
					info.secsElapsed = 1000000;	// Some large number so that the request will be accepted				
				}
				if(info.secsElapsed < info.gap) {
					info.permitted = false;
				} else {
					oStore.put(action, now);  // update the time of the last successful usage
				}
				
			}
		
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error checking limit", e);
		}
		return info;
	}
	
	private static int getGapRequired(Connection sd, int o_id, String action) throws SQLException {
		int gap = 0;
		String sql = "select gap from rate_limit where o_id = ? and action = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
			pstmt.setString(2, action);
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
