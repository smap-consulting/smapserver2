package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
 * Manage inbound SMS message
 */
public class SMSInboundManager {
	
	private static Logger log =
			 Logger.getLogger(SMSInboundManager.class.getName());
	
	/*
	 * Write a log entry that includes the survey id
	 */
	public void saveMessage(
			Connection sd,
			String fromNumber,
			String toNumber,
			String msg)  {
		
		String sql = "insert into sms_event ("
				+ "time_inserted,"
				+ "from_number,"
				+ "msg) "
				+ "values (now(), ?, ?);";

		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, fromNumber);
			pstmt.setString(2, toNumber);
			pstmt.setString(3, msg);
			
			log.info("----- new sms " + pstmt.toString());
			pstmt.executeUpdate();
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	

}


