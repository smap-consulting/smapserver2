package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;

import org.smap.sdal.model.SmapTimeZone;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
 * This class supports access to User and Organisation information in the database
 */
public class TimeZoneManager {

	private static Logger log =
			Logger.getLogger(TimeZoneManager.class.getName());
	
	public Response get(Connection sd) {
		Response response = null;
		
		
		String sql = "select name, utc_offset from timezone "
				+ "order by utc_offset asc";
		PreparedStatement pstmt = null;
		
		try {
			ArrayList<SmapTimeZone> timezones = new ArrayList<> ();
			pstmt = sd.prepareStatement(sql);
			log.info("Get timezones: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String id = rs.getString(1);
				String offset = rs.getString(2);
				offset = offset.substring(0, offset.lastIndexOf(":"));
				timezones.add(new SmapTimeZone(id, id + " (" + offset + ")"));
			}
		    
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(timezones);
			response = Response.ok(resp).build();
		} catch (Exception e) {
			
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);

		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}};
		}

		return response;
	}
	
	/*
	 * Refresh the cached timezone list
	 * This has been added as extracting data from pg_timezones is slow
	 */
	public void refresh(Connection sd) {
		
		String sqlClear = "truncate timezone";
		String sql = "insert into timezone select name, utc_offset from pg_timezone_names "
				+ "where substring(name FROM 1 FOR 3) <> 'Etc' "
				+ "and substring(name FROM 1 FOR 7) <> 'SystemV' "
				+ "and substring(name FROM 1 FOR 5) <> 'posix' "
				+ "and name <> 'GMT0' "
				+ "and name <> 'GMT-0' "
				+ "and name <> 'GMT+0' "
				+ "and name <> 'UCT' "
				+ "order by utc_offset asc";
		PreparedStatement pstmt = null;
		
		try {
			
			// Clear the old timezone values
			pstmt = sd.prepareStatement(sqlClear);
			pstmt.executeUpdate();
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}};
			
			pstmt = sd.prepareStatement(sql);
			log.info("Refresh timezones: " + pstmt.toString());
			pstmt.executeUpdate();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		} finally {		
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}};
		}
	}
}
