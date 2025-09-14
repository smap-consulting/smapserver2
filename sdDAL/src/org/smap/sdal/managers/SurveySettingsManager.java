package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.model.ChartDefn;
import org.smap.sdal.model.ConsoleColumn;
import org.smap.sdal.model.MapLayer;
import org.smap.sdal.model.SurveySettingsDefn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
 * Manage survey settings
 */
public class SurveySettingsManager {
	
	private static Logger log =
			 Logger.getLogger(SurveySettingsManager.class.getName());
	
	private ResourceBundle localisation = null;
	String tz;
	
	private Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	
	public SurveySettingsManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	/*
	 * Get the Managed Form Configuration
	 */
	public SurveySettingsDefn getSurveySettings(
			Connection sd, 
			int uId,
			String sIdent) throws SQLException, Exception  {
		
		SurveySettingsDefn ssd = null;
		
		// SQL to get settings details
		String sql = "select view, map_view, chart_view, columns "
				+ "from survey_settings "
				+ "where u_id = ? "
				+ "and s_ident = ? ";
		PreparedStatement pstmt = null;
		
		ResultSet rs = null;
		try {

			// Get the survey view
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, uId);
			pstmt.setString(2, sIdent);
			log.info("Get settings defn: " + pstmt.toString());
			
			rs = pstmt.executeQuery();
			if(rs.next()) {
				String sView = rs.getString(1);
				String sMapView = rs.getString(2);
				String sChartView = rs.getString(3);
				String sColumns = rs.getString(4);
				
				if(sView != null) {
					try {
						ssd = gson.fromJson(sView, SurveySettingsDefn.class);
					} catch (Exception e) {
						log.log(Level.SEVERE,"Error: ", e);
					}
				} 
				
				if(ssd == null) {
					ssd = new SurveySettingsDefn();
				}
				
				if(sMapView != null) {
					Type type = new TypeToken<ArrayList<MapLayer>>(){}.getType();	
					try {
						ssd.layers = gson.fromJson(sMapView, type);
					} catch (Exception e) {
						log.log(Level.SEVERE,"Error: ", e);
						ssd.layers = new ArrayList<MapLayer> ();		// If there is an error its likely that the structure of the config file has been changed and we should start from scratch
					}
				} else {
					ssd.layers = new ArrayList<MapLayer> ();
				}
				
				if(sChartView != null) {
					Type type = new TypeToken<ArrayList<ChartDefn>>(){}.getType();	
					try {
						ssd.charts = gson.fromJson(sChartView, type);
					} catch (Exception e) {
						log.log(Level.SEVERE,"Error: ", e);
						ssd.charts = new ArrayList<ChartDefn> ();		// If there is an error its likely that the structure of the config file has been changed and we should start from scratch
					}
				} else {
					ssd.charts = new ArrayList<ChartDefn> ();
				}
				
				if(sColumns != null) {
					Type type = new TypeToken<HashMap<String, ConsoleColumn>>(){}.getType();	
					try {
						ssd.columnSettings = gson.fromJson(sColumns, type);
					} catch (Exception e) {
						log.log(Level.SEVERE,"Error: ", e);
						ssd.columnSettings = new HashMap<String, ConsoleColumn> ();		
					}
				} else {
					ssd.columnSettings = new HashMap<String, ConsoleColumn> ();	
				}
				
			}
				
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}
		
		if(ssd == null) {
			ssd = new SurveySettingsDefn();
		}
		
		return ssd;
	}
	
	/*
	 * Set the Managed Form Configuration
	 */
	public void setSurveySettings(
			Connection sd, 
			int uId,
			String sIdent,
			SurveySettingsDefn ssd) throws SQLException {
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;
		try {
				
			String sql = "update survey_settings set view = ? "
						+ "where u_id = ? "
						+ "and s_ident = ? ";
			pstmt = sd.prepareStatement(sql);
			
			String sqlInsert = "insert into survey_settings (u_id, s_ident, view) "
					+ "values (?, ?, ?) ";
			pstmt = sd.prepareStatement(sql);
			
			pstmt.setString(1, gson.toJson(ssd));
			pstmt.setInt(2, uId);
			pstmt.setString(3, sIdent);
			log.info(pstmt.toString());
			int count = pstmt.executeUpdate();	
			
			if(count == 0) {
				// Insert a record
				pstmtInsert = sd.prepareStatement(sqlInsert);
				pstmtInsert.setInt(1, uId);
				pstmtInsert.setString(2, sIdent);
				pstmtInsert.setString(3, gson.toJson(ssd));
				log.info(pstmtInsert.toString());
				pstmtInsert.executeUpdate();	
			}
			
				
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtInsert != null) {pstmtInsert.close();}} catch (SQLException e) {}
			
		}
	}
	
	public void updateConsoleSettings(Connection sd, int uId, String sIdent, String oversightIdent, int pageLen, String colOrder) {
		
		PreparedStatement pstmt = null;
		String colOrderKey = sIdent + (oversightIdent == null ? "" : oversightIdent);
		String ssdColOrder = null;
		
		try {
		
			sd.setAutoCommit(false);
			String sql = "select view from survey_settings "
					+ "where u_id = ? "
					+ "and s_ident = ? ";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, uId);
			pstmt.setString(2, sIdent);
			ResultSet rs = pstmt.executeQuery();
			
			SurveySettingsDefn ssd = null;
			
			if(rs.next()) {
				
				String sView = rs.getString("view");
				if(sView != null) {
					ssd = gson.fromJson(sView, SurveySettingsDefn.class);
				}
				
			}
			if(ssd == null) {
				ssd = new SurveySettingsDefn();
			}
			if(pageLen > 0) { 
				ssd.pageLen = pageLen;
			} else if(ssd.pageLen <= 0) {
				ssd.pageLen = 10;
			}
			
			if(colOrder != null) {
				ssdColOrder = ssd.columnOrders.get(colOrderKey);
				
				if(ssdColOrder == null) {
					ssdColOrder = colOrder;
				} else {
					/*
					 * reorder column order
					 * New size should match the passed in column order
					 */
					String [] colArray = colOrder.split(",");
					String [] oldColArray = ssdColOrder.split(",");
					boolean badArray = false;
					if(colArray.length == oldColArray.length) {
						String [] newColArray = colOrder.split(",");		
						for(int idx = 0; idx < colArray.length; idx++) {
							int colIndex = Integer.valueOf(colArray[idx]);
							if(colIndex > newColArray.length) {
								badArray = true;
								break;
							} else {
								newColArray[idx] = oldColArray[colIndex];
							}
							
						}
						ssdColOrder = String.join(",", newColArray);
					} else {
						ssdColOrder = null;		// Length has changed so reset
					}
					if(badArray) {
						ssdColOrder = null;
					}
				}
			} 
			
			log.info("xoxoxoxox: Setting colOrder: " + colOrderKey + " : " + ssdColOrder);
			ssd.columnOrders.put(colOrderKey, ssdColOrder);
			setSurveySettings(sd, uId, sIdent, ssd);
			
			sd.commit();
			
		} catch (Exception e) {
			try {sd.rollback();}catch(Exception ex) {};
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			try {sd.setAutoCommit(true);}catch(Exception e) {};
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}}
		}
	}
}


