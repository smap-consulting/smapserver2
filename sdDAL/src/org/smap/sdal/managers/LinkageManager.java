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

package org.smap.sdal.managers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Link;
import org.smap.sdal.model.LinkageItem;
import org.smap.sdal.model.LinkedTarget;

import com.machinezoo.sourceafis.FingerprintImage;
import com.machinezoo.sourceafis.FingerprintImageOptions;
import com.machinezoo.sourceafis.FingerprintTemplate;

public class LinkageManager {
	
	private static Logger log =
			 Logger.getLogger(LinkageManager.class.getName());

	private final String REQUEST_FP_IMAGE = "ex:uk.ac.lshtm.keppel.android.SCAN(type='image')";
	private final String REQUEST_FP_ISO_TEMPLATE = "ex:uk.ac.lshtm.keppel.android.SCAN(type='iso')";
	
	private ResourceBundle localisation;
	
	public LinkageManager(ResourceBundle localisation) {
		localisation = this.localisation;
	}
	
	public ArrayList<Link> getSurveyLinks(Connection sd, Connection cRel, int sId, int fId, int prikey) throws SQLException {
		ArrayList<Link> links = new ArrayList<Link> ();
		
		ResultSet rs = null;
		ResultSet rsHrk = null;
		
		// SQL to get default settings for child forms
		String sql = "select f_id from form where parentform = ? and s_id = ?";
		PreparedStatement pstmt = null;
		
		// SQL to get linked forms
		String sqlLinked = "select linked_target, f_id, column_name from question "
				+ "where linked_target not null "
				+ "and f_id = ? "
				+ "and f_id in (select f_id from form where s_id = ?)";
		PreparedStatement pstmtLinked = null;
		
		PreparedStatement pstmtGetHrk = null;
		
		try {

			// If the passed in form id was 0 then use the top level form
			if(fId == 0) {
				Form f = GeneralUtilityMethods.getTopLevelForm(sd, sId);
				fId = f.id;
			}
			
			// Get the child forms
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, fId);
			pstmt.setInt(2, sId);
			log.info("Links:  Getting child forms: " + pstmt.toString());
			rs = pstmt.executeQuery();
			while(rs.next()) {
				Link l = new Link();
				l.type = "child";
				l.fId = rs.getInt(1);
				l.parkey = prikey;
				
				links.add(l);
			}
			rs.close();
			
			// Get the linked forms
			pstmtLinked = sd.prepareStatement(sqlLinked);
			pstmtLinked.setInt(1, fId);
			pstmtLinked.setInt(2, sId);
			log.info("Links:  Getting linked forms: " + pstmtLinked.toString());
			rs = pstmtLinked.executeQuery();
			while(rs.next()) {
				String linkedTarget = rs.getString(1);
				int valueFId = rs.getInt(2);
				String valueColName = rs.getString(3);
				
				LinkedTarget lt = GeneralUtilityMethods.getLinkTargetObject(linkedTarget);
				String hrk = null;
				Form valueForm = GeneralUtilityMethods.getForm(sd, sId, valueFId);
				
				Form f = GeneralUtilityMethods.getFormWithQuestion(sd, lt.qId);
	
				// SQL to get the HRK value
				String sqlGetHrk = "select " + valueColName + " from " + valueForm.tableName + " where prikey = ?;";
				pstmtGetHrk = cRel.prepareStatement(sqlGetHrk);
				pstmtGetHrk.setInt(1,prikey);
				log.info("Getting Hrk: " + pstmtGetHrk.toString());
				rsHrk = pstmtGetHrk.executeQuery();
				if(rsHrk.next()) {
					hrk = rsHrk.getString(1);
				}
				
				Link l = new Link();
				l.type = "link";
				l.fId = f.id;
				l.sId = lt.sId;
				l.qId = lt.qId;
				l.hrk = hrk;
				
				links.add(l);
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			try {if (pstmtLinked != null) {pstmtLinked.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetHrk != null) {pstmtGetHrk.close();	}} catch (SQLException e) {	}
		}

		return links;
	}
	
	/*
	 * Add a data item to a linkage list, these can be
	 *  Fingerprints
	 *  ....
	 */
	public void addDataitemToList(ArrayList<LinkageItem> links, String value, String appearance, ArrayList<KeyValueSimp> params, String sIdent, String colName) {
		
		LinkageItem item = null;
		
		if(links != null && value != null && value.trim().length() > 0  && appearance != null) {
			if(appearance.contains(REQUEST_FP_IMAGE) || appearance.contains(REQUEST_FP_ISO_TEMPLATE)) {
				// Fingerprint
				item = new LinkageItem();
				
				// Set the value
				if(appearance.contains(REQUEST_FP_IMAGE)) {
					item.fp_image = value;
				} else {
					item.fp_iso_template = value;
				}
				
				if(params != null) {
					for(KeyValueSimp p : params) {
						if(p.k.equals("fp_location")) {
							item.fp_location  = p.v;
						} else if(p.k.equals("fp_side")) {
							item.fp_side = p.v;
						} else if(p.k.equals("fp_digit")) {
							try {
								item.fp_digit = Integer.parseInt(p.v);
							} catch (Exception e) {
								
							}
						}
					}
				}
				item.validateFingerprint();
			} 
		}
		
		if(item != null) {
			item.sIdent = sIdent;
			item.colName = colName;
			links.add(item);
		}
	}
	
	/*
	 * Write the linkage items to the table
	 */
	public void writeItems(Connection sd, int oId, String changedBy, String instanceId, ArrayList<LinkageItem> items) throws SQLException {
		
		String sql = "insert into linkage (o_id, instance_id, survey_ident, col_name, fp_location, fp_side, fp_digit, fp_image, fp_iso_template, changed_by, changed_ts) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, instanceId);
			pstmt.setString(10, changedBy);
			for(LinkageItem item : items) {
				pstmt.setString(3, item.sIdent);
				pstmt.setString(4, item.colName);
				pstmt.setString(5,  item.fp_location);
				pstmt.setString(6,  item.fp_side);
				pstmt.setInt(7,  item.fp_digit);
				pstmt.setString(8,  item.fp_image);
				pstmt.setString(9,  item.fp_iso_template);
				
				log.info("Add linkage: " + pstmt.toString());
				pstmt.executeUpdate();
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
	}
	
	/*
	 * Write the linkage items to the table
	 */
	public void getMatches(Connection sd, int oId, String changedBy, String instanceId, ArrayList<LinkageItem> items) throws SQLException {
		
		String sql = "insert into linkage (o_id, instance_id, survey_ident, col_name, fp_location, fp_side, fp_digit, fp_image, fp_iso_template, changed_by, changed_ts) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, instanceId);
			pstmt.setString(10, changedBy);
			for(LinkageItem item : items) {
				pstmt.setString(3, item.sIdent);
				pstmt.setString(4, item.colName);
				pstmt.setString(5,  item.fp_location);
				pstmt.setString(6,  item.fp_side);
				pstmt.setInt(7,  item.fp_digit);
				pstmt.setString(8,  item.fp_image);
				pstmt.setString(9,  item.fp_iso_template);
				
				log.info("Add linkage: " + pstmt.toString());
				pstmt.executeUpdate();
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
	}
	
	/*
	 * Set fingerprint templates from images
	 */
	public void setFingerprintTemplates(Connection sd, String basePath, String serverName) throws SQLException, IOException {
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtUpdate = null;
		
		try {
			
			String sql = "select id, fp_image "
					+ "from linkage "
					+ "where fp_native_template is null "
					+ "and fp_image is not null";
			
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int id = rs.getInt("id");
				String value = rs.getString("fp_image");
				
				File f = new File(basePath + "/" + value);
				URI uri = null;
				
				if(f.exists()) {
					uri = f.toURI();
				} else {
					// must be on s3
					uri = URI.create("https://" + serverName + "/" + value);
				}
				
				FingerprintTemplate template = new FingerprintTemplate(
					    new FingerprintImage(
					        Files.readAllBytes(Paths.get(uri)),
					        new FingerprintImageOptions()
					            .dpi(500)));
				byte[] serialized = template.toByteArray();
				
				String sqlUpdate = "update linkage "
						+ "set fp_native_template = ? "
						+ "where id = ?";
				pstmtUpdate = sd.prepareStatement(sqlUpdate);
				pstmtUpdate.setBytes(1, serialized);
				pstmtUpdate.setInt(2,  id);
				pstmtUpdate.executeUpdate();
				
			}
			
			
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}	
			try {if (pstmtUpdate != null) {pstmtUpdate.close();}} catch (SQLException e) {}	
		}
	}
	
	/*
	 * match a fingerprint
	 */
	public void match(Connection sd) {
		
	}
}
