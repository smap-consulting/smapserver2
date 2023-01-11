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

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Link;
import org.smap.sdal.model.LinkageItem;
import org.smap.sdal.model.LinkedTarget;
import org.smap.sdal.model.Match;

import com.machinezoo.sourceafis.FingerprintImage;
import com.machinezoo.sourceafis.FingerprintImageOptions;
import com.machinezoo.sourceafis.FingerprintMatcher;
import com.machinezoo.sourceafis.FingerprintTemplate;

public class LinkageManager {
	
	private static Logger log =
			 Logger.getLogger(LinkageManager.class.getName());

	public final String REQUEST_FP_IMAGE = "ex:uk.ac.lshtm.keppel.android.SCAN(type='image')";
	public final String REQUEST_FP_ISO_TEMPLATE = "ex:uk.ac.lshtm.keppel.android.SCAN(type='iso')";
	
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
		
		if(links != null && value != null && value.trim().length() > 0  && appearance != null) {
			if(appearance.contains(REQUEST_FP_IMAGE) || appearance.contains(REQUEST_FP_ISO_TEMPLATE)) {
				// Fingerprint
				String image = null;
				String isoTemplate = null;
				String fpLocation = null;
				String fpSide = null;
				int fpDigit = 0;
				
				// Set the value
				if(appearance.contains(REQUEST_FP_IMAGE)) {
					image = value;
				} else {
					isoTemplate = value;
				}
				
				if(params != null) {
					for(KeyValueSimp p : params) {
						if(p.k.equals("fp_location")) {
							fpLocation  = p.v;
						} else if(p.k.equals("fp_side")) {
							fpSide = p.v;
						} else if(p.k.equals("fp_digit")) {
							try {
								fpDigit = Integer.parseInt(p.v);
							} catch (Exception e) {
								
							}
						}
					}
				}
				
				links.add(new LinkageItem(0, sIdent, colName, fpLocation, fpSide, fpDigit, image, isoTemplate));				

			} 
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
	 * Set fingerprint templates in the linkage table using the image in the same table
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
			log.info("Get linkages to update: " + pstmt.toString());
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
					String extension = value.substring(value.lastIndexOf('.') + 1);
					URL url = new URL("https://" + serverName + "/" + value);
					BufferedImage tempImg = ImageIO.read(url);
					File file = new File(basePath + "/temp/fp_" + UUID.randomUUID() + "." + extension);
					ImageIO.write(tempImg, extension, file);
					uri = file.toURI();
				}
				
				FingerprintTemplate template = new FingerprintTemplate(
					    new FingerprintImage(
					        Files.readAllBytes(Paths.get(uri)),
					        new FingerprintImageOptions()
					            .dpi(500)));

				byte[] serialized = null;
				try {
					serialized = template.toByteArray();
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
					
				if(serialized != null) {
					String sqlUpdate = "update linkage "
							+ "set fp_native_template = ? "
							+ "where id = ?";
					pstmtUpdate = sd.prepareStatement(sqlUpdate);
					pstmtUpdate.setBytes(1, serialized);
					pstmtUpdate.setInt(2,  id);
					log.info("update linkage: " + pstmtUpdate.toString());
					pstmtUpdate.executeUpdate();
				}
			}
			
			
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}	
			try {if (pstmtUpdate != null) {pstmtUpdate.close();}} catch (SQLException e) {}	
		}
	}
	
	/*
	 * Get a list of matches to a fingerprint template
	 */
	public ArrayList<Match> matchSingleTemplate(Connection sd, String server, int oId, FingerprintTemplate probe, double threshold, String image) throws SQLException {
		
		String sql = "select id, fp_native_template, fp_image, survey_ident, col_name,"
				+ "fp_location, fp_side, fp_digit "
				+ "from linkage "
				+ "where fp_native_template is not null "
				+ "and fp_image is not null "
				+ "and o_id  = ? "
				+ "order by id desc";
		
		PreparedStatement pstmt = null;
		
		ArrayList<Match> matches = new ArrayList<> ();
		try {
			
			pstmt = sd.prepareStatement(sql);
			
			sd.setAutoCommit(false);	// page the results to reduce memory usage	
			pstmt.setFetchSize(100);
			pstmt.setInt(1,  oId);
			
			log.info("Match fingerprint: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {

				int id = rs.getInt("id");
				FingerprintTemplate candidate = new FingerprintTemplate(rs.getBytes("fp_native_template"));
				String fpImage = rs.getString("fp_image");
				if(fpImage != null) {
					fpImage = "https://" + server + "/" + fpImage;
				}
				
				if(image != null && fpImage != null && !image.equals(fpImage)) {  // Don't match against identical image
					String sIdent = rs.getString("survey_ident");
					String colName = rs.getString("col_name");
					String fpLocation = rs.getString("fp_location");
					String fpSide = rs.getString("fp_side");
					int fpDigit = rs.getInt("fp_digit");
						
					double score = new FingerprintMatcher(probe).match(candidate);
					log.info("Score: " + score + " Threshold: " + threshold);
					
					if(score > threshold) {
						Match match = new Match(id, score, new LinkageItem(id, sIdent, colName, fpLocation, fpSide, fpDigit, fpImage, null));
						
						/*
						 * Get the details of where this match is stored
						 * If the match is not found, presumably because it has been deleted since the linkage cache was created then expire the link
						 */
						if(!getStorageDetails(sd, match)) {
							markExpired();
						} else {	
							matches.add(match);
						}
					}
				}
			}
					
		} finally {
			try {sd.setAutoCommit(true);} catch (Exception ex) {}
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		
		return matches;
	}
	
	
	/*
	 * Get linkages in a single record of a survey
	 */
	public ArrayList<LinkageItem> getRecordLinkages(Connection sd, String server, String sIdent, String instanceId) throws SQLException {
		
		String sql = "select id, fp_image, col_name,"
				+ "fp_location, fp_side, fp_digit "
				+ "from linkage "
				+ "where survey_ident = ? "
				+ "and instance_id = ? "
				+ "order by id desc";
		
		PreparedStatement pstmt = null;
		
		ArrayList<LinkageItem> items = new ArrayList<> ();
		try {
			
			pstmt = sd.prepareStatement(sql);
			
			pstmt.setString(1, sIdent);
			pstmt.setString(2, instanceId);
			
			log.info("Get linkage items for a record: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {

				int id = rs.getInt("id");
				String fpImage = rs.getString("fp_image");
				String colName = rs.getString("col_name");
				String fpLocation = rs.getString("fp_location");
				String fpSide = rs.getString("fp_side");
				int fpDigit = rs.getInt("fp_digit");

				if(fpImage != null) {
					fpImage = "https://" + server + "/" + fpImage;
					items.add(new LinkageItem(id, sIdent, colName, fpLocation, fpSide, fpDigit, fpImage, null));	// Only add matches for images TODO iso template
				}
			}
					
		} finally {
			try {sd.setAutoCommit(true);} catch (Exception ex) {}
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		
		return items;
	}
	
	private boolean getStorageDetails(Connection sd, Match match) {
		return true;
	}
	
	private void markExpired() {
		
	}
	
}
