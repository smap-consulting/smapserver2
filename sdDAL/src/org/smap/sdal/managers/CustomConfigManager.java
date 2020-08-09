package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ResourceBundle;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class CustomConfigManager {

	private ResourceBundle localisation;
	public CustomConfigManager(ResourceBundle l) {
		this.localisation = l;
	}
	public String getConfig(Connection sd, int id) throws SQLException {
		String config = null;
		String sql = "select name, config, type_id, survey_ident "
				+ "from custom_report "
				+ "where id = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, id);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				config = rs.getString("config");
			}
		} finally {
			if(pstmt != null){try {pstmt.close();}catch(Exception e) {}}
		}
		return config;
	}
	
	
	public void validateColumn(Connection cResults, String tableName, String col, String surveyName) throws ApplicationException {
		if(!GeneralUtilityMethods.hasColumn(cResults, tableName, col)) {
			String msg = localisation.getString("qnf").replace("%s1", col).replace("%s2", surveyName);
			throw new ApplicationException(msg);
		}
	}
}
