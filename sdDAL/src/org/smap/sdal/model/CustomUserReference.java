package org.smap.sdal.model;

import java.sql.PreparedStatement;
import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class CustomUserReference {
	public boolean roles;	// primary key of the record
	public boolean myReferenceData;
	
	public boolean needCustomFile() {
		return roles || myReferenceData;
	}
	
	public int setFilterParams(PreparedStatement pstmt, ArrayList<SqlFrag> rfArray, int index, String tz, String userIdent) throws Exception {
		if (roles) {
			index = GeneralUtilityMethods.setArrayFragParams(pstmt, rfArray, index, tz);
		}
		if(myReferenceData) {
			pstmt.setString(index++, userIdent);
		}
		return index;
	}
}
