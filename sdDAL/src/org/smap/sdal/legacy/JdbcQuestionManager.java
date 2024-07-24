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

package org.smap.sdal.legacy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JdbcQuestionManager {
	
	PreparedStatement pstmt = null;
	String sql = "insert into question ("
			+ "q_id, "
			+ "f_id, "
			+ "seq, "
			+ "qname, "
			+ "qtype, "
			+ "question, "
			+ "qtext_id, "
			+ "defaultanswer, "
			+ "info, "
			+ "infotext_id,"
			+ "visible,"
			+ "source,"
			+ "source_param,"
			+ "readonly,"
			+ "readonly_expression,"
			+ "mandatory,"
			+ "relevant,"
			+ "calculate,"
			+ "qconstraint,"
			+ "constraint_msg,"
			+ "required_expression,"
			+ "appearance,"
			+ "parameters,"
			+ "nodeset,"
			+ "nodeset_value,"
			+ "nodeset_label,"
			+ "column_name,"
			+ "published,"
			+ "l_id,"
			+ "autoplay,"
			+ "accuracy,"
			+ "dataType,"
			+ "compressed,"
			+ "intent,"
			+ "set_value"
			+ ") "
			+ "values (nextval('q_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
				+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
				+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
	
	PreparedStatement pstmtGetBySurveyId;
	PreparedStatement pstmtGetByFormId;
	String sqlGet = "select "
			+ "q_id, "
			+ "f_id, "
			+ "seq, "
			+ "qname, "
			+ "qtype, "
			+ "question, "
			+ "qtext_id, "
			+ "defaultanswer, "
			+ "info, "
			+ "infotext_id,"
			+ "visible,"
			+ "source,"
			+ "source_param,"
			+ "readonly,"
			+ "readonly_expression,"
			+ "mandatory,"
			+ "relevant,"
			+ "calculate,"
			+ "qconstraint,"
			+ "constraint_msg,"
			+ "required_expression,"
			+ "appearance,"
			+ "parameters,"
			+ "nodeset,"
			+ "nodeset_value,"
			+ "nodeset_label,"
			+ "column_name,"
			+ "published,"
			+ "l_id,"
			+ "autoplay,"
			+ "accuracy,"
			+ "dataType,"
			+ "compressed,"
			+ "intent,"
			+ "set_value,"
			+ "trigger "
			+ "from question where soft_deleted = 'false' and ";
	String sqlGetBySurveyId = "f_id in (select f_id from form where s_id = ?)"
			+ " order by f_id, seq";
	String sqlGetByFormId = "f_id  = ?"
			+ " order by seq";
	
	String sqlGetListName = "select name from listname where l_id = ?";
	PreparedStatement pstmtGetListName;
	
	/*
	 * Constructor
	 */
	public JdbcQuestionManager(Connection sd) throws SQLException {
		pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		pstmtGetBySurveyId = sd.prepareStatement(sqlGet + sqlGetBySurveyId);
		pstmtGetByFormId = sd.prepareStatement(sqlGet + sqlGetByFormId);
		pstmtGetListName = sd.prepareStatement(sqlGetListName);
	}
	
	/*
	 * Write to the database
	 */
	public void write(Question q, String xFormRoot) throws Exception {
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		pstmt.setInt(1, q.getFormId());
		pstmt.setInt(2, q.getSeq());
		pstmt.setString(3, q.getName());
		pstmt.setString(4, q.getType());
		pstmt.setString(5, q.getQuestion());
		pstmt.setString(6, q.getQTextId());
		pstmt.setString(7, q.getDefaultAnswer());
		pstmt.setString(8,  q.getInfo());
		pstmt.setString(9, q.getInfoTextId());	
		pstmt.setBoolean(10, q.isVisible());	
		pstmt.setString(11, q.getSource());	
		pstmt.setString(12, q.getSourceParam());	
		pstmt.setBoolean(13, q.isReadOnly());
		pstmt.setBoolean(14, q.isMandatory());
		pstmt.setString(15, q.getRelevant(false, null, xFormRoot));
		pstmt.setString(16, q.getCalculate(false, null, xFormRoot));
		pstmt.setString(17, q.getConstraint(false, null, xFormRoot));
		pstmt.setString(18, q.getConstraintMsg()); // ok
		pstmt.setString(19, q.getRequiredExpression());
		pstmt.setString(20, q.getAppearance(false, null));
		pstmt.setString(21, GeneralUtilityMethods.convertParametersToString(q.getParameters()));
		
		String nodeset = null;
		String nodeset_value = null;
		String nodeset_label = null;
		if(q.getType().startsWith("select")) {
			
			nodeset = UtilityMethods.getNodeset(false, 
					true, 
					null, 
					false,
					q.getNodeset(),
					q.getAppearance(false, null),
					q.getFormId(),
					q.getName(),
					false	// Not converted to XPath hence relativePath setting is ignored
					);
			if(nodeset == null || nodeset.trim().length() == 0) {
				nodeset = GeneralUtilityMethods.getNodesetFromChoiceFilter(null, q.getListName());
				nodeset_value = "name";
				nodeset_label = "jr:itext(itextId)";
			} else {
				nodeset_value = q.getNodesetValue();
				nodeset_label = q.getNodesetLabel();
			}
			
		}	
		pstmt.setString(22, nodeset);
		pstmt.setString(23, nodeset_value);
		pstmt.setString(24, nodeset_label);
		
		pstmt.setString(25, q.getColumnName(false)); 
		pstmt.setBoolean(26, q.isPublished());
		pstmt.setInt(27, q.getListId());  
		pstmt.setString(28, q.getAutoPlay());
		pstmt.setString(29, q.getAccuracy());
		pstmt.setString(30, q.getDataType());
		if(q.getType().equals("select")) {
			pstmt.setBoolean(31, true);			// Set all select multiple to compressed
		} else {
			pstmt.setBoolean(31, false);
		}
		pstmt.setString(32, q.getIntent());
		pstmt.setString(33, q.getSetValueArrayAsString(gson));
		
		pstmt.executeUpdate();
		
		ResultSet rs = pstmt.getGeneratedKeys();
		if(rs.next()) {
			q.setId(rs.getInt(1));
			
		}
	}
	
	/*
	 * Get a list of questions in the passed in survey
	 */
	public List <Question> getBySurveyId(int sId, List <Form> forms) throws SQLException, ApplicationException {
		pstmtGetBySurveyId.setInt(1, sId);
		return getQuestionList(pstmtGetBySurveyId, forms);
	}
	
	/*
	 * Get a list of questions in the passed in form
	 */
	public List <Question> getByFormId(int fId) throws SQLException, ApplicationException {
		pstmtGetByFormId.setInt(1, fId);
		return getQuestionList(pstmtGetByFormId, null);
	}
	
	/*
	 * Close prepared statements
	 */
	public void close() {
		try {if(pstmt != null) {pstmt.close();}} catch(Exception e) {};
		try {if(pstmtGetByFormId != null) {pstmtGetByFormId.close();}} catch(Exception e) {};
		try {if(pstmtGetBySurveyId != null) {pstmtGetBySurveyId.close();}} catch(Exception e) {};
	}
	
	private List<Question> getQuestionList(PreparedStatement pstmtGet, 
			List <Form> forms) throws SQLException, ApplicationException {
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		ArrayList <Question> questions = new ArrayList<Question> ();
		Stack<String> paths = new Stack<String>();
		String currentPath = "";
		
		ResultSet rs = pstmtGet.executeQuery();
		while(rs.next()) {
			Question q = new Question();
			q.setId(rs.getInt("q_id"));
			q.setFormId(rs.getInt("f_id"));
			q.setSeq(rs.getInt("seq"));
			q.setName(rs.getString("qname"));
			q.setType(rs.getString("qtype"));
			q.setQuestion(rs.getString("question"));
			q.setQTextId(rs.getString("qtext_id"));
			q.setDefaultAnswer(rs.getString("defaultanswer"));
			q.setInfo(rs.getString("info"));
			q.setInfoTextId(rs.getString("infotext_id"));
			q.setVisible(rs.getBoolean("visible"));
			q.setSource(rs.getString("source"));
			q.setSourceParam(rs.getString("source_param"));
			q.setReadOnly(rs.getBoolean("readonly"));
			q.setReadOnlyExpression(rs.getString("readonly_expression"));
			q.setMandatory(rs.getBoolean("mandatory"));
			q.setRelevant(rs.getString("relevant"));
			q.setCalculate(rs.getString("calculate"));
			q.setConstraint(rs.getString("qconstraint"));
			q.setConstraintMsg(rs.getString("constraint_msg"));
			q.setRequiredExpression(rs.getString("required_expression"));
			q.setAppearance(rs.getString("appearance"));
			q.setParameters(rs.getString("parameters"));
			q.setNodeset(rs.getString("nodeset"));
			q.setNodesetValue(rs.getString("nodeset_value"));
			q.setNodesetLabel(rs.getString("nodeset_label"));
			q.setColumnName(rs.getString("column_name"));
			q.setPublished(rs.getBoolean("published"));
			q.setListId(rs.getInt("l_id"));
			q.setAutoPlay(rs.getString("autoplay"));
			q.setAccuracy(rs.getString("accuracy"));
			q.setDataType(rs.getString("dataType"));
			q.setCompressed(rs.getBoolean("compressed"));
			q.setIntent(rs.getString("intent"));
			q.setSetValue(gson, rs.getString("set_value"));
			q.setTrigger(rs.getString("trigger"));
		
			/*
			 * If the list id exists then set the list name
			 */
			if(q.getListId() > 0) {
				pstmtGetListName.setInt(1, q.getListId());
				ResultSet rsListName = pstmtGetListName.executeQuery();
				if(rsListName.next()) {
					q.setListName(rsListName.getString(1));
				}
			}
			/*
			 * Set the relative path
			 */
			q.setRelativePath(currentPath + "/" + q.getName());
			if(q.getType().equals("begin group")) {
				paths.push(currentPath);
				currentPath = q.getRelativePath();
			} else if(q.getType().equals("end group")) {
				if(paths.size() > 0) {
					currentPath = paths.pop();
				} else {
					// Just throw the error in English, this should not normally happen unless the template gets corrupted
					throw new ApplicationException("Question " + q.getName() + " with type \"end group\" does not have a begin group. Export the template to an XLS file and fix it there.");
				}
			}
			
			/*
			 * If this question marks the start of a sub form then set the relative path of the subform
			 */
			if(forms != null && (q.getType().equals("begin repeat") || q.getType().equals("geopolygon") || q.getType().equals("geolinestring"))) {
				for(Form f : forms) {
					if(f.getParentQuestionId() == q.getId()) {
						f.setRelativePath(q.getRelativePath());
					}
				}
			}
			
			questions.add(q);
		}
		return questions;
	}
}
