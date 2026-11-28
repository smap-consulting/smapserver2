package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;

/*
 * Dashboard panels, as the Analysis page shows them.
 *
 * Panels belong to a user and are listed per project.  An administrator can manage another user's
 * panels, but only a user of their own organisation, and a survey panel can only be for a survey the
 * panel's user can reach, as that is the only place it would be shown.
 *
 * The console's dashboard endpoint saves panels in its own client shaped form and uses the checks here.
 * The panel methods are used by MCP, for graph and table panels.
 */
public class DashboardManager {

	public static final String GRAPH = "graph";
	public static final String TABLE = "table";

	public static class Panel {
		public int id;
		public int seq;
		public String state;
		public String type;
		public String title;
		public int sId;
		public String sName;
		public String lang;
		public String qname;			// The question shown, or null for a table of all the data
		public String question;			// Its label
		public String fn;
		public String groupQuestionName;
		public String groupQuestionText;
		public String groupType;
		public String dateQuestionName;
		public String timeGroup;
		public Date fromDate;
		public Date toDate;
		public String chartType;
	}

	/*
	 * The user whose panels are being managed, or null if the requesting user may not manage them.
	 * Their own, or as an administrator those of a user in the same organisation
	 */
	public static String effectiveUser(Connection sd, String requestingUser, String onBehalfOf) throws SQLException {
		if(onBehalfOf == null || onBehalfOf.trim().isEmpty() || onBehalfOf.equals(requestingUser)) {
			return requestingUser;
		}
		if(!GeneralUtilityMethods.hasSecurityGroup(sd, requestingUser, Authorise.ADMIN_ID)) {
			return null;
		}
		int oId = GeneralUtilityMethods.getOrganisationId(sd, requestingUser);
		if(oId <= 0 || oId != GeneralUtilityMethods.getOrganisationId(sd, onBehalfOf)) {
			return null;
		}
		return onBehalfOf;
	}

	/*
	 * True if the survey is in a project the user is a member of, which is where a panel for it is shown
	 */
	public static boolean surveyReachable(Connection sd, String user, int sId) throws SQLException {
		String sql = "select 1 from survey s, user_project up, users u "
				+ "where s.s_id = ? and s.p_id = up.p_id and up.u_id = u.id and u.ident = ? and not s.deleted";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			pstmt.setString(2, user);
			return pstmt.executeQuery().next();
		}
	}

	/*
	 * A user's survey panels in a project, in the order they are shown
	 */
	public ArrayList<Panel> getPanels(Connection sd, String user, int projectId) throws SQLException {
		ArrayList<Panel> panels = new ArrayList<>();
		String sql = SELECT
				+ "from dashboard_settings d, survey s "
				+ "where d.ds_user_ident = ? "
				+ "and d.ds_subject_type = 'survey' "
				+ "and s.s_id = d.ds_s_id "
				+ "and s.p_id = ? "
				+ "order by d.ds_seq asc";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			pstmt.setInt(2, projectId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				panels.add(read(rs));
			}
		}
		return panels;
	}

	/*
	 * One of a user's survey panels, or null
	 */
	public Panel getPanel(Connection sd, String user, int id) throws SQLException {
		String sql = SELECT
				+ "from dashboard_settings d "
				+ "where d.ds_id = ? and d.ds_user_ident = ? and d.ds_subject_type = 'survey'";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, id);
			pstmt.setString(2, user);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? read(rs) : null;
		}
	}

	/*
	 * Add a panel after the user's other panels.  Returns its id
	 */
	public int addPanel(Connection sd, String user, Panel p) throws SQLException {
		String sql = "insert into dashboard_settings ("
				+ "ds_user_ident, ds_seq, ds_state, ds_subject_type, ds_layer_id, ds_region, ds_u_id, "
				+ "ds_q_is_calc, ds_inc_ro, ds_show_meta, ds_wrap_text, "
				+ "ds_type, ds_title, ds_s_id, ds_s_name, ds_lang, ds_qname, ds_question, ds_fn, "
				+ "ds_group_question_name, ds_group_question_text, ds_group_type, ds_date_question_name, "
				+ "ds_time_group, ds_from_date, ds_to_date, ds_chart_type) "
				+ "values (?, (select coalesce(max(ds_seq), -1) + 1 from dashboard_settings where ds_user_ident = ?), "
				+ "'shown', 'survey', -1, 'none', 0, false, false, true, true, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		try (PreparedStatement pstmt = sd.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)) {
			pstmt.setString(1, user);
			pstmt.setString(2, user);
			setPanelValues(pstmt, 3, p);
			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	/*
	 * Replace what a panel shows
	 */
	public void updatePanel(Connection sd, String user, Panel p) throws SQLException {
		String sql = "update dashboard_settings set "
				+ "ds_type = ?, ds_title = ?, ds_s_id = ?, ds_s_name = ?, ds_lang = ?, ds_qname = ?, "
				+ "ds_question = ?, ds_fn = ?, ds_group_question_name = ?, ds_group_question_text = ?, "
				+ "ds_group_type = ?, ds_date_question_name = ?, ds_time_group = ?, ds_from_date = ?, "
				+ "ds_to_date = ?, ds_chart_type = ? "
				+ "where ds_id = ? and ds_user_ident = ? and ds_subject_type = 'survey'";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			int idx = setPanelValues(pstmt, 1, p);
			pstmt.setInt(idx++, p.id);
			pstmt.setString(idx++, user);
			pstmt.executeUpdate();
		}
	}

	/*
	 * Delete a panel, and any panels shown as layers of it, as the console does
	 */
	public int deletePanel(Connection sd, String user, int id) throws SQLException {
		String sql = "delete from dashboard_settings "
				+ "where (ds_id = ? or (ds_layer_id != -1 and ds_layer_id = ?)) "
				+ "and ds_user_ident = ?";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, id);
			pstmt.setInt(2, id);
			pstmt.setString(3, user);
			return pstmt.executeUpdate();
		}
	}

	private static final String SELECT = "select d.ds_id, d.ds_seq, d.ds_state, d.ds_type, d.ds_title, "
			+ "d.ds_s_id, d.ds_s_name, d.ds_lang, d.ds_qname, d.ds_question, d.ds_fn, "
			+ "d.ds_group_question_name, d.ds_group_question_text, d.ds_group_type, "
			+ "d.ds_date_question_name, d.ds_time_group, d.ds_from_date, d.ds_to_date, d.ds_chart_type ";

	private Panel read(ResultSet rs) throws SQLException {
		Panel p = new Panel();
		p.id = rs.getInt("ds_id");
		p.seq = rs.getInt("ds_seq");
		p.state = rs.getString("ds_state");
		p.type = rs.getString("ds_type");
		p.title = rs.getString("ds_title");
		p.sId = rs.getInt("ds_s_id");
		p.sName = rs.getString("ds_s_name");
		p.lang = rs.getString("ds_lang");
		p.qname = rs.getString("ds_qname");
		p.question = rs.getString("ds_question");
		p.fn = rs.getString("ds_fn");
		p.groupQuestionName = rs.getString("ds_group_question_name");
		p.groupQuestionText = rs.getString("ds_group_question_text");
		p.groupType = rs.getString("ds_group_type");
		p.dateQuestionName = rs.getString("ds_date_question_name");
		p.timeGroup = rs.getString("ds_time_group");
		p.fromDate = rs.getDate("ds_from_date");
		p.toDate = rs.getDate("ds_to_date");
		p.chartType = rs.getString("ds_chart_type");
		return p;
	}

	private int setPanelValues(PreparedStatement pstmt, int idx, Panel p) throws SQLException {
		pstmt.setString(idx++, p.type);
		pstmt.setString(idx++, p.title);
		pstmt.setInt(idx++, p.sId);
		pstmt.setString(idx++, p.sName);
		pstmt.setString(idx++, p.lang);
		pstmt.setString(idx++, p.qname);
		pstmt.setString(idx++, p.question);
		pstmt.setString(idx++, p.fn);
		pstmt.setString(idx++, p.groupQuestionName);
		pstmt.setString(idx++, p.groupQuestionText);
		pstmt.setString(idx++, p.groupType);
		pstmt.setString(idx++, p.dateQuestionName);
		pstmt.setString(idx++, p.timeGroup);
		pstmt.setDate(idx++, p.fromDate);
		pstmt.setDate(idx++, p.toDate);
		pstmt.setString(idx++, p.chartType);
		return idx;
	}
}
