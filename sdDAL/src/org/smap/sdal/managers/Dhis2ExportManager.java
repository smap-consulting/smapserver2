package org.smap.sdal.managers;

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Dhis2Export;
import org.smap.sdal.model.Dhis2ExportItem;
import org.smap.sdal.model.Dhis2ImportSummary;
import org.smap.sdal.model.Dhis2Server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/*
 * Builds and sends aggregate data values from a survey bundle into a DHIS2 data set
 *
 * A DHIS2 data value is a total for a period and an organisation unit, so the work here is to
 * turn individual Smap submissions into those totals and key them the way DHIS2 expects.
 *
 * Re-sending is safe.  A data value is keyed by data element, period, org unit, category option
 * combo and attribute option combo, so sending the same period again corrects rather than
 * duplicates.  That is what makes late submissions a re-export rather than a problem
 */
public class Dhis2ExportManager {

	private static Logger log = Logger.getLogger(Dhis2ExportManager.class.getName());

	/*
	 * Build the data values for a period range and send them
	 * With dryRun true, DHIS2 validates and reports without storing anything
	 */
	public Dhis2ImportSummary export(Connection sd, Connection cResults, int oId,
			Dhis2Export export, String startDate, String endDate, boolean dryRun) throws Exception {

		Dhis2Server server = new Dhis2ServerManager().getWithToken(sd, oId);
		if(server == null) {
			throw new Exception("No DHIS2 connection has been set up for this organisation");
		}
		if(!server.enabled) {
			throw new Exception("The DHIS2 connection is disabled");
		}

		JsonArray dataValues = buildDataValues(sd, cResults, oId, export, startDate, endDate);
		if(dataValues.size() == 0) {
			throw new Exception("No data was found to export for that period");
		}

		JsonObject payload = new JsonObject();

		/*
		 * The data set has to be named, not just implied by the elements
		 *
		 * A data element commonly belongs to several data sets.  Without this DHIS2 cannot tell
		 * which one a value is for and rejects the whole import with "Data set detection failed,
		 * found multiple sets".  Naming it also makes DHIS2 check the elements really do belong
		 * to that data set, which catches a mis-mapped element rather than storing it quietly
		 */
		payload.addProperty("dataSet", export.dataset_uid);
		payload.add("dataValues", dataValues);

		Dhis2ImportSummary summary = new Dhis2Manager().postDataValueSet(server, payload, dryRun);
		summary.sent = dataValues.size();

		return summary;
	}

	/*
	 * Turn submissions into DHIS2 data values
	 *
	 * Each mapped item becomes one aggregate expression, and the whole lot is grouped by period
	 * and organisation unit in a single query rather than one query per item
	 */
	public JsonArray buildDataValues(Connection sd, Connection cResults, int oId,
			Dhis2Export export, String startDate, String endDate) throws Exception {

		JsonArray values = new JsonArray();

		if(export.items.isEmpty()) {
			throw new Exception("This export has no values mapped");
		}

		String table = getBundleTable(sd, cResults, export.group_survey_ident);
		if(table == null) {
			throw new Exception("No data table was found for this bundle. Has any data been submitted?");
		}

		String tz = GeneralUtilityMethods.getOrganisationTZ(sd, oId);

		// Columns are resolved from question names, which is what the mapping is keyed on
		String orgUnitCol = getColumnName(sd, export.group_survey_ident, export.orgunit_question);
		if(orgUnitCol == null) {
			throw new Exception("Question not found in this bundle: " + export.orgunit_question);
		}

		String periodExpr = getPeriodExpression(sd, export, tz);
		String dateCol = getDateColumn(sd, export);

		/*
		 * One query, grouped by period and org unit.  The aggregate expressions are built from
		 * the mapping, so a data set with twenty values is still one pass over the data
		 */
		StringBuilder sql = new StringBuilder("select ").append(periodExpr).append(" as dhis_period, ")
				.append(orgUnitCol).append(" as dhis_ou");

		ArrayList<Dhis2ExportItem> ordered = new ArrayList<>(export.items);
		for(int i = 0; i < ordered.size(); i++) {
			sql.append(", ").append(aggregateExpression(sd, export, ordered.get(i)))
				.append(" as v").append(i);
		}

		sql.append(" from ").append(table)
			.append(" where _bad = 'false'")
			.append(" and ").append(orgUnitCol).append(" is not null")
			.append(" and ").append(orgUnitCol).append(" != ''")
			.append(" and ").append(dateCol).append(" is not null");

		if(startDate != null && startDate.trim().length() > 0) {
			sql.append(" and ").append(dateCol).append(" >= ?::date");
		}
		if(endDate != null && endDate.trim().length() > 0) {
			// Inclusive of the end date, so a caller can ask for a month by its first and last day
			sql.append(" and ").append(dateCol).append(" < (?::date + interval '1 day')");
		}

		sql.append(" group by 1, 2 order by 1, 2");

		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql.toString());
			int idx = 1;
			if(startDate != null && startDate.trim().length() > 0) {
				pstmt.setString(idx++, startDate.trim());
			}
			if(endDate != null && endDate.trim().length() > 0) {
				pstmt.setString(idx++, endDate.trim());
			}

			log.info("DHIS2 export: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();

			while(rs.next()) {
				String period = rs.getString("dhis_period");
				String orgUnit = rs.getString("dhis_ou");
				if(period == null || orgUnit == null) {
					continue;
				}

				for(int i = 0; i < ordered.size(); i++) {
					String value = rs.getString("v" + i);
					if(value == null) {
						continue;		// Nothing to say about this element for this period
					}

					Dhis2ExportItem item = ordered.get(i);
					JsonObject dv = new JsonObject();
					dv.addProperty("dataElement", item.data_element);
					dv.addProperty("period", period);
					dv.addProperty("orgUnit", orgUnit);
					if(item.category_option_combo != null && item.category_option_combo.trim().length() > 0) {
						dv.addProperty("categoryOptionCombo", item.category_option_combo.trim());
					}
					dv.addProperty("value", value);
					values.add(dv);
				}
			}

		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return values;
	}

	// -------------------------------------------------------------------------
	// Building the query
	// -------------------------------------------------------------------------

	/*
	 * The aggregate expression for one mapped value
	 *
	 * Deliberately limited to counting and summing.  DHIS2 has indicators, program indicators and
	 * predictors of its own, so the useful division of labour is to send it the smallest raw
	 * numbers and let it derive what it wants.  An expression language here would duplicate
	 * theirs and then diverge from it
	 */
	private String aggregateExpression(Connection sd, Dhis2Export export, Dhis2ExportItem item)
			throws Exception {

		if(Dhis2ExportItem.AGG_COUNT.equals(item.aggregation)) {
			if(item.question_name == null || item.question_name.trim().length() == 0) {
				return "count(*)::text";		// Number of submissions
			}
			String col = requireColumn(sd, export, item.question_name);
			// Count the submissions that answered this question, not all of them
			return "count(" + col + ")::text";
		}

		if(Dhis2ExportItem.AGG_SUM.equals(item.aggregation)) {
			String col = requireColumn(sd, export, item.question_name);
			/*
			 * Results columns for numeric questions are numeric, but a question whose type was
			 * changed can leave a text column behind, so the cast is explicit and a value that
			 * will not cast is a clear error rather than a silent zero
			 */
			return "sum(" + col + ")::text";
		}

		if(Dhis2ExportItem.AGG_ONE.equals(item.aggregation)) {
			String col = requireColumn(sd, export, item.question_name);
			/*
			 * One submission is one value.  Still aggregated, because the query groups by period
			 * and org unit: max() returns the single value where there is one, and where a period
			 * holds more than one submission it is a mapping mistake rather than a total
			 */
			return "max(" + col + "::text)";
		}

		throw new Exception("Unknown aggregation: " + item.aggregation);
	}

	private String requireColumn(Connection sd, Dhis2Export export, String questionName) throws Exception {
		if(questionName == null || questionName.trim().length() == 0) {
			throw new Exception("A question is required for this aggregation");
		}
		String col = getColumnName(sd, export.group_survey_ident, questionName);
		if(col == null) {
			throw new Exception("Question not found in this bundle: " + questionName);
		}
		return col;
	}

	/*
	 * The SQL producing a DHIS2 period identifier
	 *
	 * Two cases, and they must be treated differently.  A date question is a PostgreSQL date and
	 * carries no timezone, so it is used as it stands.  A timestamp holds an instant normalised
	 * to UTC, so it is rendered in the organisation's timezone first: without that, a submission
	 * made in the evening east of UTC lands in the previous month
	 */
	private String getPeriodExpression(Connection sd, Dhis2Export export, String tz) throws Exception {

		String col = getDateColumn(sd, export);
		String d;

		if(isTimestamp(sd, export)) {
			d = "(" + col + " at time zone " + quote(tz) + ")";
		} else {
			d = col;
		}

		String type = export.period_type == null ? "Monthly" : export.period_type;

		if("Monthly".equalsIgnoreCase(type)) {
			return "to_char(" + d + ", 'YYYYMM')";
		} else if("Weekly".equalsIgnoreCase(type)) {
			// DHIS2 weeks are ISO weeks, 2026W12
			return "to_char(" + d + ", 'IYYY') || 'W' || to_char(" + d + ", 'IW')";
		} else if("Quarterly".equalsIgnoreCase(type)) {
			return "to_char(" + d + ", 'YYYY') || 'Q' || to_char(" + d + ", 'Q')";
		} else if("Yearly".equalsIgnoreCase(type)) {
			return "to_char(" + d + ", 'YYYY')";
		} else if("Daily".equalsIgnoreCase(type)) {
			return "to_char(" + d + ", 'YYYYMMDD')";
		}

		throw new Exception("Period type not supported: " + type);
	}

	/*
	 * The column the period is derived from.  Falls back to the upload time where no date
	 * question has been chosen
	 */
	private String getDateColumn(Connection sd, Dhis2Export export) throws Exception {

		if(export.period_question == null || export.period_question.trim().length() == 0) {
			return "_upload_time";
		}

		String col = getColumnName(sd, export.group_survey_ident, export.period_question);
		if(col == null) {
			throw new Exception("Question not found in this bundle: " + export.period_question);
		}
		return col;
	}

	/*
	 * True where the period comes from something holding an instant rather than a calendar date
	 */
	private boolean isTimestamp(Connection sd, Dhis2Export export) throws SQLException {

		if(export.period_question == null || export.period_question.trim().length() == 0) {
			return true;		// _upload_time is a timestamp with time zone
		}

		String sql = "select q.qtype from question q, form f, survey s "
				+ "where q.f_id = f.f_id "
				+ "and f.s_id = s.s_id "
				+ "and s.group_survey_ident = ? "
				+ "and not s.deleted "
				+ "and not q.soft_deleted "
				+ "and q.qname = ? "
				+ "limit 1";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, export.group_survey_ident);
			pstmt.setString(2, export.period_question);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String qtype = rs.getString(1);
				return "dateTime".equals(qtype) || "start".equals(qtype) || "end".equals(qtype);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return false;
	}

	// -------------------------------------------------------------------------
	// Resolving the bundle
	// -------------------------------------------------------------------------

	/*
	 * The data table shared by the surveys in a bundle
	 * Any survey in the bundle answers this, since sharing a table is what a bundle is
	 */
	public String getBundleTable(Connection sd, Connection cResults, String groupSurveyIdent) throws SQLException {

		String table = null;
		String sql = "select f.table_name from form f, survey s "
				+ "where f.s_id = s.s_id "
				+ "and s.group_survey_ident = ? "
				+ "and not s.deleted "
				+ "and f.parentform = 0 "
				+ "limit 1";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, groupSurveyIdent);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String name = rs.getString(1);
				if(GeneralUtilityMethods.tableExists(cResults, name)) {
					table = name;
				}
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return table;
	}

	/*
	 * The results column for a question name anywhere in the bundle
	 *
	 * Name rather than id, deliberately.  Replacing a survey from an XLSForm creates new
	 * questions with new ids while the names carry across, and the mapping is expected to keep
	 * working, exactly as role rules and relevance do
	 */
	public String getColumnName(Connection sd, String groupSurveyIdent, String questionName) throws SQLException {

		String column = null;
		String sql = "select q.column_name from question q, form f, survey s "
				+ "where q.f_id = f.f_id "
				+ "and f.s_id = s.s_id "
				+ "and s.group_survey_ident = ? "
				+ "and not s.deleted "
				+ "and not q.soft_deleted "
				+ "and f.parentform = 0 "
				+ "and q.qname = ? "
				+ "limit 1";
		PreparedStatement pstmt = null;

		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, groupSurveyIdent);
			pstmt.setString(2, questionName);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				column = rs.getString(1);
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Getting column for " + questionName, e);
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(SQLException e) {}}
		}

		return column;
	}

	/*
	 * A timezone name goes into the SQL rather than a parameter, because it sits inside an
	 * expression that is also used in the group by.  Quoted and checked rather than trusted
	 */
	private String quote(String tz) throws Exception {
		if(tz == null || !tz.matches("[A-Za-z0-9_+/\\-]+")) {
			throw new Exception("Unusable timezone for this organisation: " + tz);
		}
		return "'" + tz + "'";
	}
}
