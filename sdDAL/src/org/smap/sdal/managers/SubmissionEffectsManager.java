package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import com.google.gson.Gson;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.NotifyDetails;

/*
 * What will happen if a record is added to this survey, worked out before it is.
 *
 * A submission is not only a row. SubmissionEventManager hands every new record to
 * NotificationManager and then to TaskManager, so it can send email, send SMS, call a webhook and
 * create tasks. The row can be undone; a sent message cannot, which is why anything submitting on a
 * person's behalf has to be able to say what it is about to set off before it does it.
 *
 * Every number here is an upper bound, and deliberately so. A notification carries a filter and a
 * task group carries a rule, both evaluated against the finished record - see
 * NotificationManager's call to testFilter, which takes an instanceId. Before the record exists
 * there is nothing to evaluate them against, so this counts what is configured and eligible and
 * says plainly that filters may reduce it. An estimate that admitted no uncertainty would be a
 * worse thing to put in front of someone approving a send.
 */
public class SubmissionEffectsManager {

	private static Logger log = Logger.getLogger(SubmissionEffectsManager.class.getName());

	private ResourceBundle localisation;

	public SubmissionEffectsManager(ResourceBundle l) {
		this.localisation = l;
	}

	/* What one new record would set off */
	public static class Effects {
		public int emails;			// recipients, not notifications
		public int smsMessages;
		public int webhooks;
		public int tasks;
		public List<String> notifications = new ArrayList<>();		// names, so a person can recognise them
		public List<String> taskGroups = new ArrayList<>();

		public boolean isEmpty() {
			return emails == 0 && smsMessages == 0 && webhooks == 0 && tasks == 0;
		}

		/*
		 * What a caller has to restate to show they were told.
		 *
		 * The counts rather than a token, because the point is that the numbers pass through the
		 * conversation where a person can see them before approving the call. A token would prove
		 * only that the tool had been called twice.
		 */
		public boolean matches(Integer emails, Integer sms, Integer webhooks, Integer tasks) {
			return emails != null && emails == this.emails
					&& sms != null && sms == this.smsMessages
					&& webhooks != null && webhooks == this.webhooks
					&& tasks != null && tasks == this.tasks;
		}

		/* A sentence for somebody deciding whether to allow it */
		public String describe() {
			if(isEmpty()) {
				return "This survey sends nothing and creates no tasks when a record arrives.";
			}
			StringBuilder sb = new StringBuilder("Adding a record to this survey can send ");
			List<String> parts = new ArrayList<>();
			if(emails > 0) {
				parts.add(emails + " email" + (emails == 1 ? "" : "s"));
			}
			if(smsMessages > 0) {
				parts.add(smsMessages + " SMS message" + (smsMessages == 1 ? "" : "s"));
			}
			if(webhooks > 0) {
				parts.add(webhooks + " webhook call" + (webhooks == 1 ? "" : "s"));
			}
			if(parts.isEmpty()) {
				sb = new StringBuilder("Adding a record to this survey can create ");
			} else {
				sb.append(String.join(", ", parts));
				if(tasks > 0) {
					sb.append(" and create ");
				}
			}
			if(tasks > 0) {
				sb.append(tasks).append(" task").append(tasks == 1 ? "" : "s");
			}
			sb.append(". These are the most it can do: each notification and task rule is also "
					+ "filtered on the record itself, which is not known until it exists, so the "
					+ "real number may be lower. Messages that go out cannot be recalled.");
			return sb.toString();
		}
	}

	public Effects predict(Connection sd, int sId, String sIdent) throws Exception {

		Effects e = new Effects();
		String bundleIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);

		notifications(sd, sId, bundleIdent, e);
		tasks(sd, sId, e);
		return e;
	}

	/*
	 * The same selection NotificationManager makes when a record arrives: enabled, triggered by a
	 * submission, belonging to this survey or its bundle, and not one of the two targets that move
	 * data rather than tell somebody about it.
	 *
	 * Kept deliberately close to that query.  If the two drift, this reports a send that will not
	 * happen or, worse, stays silent about one that will.
	 */
	private void notifications(Connection sd, int sId, String bundleIdent, Effects e) throws Exception {

		String sql = "select n.name, n.target, n.notify_details "
				+ "from forward n "
				+ "where ((n.s_id = ? and not n.bundle) or (n.bundle_ident = ? and n.bundle)) "
				+ "and n.target != 'forward' "
				+ "and n.target != 'document' "
				+ "and n.enabled = 'true' "
				+ "and n.trigger = 'submission'";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			pstmt.setString(2, bundleIdent);
			log.fine("Predicting notifications: " + pstmt.toString());

			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String name = rs.getString("name");
				String target = rs.getString("target");
				NotifyDetails nd = null;
				try {
					nd = new Gson().fromJson(rs.getString("notify_details"), NotifyDetails.class);
				} catch (Exception ex) {
					log.warning("Unreadable notify_details for notification " + name);
				}

				if("email".equals(target) || "escalate".equals(target)) {
					/*
					 * Counted by recipient rather than by notification, because that is the number
					 * that matters to whoever is approving: one notification addressed to forty
					 * people is forty emails.
					 */
					Recipients r = recipients(nd);
					e.emails += r.count;
					e.notifications.add(name + " (" + target + "): " + r.describe());
				} else if("sms".equals(target) || "conversation".equals(target)) {
					Recipients r = recipients(nd);
					e.smsMessages += r.count;
					e.notifications.add(name + " (" + target + "): " + r.describe());
				} else if("webhook".equals(target)) {
					e.webhooks++;
					e.notifications.add(name + " (" + target + ")");
				} else {
					e.notifications.add(name + " (" + target + ")");
				}
			}
		}
	}

	/*
	 * How many people one notification reaches.
	 *
	 * The fixed address list can be counted exactly.  A notification can also address whoever
	 * answered a question on the record, or the person a case is assigned to, and neither is
	 * knowable before the record exists - each is counted as one more, so the total is never lower
	 * than what will actually be sent.
	 */
	private Recipients recipients(NotifyDetails nd) {

		Recipients r = new Recipients();
		if(nd == null) {
			r.count = 1;					// unreadable settings: assume it reaches somebody
			r.sources.add("settings that could not be read");
			return r;
		}

		/*
		 * Only addresses with something in them.  A notification edited in the console can keep an
		 * empty row in its address list, and counting that would overstate the send.
		 */
		int addresses = 0;
		if(nd.emails != null) {
			for(String address : nd.emails) {
				if(address != null && address.trim().length() > 0) {
					addresses++;
				}
			}
		}
		if(addresses > 0) {
			r.count += addresses;
			r.sources.add(addresses + (addresses == 1 ? " address" : " addresses"));
		}

		/*
		 * These two are addressed from the record, so how many people they reach is not knowable
		 * until it exists.  Each counts as one, which keeps the total from ever being lower than
		 * what is actually sent, and each is named so the reason for the number is visible rather
		 * than having to be worked out from a total that looks wrong.
		 */
		if(isSet(nd.emailQuestionName)) {
			r.count++;
			r.sources.add("whoever is named in " + nd.emailQuestionName);
		} else if(nd.emailQuestion > 0) {
			r.count++;
			r.sources.add("whoever is named in a question on the record");
		}
		if(nd.emailAssigned) {
			r.count++;
			r.sources.add("the person the record is assigned to");
		}

		if(r.count == 0) {
			r.count = 1;
			r.sources.add("a recipient this cannot identify in advance");
		}
		return r;
	}

	/*
	 * Whether a question was actually chosen to take the address from.
	 *
	 * "-1" means nobody picked one.  It is not empty and it is not null, so a plain presence test
	 * reads it as a real question and counts a recipient that does not exist - which is what made a
	 * notification with two addresses report three emails.  The same test is written in
	 * NotificationManager where it decides whether to resolve the name, and this has to agree with
	 * it: counting a recipient the sender will not use overstates the send, and an approval figure
	 * that cannot be reconciled with what the console shows is one nobody will trust.
	 */
	private boolean isSet(String questionName) {
		return questionName != null
				&& questionName.trim().length() > 0
				&& !questionName.trim().equals("-1");
	}

	/* Where a notification's recipients come from, so a total can be checked rather than trusted */
	private static class Recipients {
		int count;
		List<String> sources = new ArrayList<>();

		String describe() {
			return count + (count == 1 ? " recipient" : " recipients")
					+ " (" + String.join(", ", sources) + ")";
		}
	}

	/*
	 * Task groups fed by this survey.  TaskManager.updateTasksForSubmission reads exactly these, one
	 * task per rule, each with its own condition on the record.
	 */
	private void tasks(Connection sd, int sId, Effects e) throws Exception {

		String sql = "select name from task_group where source_s_id = ?";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				e.tasks++;
				e.taskGroups.add(rs.getString("name"));
			}
		}
	}
}
