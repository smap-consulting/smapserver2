package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/*
 * The multi step jobs, written down.
 *
 * A tool list says what can be done one call at a time.  It does not say that moving a survey between
 * projects means making the project, putting people in it and then changing the survey's settings, in
 * that order, or that the second step is the one everybody forgets.  Somebody reading only the tool
 * descriptions has to rediscover each of those, and will rediscover some of them wrongly.
 *
 * So these are not prompts in the sense of clever wording.  They are the procedures: which tool
 * first, what to check before changing anything, and which mistakes are the expensive ones.  A
 * client offers them to a person as slash commands, and the text arrives as the opening instruction.
 *
 * Two rules run through all of them, because they are the two that cost something when forgotten:
 *
 *   - **Read before you write.**  Nearly every write tool here replaces a list rather than adding to
 *     it, so the current value is an input to the change, not a thing to be discovered afterwards.
 *   - **Say what changed, not that it worked.**  Every write tool returns what it replaced. Passing
 *     that on is the difference between an audit trail and an assurance.
 */
public class McpPrompts {

	private final List<Prompt> prompts = new ArrayList<>();

	public static class Prompt {
		public final String name;
		public final String title;
		public final String description;
		public final List<Argument> arguments;
		public final String text;

		Prompt(String name, String title, String description, List<Argument> arguments, String text) {
			this.name = name;
			this.title = title;
			this.description = description;
			this.arguments = arguments;
			this.text = text;
		}
	}

	public static class Argument {
		public final String name;
		public final String description;
		public final boolean required;

		Argument(String name, String description, boolean required) {
			this.name = name;
			this.description = description;
			this.required = required;
		}
	}

	private static Argument arg(String name, String description, boolean required) {
		return new Argument(name, description, required);
	}

	private void add(String name, String title, String description, List<Argument> arguments,
			String text) {
		prompts.add(new Prompt(name, title, description, arguments, text));
	}

	public McpPrompts() {

		add("analyse_survey_data",
				"Analyse a survey's data",
				"Work through what a survey's submissions actually say, without pulling every record "
						+ "into the conversation.",
				Arrays.asList(arg("survey", "The survey to analyse, by name or id", true),
						arg("question", "Optional. What you are trying to find out.", false)),
				"Analyse the data in the survey named in the argument.\n\n"
				+ "Work in this order, and stop at the first step that answers the question:\n\n"
				+ "1. survey_list to find the id, then survey_submission_counts for how much there "
				+ "is and over what period. A survey with eleven records does not need a method.\n"
				+ "2. survey_questions to learn what was actually asked. Do not guess column names "
				+ "from the survey name.\n"
				+ "3. **data_aggregate before data_query.** Counts and groupings are computed in the "
				+ "database and come back small. data_query returns records, is capped, and a capped "
				+ "answer read as a complete one is how a wrong total gets stated confidently.\n"
				+ "4. data_query only for the records you are going to look at individually.\n\n"
				+ "Report what the data shows and say plainly what it cannot show: how many records "
				+ "you looked at, whether that was all of them, and which questions were left blank "
				+ "often enough to matter. A distribution over a question half the respondents "
				+ "skipped is a distribution over the half who answered.\n\n"
				+ "If row filters apply to you, you are seeing your own slice. Say so rather than "
				+ "presenting it as the whole.");

		add("design_survey",
				"Design or extend a survey",
				"Add questions to a survey, or build a new one, without breaking the data already "
						+ "collected.",
				Arrays.asList(arg("survey", "The survey to change, or a name for a new one", true),
						arg("intent", "What the survey or the new questions are for", false)),
				"Design or extend the survey named in the argument.\n\n"
				+ "Before adding anything:\n\n"
				+ "1. survey_get and survey_questions, to see what is already there. Surveys collect "
				+ "the same thing twice more often than they are missing something.\n"
				+ "2. survey_submission_counts. A survey with submissions is a different job from an "
				+ "empty one: its columns exist in the results tables and its respondents have "
				+ "already answered.\n\n"
				+ "When adding:\n\n"
				+ "- survey_add_question, one at a time, checking the answer each time.\n"
				+ "- A new question is not retrospective. Every existing record will be blank for "
				+ "it, and that blank means \"never asked\", not \"declined to say\". Say so.\n"
				+ "- **Changing a question's type is the dangerous one.** Run "
				+ "survey_check_type_change first: once a column exists in the results table the "
				+ "change can cost the data in it.\n"
				+ "- survey_delete_question soft deletes. The data stays and the question can come "
				+ "back, which is worth saying to somebody who thinks they have removed something.\n\n"
				+ "Finish with survey_history so the person can see exactly what was done, and in "
				+ "which order.");

		add("data_quality_check",
				"Check a survey's data quality",
				"Look for the things that are wrong with collected data before somebody analyses it.",
				Arrays.asList(arg("survey", "The survey to check", true)),
				"Check the quality of the data in the survey named in the argument. Report; do not "
				+ "fix anything without being asked.\n\n"
				+ "Look for, in this order:\n\n"
				+ "1. **Is anything arriving at all?** survey_submission_counts over time. A survey "
				+ "that stopped receiving data three weeks ago is a bigger problem than anything "
				+ "below, and organisation_get will say whether submitting is switched off "
				+ "organisation wide.\n"
				+ "2. **Blanks.** data_aggregate grouped by each question. A question nobody "
				+ "answers is usually a question nobody understands.\n"
				+ "3. **Values that cannot be right.** Numbers outside what the question could mean, "
				+ "dates in the future, a select answer that appears once among a thousand.\n"
				+ "4. **Duplicates.** Records identical in everything a person would type.\n"
				+ "5. **Edits.** data_audit on anything that looks corrected. A record changed "
				+ "repeatedly after submission is worth a human looking at.\n\n"
				+ "Report what you found and how confident you are. \"Seventeen records have a date "
				+ "of birth after today\" is useful; \"data quality looks reasonable\" is not.\n\n"
				+ "If a fix is wanted, data_bulk_update changes many records at once and "
				+ "data_bulk_undo puts them back. Say the undo exists before making the change, not "
				+ "after.");

		add("triage_open_tasks",
				"Triage open tasks",
				"Work out what is outstanding, what is late, and who is overloaded.",
				Arrays.asList(arg("project", "Optional. Narrow to one project.", false)),
				"Triage the outstanding work.\n\n"
				+ "1. ops_status first. It is the overview, sorted with the most urgent first, and "
				+ "it answers most of this on its own.\n"
				+ "2. task_list for detail on whatever ops_status flagged.\n"
				+ "3. user_list to see who could take work on. **Somebody can only be assigned work "
				+ "in a project they belong to**, which is the reason most reassignments fail.\n\n"
				+ "When proposing changes:\n\n"
				+ "- task_action reassigns or cancels one task. There is no bulk reassign, "
				+ "deliberately.\n"
				+ "- Say who is being given what, and what they already have. Moving six tasks onto "
				+ "the person who is already behind is the obvious mistake.\n"
				+ "- New tasks come from rules, not from nowhere. workflow_list shows the rules that "
				+ "create tasks; if work should appear automatically, that is where it comes from.\n\n"
				+ "Propose before acting. Reassignment changes somebody's day.");

		add("weekly_summary",
				"Summarise the week",
				"What happened on this server in the last week, for somebody who was not watching.",
				Arrays.asList(arg("days", "Optional. How many days back. Default 7.", false)),
				"Summarise what has happened recently, over the last seven days unless the argument "
				+ "says otherwise.\n\n"
				+ "Gather:\n\n"
				+ "- event_list for what was done: surveys created and changed, users added, access "
				+ "refused, errors.\n"
				+ "- survey_submission_counts across the surveys that matter, for what came in.\n"
				+ "- ops_status for what is outstanding now.\n"
				+ "- usage_report if the question is about volume or cost. Submissions and metered "
				+ "services are different questions and are never added together.\n\n"
				+ "Write it for somebody who was away. Lead with what changed or went wrong, not "
				+ "with totals. A week in which nothing happened should say so in one line rather "
				+ "than being padded into a report.\n\n"
				+ "Where something was done by an AI client rather than by a person, say so - "
				+ "survey_history and data_audit both report which application acted, and that is "
				+ "part of what happened.");

		add("investigate_submission_failures",
				"Find out why data is not arriving",
				"Work down from the whole organisation to one device, in the order that finds it "
						+ "fastest.",
				Arrays.asList(arg("survey", "Optional. The survey data is missing from.", false)),
				"Find out why submissions are not arriving. Work outside in - each step rules out "
				+ "everything below it, and starting at the bottom is how an afternoon disappears.\n\n"
				+ "1. **organisation_get.** If submitting is switched off for the organisation, "
				+ "nothing arrives from anybody and every other explanation is wrong. Same for the "
				+ "API and notifications.\n"
				+ "2. **ops_status.** Queues backing up, errors being logged.\n"
				+ "3. **event_list**, filtered for errors and refused access. A refused submission "
				+ "is recorded; a submission nobody attempted is not, and those look identical from "
				+ "the data.\n"
				+ "4. **survey_list** - is the survey blocked, or deleted? A blocked survey refuses "
				+ "submissions and looks fine from the console.\n"
				+ "5. **survey_submission_counts** - when did it last receive anything? That date "
				+ "is usually the answer: find what changed that day.\n"
				+ "6. **user_list** - can the person submitting still reach the project? Project "
				+ "membership is what makes a survey reachable, and losing it looks exactly like a "
				+ "broken device.\n\n"
				+ "Report the first thing that explains it and stop. If nothing does, say what you "
				+ "ruled out - that is what somebody needs to carry on from where you stopped.");

		add("reorganise_projects",
				"Reorganise projects and access",
				"Move surveys between projects and give people access, in an order that does not "
						+ "lock anybody out.",
				Arrays.asList(arg("intent", "What the reorganisation is meant to achieve", true)),
				"Reorganise projects and who can reach what.\n\n"
				+ "**Order matters here, and getting it wrong locks somebody out of their own "
				+ "work.** Do it in this order:\n\n"
				+ "1. project_list and survey_list to see what exists now. user_list with a "
				+ "project_id shows who is in each.\n"
				+ "2. project_create for anything new. The person creating it becomes a member; "
				+ "nobody else does.\n"
				+ "3. **user_set_projects to put people in the new project, before moving any "
				+ "survey into it.** A survey moved into a project nobody belongs to is a survey "
				+ "nobody can reach, including the person who moved it.\n"
				+ "4. survey_set_settings to move each survey. Its data moves with it.\n"
				+ "5. project_delete for anything now empty. A project still holding surveys will "
				+ "refuse, which is the safety net, not an error.\n\n"
				+ "**user_set_projects replaces the whole list.** Read what somebody has before you "
				+ "set it, and include everything they should keep. Sending the one project you are "
				+ "adding removes every other one, and the answer will look entirely normal.\n\n"
				+ "Say what each person can reach afterwards, not just what you changed. That is the "
				+ "thing somebody will be asked about tomorrow.");

		add("onboard_user",
				"Add somebody and give them access",
				"Create a person and make their account actually usable, which takes more than "
						+ "creating it.",
				Arrays.asList(arg("who", "Their name, and their email address if they have one", true),
						arg("doing", "What they will be doing - collecting data, analysing, "
								+ "administering", false)),
				"Add somebody to this organisation and give them what they need.\n\n"
				+ "1. group_list and project_list first. Groups decide what kind of thing they may "
				+ "do; **projects decide which surveys they can reach**. Both are needed - an "
				+ "account with groups and no project can sign in and see nothing.\n"
				+ "2. user_create, with groups and projects together. Doing it in three calls "
				+ "invites the last two to be forgotten.\n"
				+ "3. If the records they should see are restricted, role_list and user_set_roles. "
				+ "A role narrows which records they see inside a survey they can already reach.\n\n"
				+ "**No email is sent.** The account exists but cannot be signed into until a "
				+ "password is set: they use the forgotten password page if they have an email "
				+ "address, or an administrator sets one in the console. Tell the person who asked "
				+ "for the account, because otherwise nothing happens and nobody knows why.\n\n"
				+ "Not every administrator may grant every group - group_list says who may grant "
				+ "each. If one is refused, the whole call is refused rather than partly applied, "
				+ "so nothing is silently missing.");
	}

	/* The listing, which is the same for everybody: a prompt is a procedure, not a permission */
	public List<Map<String, Object>> list() {

		List<Map<String, Object>> out = new ArrayList<>();
		for(Prompt p : prompts) {
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("name", p.name);
			m.put("title", p.title);
			m.put("description", p.description);

			List<Map<String, Object>> args = new ArrayList<>();
			for(Argument a : p.arguments) {
				Map<String, Object> am = new LinkedHashMap<>();
				am.put("name", a.name);
				am.put("description", a.description);
				am.put("required", a.required);
				args.add(am);
			}
			m.put("arguments", args);
			out.add(m);
		}
		return out;
	}

	public Prompt get(String name) {
		for(Prompt p : prompts) {
			if(p.name.equals(name)) {
				return p;
			}
		}
		return null;
	}

	/*
	 * One prompt, with whatever arguments were supplied appended as context.
	 *
	 * The arguments are not substituted into the text.  A procedure that reads "analyse the survey
	 * named in the argument" stays true whatever arrives, and nothing the caller sends can rewrite
	 * the instruction it is attached to - which is the whole reason not to interpolate.
	 */
	public Map<String, Object> render(Prompt p, Map<String, Object> arguments) {

		StringBuilder text = new StringBuilder(p.text);
		if(arguments != null && !arguments.isEmpty()) {
			text.append("\n\n---\nArguments given:");
			for(Argument a : p.arguments) {
				Object v = arguments.get(a.name);
				if(v != null && !v.toString().trim().isEmpty()) {
					text.append("\n- ").append(a.name).append(": ").append(v.toString().trim());
				}
			}
		}

		Map<String, Object> content = new LinkedHashMap<>();
		content.put("type", "text");
		content.put("text", text.toString());

		Map<String, Object> message = new LinkedHashMap<>();
		message.put("role", "user");
		message.put("content", content);

		List<Map<String, Object>> messages = new ArrayList<>();
		messages.add(message);

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("description", p.description);
		out.put("messages", messages);
		return out;
	}

	/* Prompt names, for completion */
	public List<String> names(String prefix) {
		List<String> out = new ArrayList<>();
		for(Prompt p : prompts) {
			if(prefix == null || prefix.isEmpty() || p.name.startsWith(prefix)) {
				out.add(p.name);
			}
		}
		return out;
	}
}
