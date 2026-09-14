package org.smap.sdal.mcp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/*
 * Where every question in a survey actually sits.
 *
 * The survey model is built for reading a form, and it reports questions in order without saying what
 * the order is made of.  Anything that rearranges a form needs the raw facts instead: which form a
 * question is in, its sequence within that form, and whether it has data behind it.
 *
 * The two containers are not alike, and this is the thing to understand before changing a form:
 *
 *   A **group** is not a form.  It is a begin group question, an end group question, and whatever
 *   happens to lie between them in sequence.  Moving a question "into a group" is moving it to a
 *   sequence inside that range - there is nothing else to set.
 *
 *   A **repeat** is a form.  Its questions have their own f_id and their own results table, so moving
 *   a question into one is moving it between forms, which is a different operation underneath even
 *   though it looks the same on screen.
 *
 * Read once and held, because resolving a move needs several questions at once and re-reading between
 * them would see a form that had already started changing.
 */
public class McpSurveyLayout {

	public static class Item {
		public int qId;
		public int fId;
		public String formName;
		public int seq;
		public String name;
		public String type;
		public boolean published;

		public boolean isGroup() {
			return "begin group".equals(type);
		}

		public boolean isRepeat() {
			return "begin repeat".equals(type)
					|| "geopolygon".equals(type) || "geolinestring".equals(type);
		}

		public boolean isEndGroup() {
			return "end group".equals(type);
		}
	}

	private final List<Item> items = new ArrayList<>();

	public McpSurveyLayout(Connection sd, int sId) throws SQLException {

		String sql = "select q.q_id, q.f_id, f.name as form_name, q.seq, q.qname, q.qtype, "
				+ "q.published "
				+ "from question q, form f "
				+ "where q.f_id = f.f_id "
				+ "and f.s_id = ? "
				+ "and not q.soft_deleted "
				+ "order by f.f_id asc, q.seq asc";

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				Item i = new Item();
				i.qId = rs.getInt("q_id");
				i.fId = rs.getInt("f_id");
				i.formName = rs.getString("form_name");
				i.seq = rs.getInt("seq");
				i.name = rs.getString("qname");
				i.type = rs.getString("qtype");
				i.published = rs.getBoolean("published");
				items.add(i);
			}
		}
	}

	public List<Item> all() {
		return items;
	}

	/* By name, which is unique within a survey for anything not soft deleted */
	public Item find(String name) {
		if(name == null) {
			return null;
		}
		for(Item i : items) {
			if(name.equalsIgnoreCase(i.name)) {
				return i;
			}
		}
		return null;
	}

	public List<Item> inForm(int fId) {
		List<Item> out = new ArrayList<>();
		for(Item i : items) {
			if(i.fId == fId) {
				out.add(i);
			}
		}
		return out;
	}

	/*
	 * The end group that closes this one, found by nesting rather than by name.
	 *
	 * Names go out of step - a group renamed in the editor leaves its end group called after the old
	 * name - so counting depth is the only reliable way, and it is what QuestionManager does when it
	 * deletes one.  Null when the group has no end, which real forms do contain.
	 */
	public Item endOf(Item group) {
		if(group == null || !group.isGroup()) {
			return null;
		}
		int depth = 0;
		for(Item i : inForm(group.fId)) {
			if(i.seq < group.seq) {
				continue;
			}
			if(i.isGroup()) {
				depth++;
			} else if(i.isEndGroup()) {
				depth--;
				if(depth == 0) {
					return i;
				}
				if(depth < 0) {
					return null;		// Closes something enclosing this, so this group has no end
				}
			}
		}
		return null;
	}

	/* Everything between a group's begin and its end, not including either */
	public List<Item> contentsOf(Item group) {
		List<Item> out = new ArrayList<>();
		Item end = endOf(group);
		if(end == null) {
			return out;
		}
		for(Item i : inForm(group.fId)) {
			if(i.seq > group.seq && i.seq < end.seq) {
				out.add(i);
			}
		}
		return out;
	}

	/*
	 * The group a question is inside, or null when it is at the top level of its form.  The innermost
	 * one, since groups nest.
	 */
	public Item groupContaining(Item q) {
		Item found = null;
		for(Item i : inForm(q.fId)) {
			if(i.isGroup() && i.seq < q.seq) {
				Item end = endOf(i);
				if(end != null && end.seq > q.seq) {
					if(found == null || i.seq > found.seq) {
						found = i;
					}
				}
			}
		}
		return found;
	}

	/* The highest sequence in a form, so something can be put after everything else */
	public int lastSeq(int fId) {
		int max = -1;
		for(Item i : inForm(fId)) {
			if(i.seq > max) {
				max = i.seq;
			}
		}
		return max;
	}
}
