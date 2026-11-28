package org.smap.sdal.formexpressions;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;

/*
 * The functions and names form expressions can use, read from form-expressions.json.  The same file holds the text
 * of the expressions page in the documentation, which is generated from it.
 */
public class FormExpressionSpec {

	public static class Function {
		public String name;
		public int minArgs;
		public Integer maxArgs;			// null for no limit
		public String origin;			// xpath, odk or smap
		public List<String> engines;	// fieldTask, webform
		public String note;
	}

	public List<String> meta = new ArrayList<> ();
	public List<Function> functions = new ArrayList<> ();

	private transient HashMap<String, Function> byName;

	private static volatile FormExpressionSpec spec;

	public static FormExpressionSpec get() {
		FormExpressionSpec s = spec;
		if(s == null) {
			try (Reader r = new InputStreamReader(FormExpressionSpec.class.getResourceAsStream(
					"/org/smap/sdal/resources/form-expressions.json"), StandardCharsets.UTF_8)) {
				s = new Gson().fromJson(r, FormExpressionSpec.class);
			} catch (Exception e) {
				throw new RuntimeException("Cannot read form-expressions.json", e);
			}
			s.byName = new HashMap<> ();
			for(Function f : s.functions) {
				s.byName.put(f.name, f);
			}
			spec = s;
		}
		return s;
	}

	public Function getFunction(String name) {
		return byName.get(name);
	}

	/*
	 * The known function most like a name that is not one, or null
	 */
	public String closest(String name) {
		String best = null;
		int bestDist = 3;
		for(Function f : functions) {
			int d = f.name.equalsIgnoreCase(name) ? 0 : distance(f.name, name.toLowerCase());
			if(d < bestDist) {
				bestDist = d;
				best = f.name;
			}
		}
		return best;
	}

	private int distance(String a, String b) {
		int [] prev = new int[b.length() + 1];
		int [] cur = new int[b.length() + 1];
		for(int j = 0; j <= b.length(); j++) prev[j] = j;
		for(int i = 1; i <= a.length(); i++) {
			cur[0] = i;
			for(int j = 1; j <= b.length(); j++) {
				int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
				cur[j] = Math.min(Math.min(cur[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
			}
			int [] t = prev; prev = cur; cur = t;
		}
		return prev[b.length()];
	}
}
