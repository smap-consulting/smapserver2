package org.smap.sdal.formexpressions;

import java.util.ArrayList;
import java.util.List;

/*
 * Reads the expressions used in survey questions: relevant, constraint, calculation, choice filter, required and
 * read only.  These are XPath 1.0 with the additions of ODK and Smap, chiefly ${name} for a question and #{name}
 * for a column of a csv file.  The grammar, followed rule for rule by the methods below:
 *
 *   Expr           = OrExpr
 *   OrExpr         = AndExpr ("or" AndExpr)*
 *   AndExpr        = EqualityExpr ("and" EqualityExpr)*
 *   EqualityExpr   = RelationalExpr (("=" | "!=") RelationalExpr)*
 *   RelationalExpr = AdditiveExpr (("<" | "<=" | ">" | ">=") AdditiveExpr)*
 *   AdditiveExpr   = MultiplyExpr (("+" | "-") MultiplyExpr)*
 *   MultiplyExpr   = UnaryExpr (("*" | "div" | "mod") UnaryExpr)*
 *   UnaryExpr      = "-"* UnionExpr
 *   UnionExpr      = PathExpr ("|" PathExpr)*
 *   PathExpr       = LocationPath | FilterExpr (("/" | "//") RelativePath)?
 *   FilterExpr     = Primary Predicate*
 *   Primary        = "(" Expr ")" | Number | String | "${" name "}" | "#{" name "}" | "$" Name | FunctionCall
 *   FunctionCall   = Name "(" (Expr ("," Expr)*)? ")"
 *   LocationPath   = ("/" | "//") RelativePath? | RelativePath
 *   RelativePath   = Step (("/" | "//") Step)*
 *   Step           = "." | ".." | ("@" | Name "::")? (Name | "*" | NodeType "(" ")") Predicate*
 *   Predicate      = "[" Expr "]"
 *
 * A word is an operator (and, or, div, mod) and "*" multiplies when they come where an operator is expected, as
 * in XPath.  Problems are reported with the character they are at, in words a survey designer can act on.
 */
public class FormExpressionParser {

	private static final int NUM = 1, STR = 2, QREF = 3, CREF = 4, VAR = 5, NAME = 6, SYM = 7, END = 8;

	private static final String [] NODE_TYPES = {"node", "text", "comment", "processing-instruction"};

	private static class Token {
		int kind;
		String text;
		int pos;
		Token(int kind, String text, int pos) {
			this.kind = kind;
			this.text = text;
			this.pos = pos;
		}
	}

	private String expr;
	private List<Token> tokens;
	private int idx;

	/*
	 * Parse an expression, throwing a FormExpressionException that says what is wrong and where
	 */
	public ExprNode parse(String expression) throws FormExpressionException {
		expr = expression;
		tokens = tokenise(expression);
		idx = 0;
		if(peek().kind == END) {
			throw new FormExpressionException("fe_empty", -1);
		}
		ExprNode n = orExpr();
		if(peek().kind != END) {
			throw unexpected(peek());
		}
		return n;
	}

	/*
	 * ======================= Tokens
	 */
	private List<Token> tokenise(String s) throws FormExpressionException {
		List<Token> out = new ArrayList<> ();
		int i = 0;
		int n = s.length();
		while(i < n) {
			char c = s.charAt(i);
			if(Character.isWhitespace(c)) {
				i++;
			} else if(Character.isDigit(c) || (c == '.' && i + 1 < n && Character.isDigit(s.charAt(i + 1)))) {
				int start = i;
				while(i < n && Character.isDigit(s.charAt(i))) i++;
				if(i < n && s.charAt(i) == '.') {
					i++;
					while(i < n && Character.isDigit(s.charAt(i))) i++;
				}
				out.add(new Token(NUM, s.substring(start, i), start));
			} else if(c == '\'' || c == '"') {
				int end = s.indexOf(c, i + 1);
				if(end < 0) {
					throw new FormExpressionException("fe_quote", i, String.valueOf(c));
				}
				out.add(new Token(STR, s.substring(i + 1, end), i));
				i = end + 1;
			} else if(c == '‘' || c == '’' || c == '“' || c == '”') {
				throw new FormExpressionException("fe_curly", i, String.valueOf(c));
			} else if((c == '$' || c == '#') && i + 1 < n && s.charAt(i + 1) == '{') {
				int end = s.indexOf('}', i + 2);
				if(end < 0) {
					throw new FormExpressionException("fe_brace", i, c + "{");
				}
				String name = s.substring(i + 2, end).trim();
				if(name.length() == 0) {
					throw new FormExpressionException("fe_brace_empty", i, c + "{}");
				}
				out.add(new Token(c == '$' ? QREF : CREF, name, i));
				i = end + 1;
			} else if(c == '$' && i + 1 < n && isNameStart(s.charAt(i + 1))) {
				int start = i++;
				while(i < n && isNameChar(s.charAt(i))) i++;
				out.add(new Token(VAR, s.substring(start + 1, i), start));
			} else if(isNameStart(c)) {
				int start = i;
				while(i < n && isNameChar(s.charAt(i))) i++;
				// A prefix such as jr: in jr:choice-name, but not the axis separator ::
				if(i + 1 < n && s.charAt(i) == ':' && s.charAt(i + 1) != ':' && isNameStart(s.charAt(i + 1))) {
					i++;
					while(i < n && isNameChar(s.charAt(i))) i++;
				}
				out.add(new Token(NAME, s.substring(start, i), start));
			} else {
				String two = i + 1 < n ? s.substring(i, i + 2) : "";
				if(two.equals("==") || two.equals("&&") || two.equals("||") || two.equals("<>")) {
					String instead = two.equals("==") ? "=" : two.equals("&&") ? "and" : two.equals("||") ? "or" : "!=";
					throw new FormExpressionException("fe_op", i, two, instead);
				}
				if(two.equals("//") || two.equals("!=") || two.equals("<=") || two.equals(">=") || two.equals("::")
						|| two.equals("..")) {
					out.add(new Token(SYM, two, i));
					i += 2;
				} else if("/()[],|+-*=<>@.".indexOf(c) >= 0) {
					out.add(new Token(SYM, String.valueOf(c), i));
					i++;
				} else {
					throw new FormExpressionException("fe_char", i, String.valueOf(c));
				}
			}
		}
		out.add(new Token(END, "", n));
		return out;
	}

	private boolean isNameStart(char c) {
		return Character.isLetter(c) || c == '_';
	}

	private boolean isNameChar(char c) {
		return Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.';
	}

	/*
	 * ======================= Grammar
	 */
	private ExprNode orExpr() throws FormExpressionException {
		ExprNode left = andExpr();
		while(isWord("or")) {
			Token t = next();
			left = binary(t, left, andExpr());
		}
		return left;
	}

	private ExprNode andExpr() throws FormExpressionException {
		ExprNode left = equalityExpr();
		while(isWord("and")) {
			Token t = next();
			left = binary(t, left, equalityExpr());
		}
		return left;
	}

	private ExprNode equalityExpr() throws FormExpressionException {
		ExprNode left = relationalExpr();
		while(isSym("=") || isSym("!=")) {
			Token t = next();
			left = binary(t, left, relationalExpr());
		}
		return left;
	}

	private ExprNode relationalExpr() throws FormExpressionException {
		ExprNode left = additiveExpr();
		while(isSym("<") || isSym("<=") || isSym(">") || isSym(">=")) {
			Token t = next();
			left = binary(t, left, additiveExpr());
		}
		return left;
	}

	private ExprNode additiveExpr() throws FormExpressionException {
		ExprNode left = multiplyExpr();
		while(isSym("+") || isSym("-")) {
			Token t = next();
			left = binary(t, left, multiplyExpr());
		}
		return left;
	}

	private ExprNode multiplyExpr() throws FormExpressionException {
		ExprNode left = unaryExpr();
		while(isSym("*") || isWord("div") || isWord("mod")) {
			Token t = next();
			left = binary(t, left, unaryExpr());
		}
		return left;
	}

	private ExprNode unaryExpr() throws FormExpressionException {
		if(isSym("-")) {
			Token t = next();
			return new ExprNode(ExprNode.NEG, "-", t.pos).add(unaryExpr());
		}
		return unionExpr();
	}

	private ExprNode unionExpr() throws FormExpressionException {
		ExprNode left = pathExpr();
		while(isSym("|")) {
			Token t = next();
			left = binary(t, left, pathExpr());
		}
		return left;
	}

	private ExprNode pathExpr() throws FormExpressionException {
		Token t = peek();
		if(isSym("/") || isSym("//")) {
			next();
			ExprNode path = new ExprNode(ExprNode.PATH, null, t.pos);
			if(startsStep()) {
				relativePath(path);
			} else if(t.text.equals("//")) {
				throw expected("fe_x_step", peek());
			}
			return finishPath(path, t.pos);
		}
		if(startsStep()) {
			ExprNode path = new ExprNode(ExprNode.PATH, null, t.pos);
			relativePath(path);
			return finishPath(path, t.pos);
		}
		ExprNode primary = filterExpr();
		if(isSym("/") || isSym("//")) {
			ExprNode path = new ExprNode(ExprNode.PATH, null, t.pos).add(primary);
			next();
			if(!startsStep()) {
				throw expected("fe_x_step", peek());
			}
			relativePath(path);
			return finishPath(path, t.pos);
		}
		return primary;
	}

	private ExprNode finishPath(ExprNode path, int start) {
		path.value = expr.substring(start, peek().pos).trim();
		return path;
	}

	private ExprNode filterExpr() throws FormExpressionException {
		ExprNode n = primary();
		if(isSym("[")) {
			ExprNode filtered = new ExprNode(ExprNode.PATH, null, n.position).add(n);
			while(isSym("[")) {
				predicate(filtered);
			}
			return finishPath(filtered, n.position);
		}
		return n;
	}

	private ExprNode primary() throws FormExpressionException {
		Token t = peek();
		switch(t.kind) {
		case NUM:
			next();
			return new ExprNode(ExprNode.NUMBER, t.text, t.pos);
		case STR:
			next();
			return new ExprNode(ExprNode.STRING, t.text, t.pos);
		case QREF:
			next();
			return new ExprNode(ExprNode.QUESTION, t.text, t.pos);
		case CREF:
			next();
			return new ExprNode(ExprNode.COLUMN, t.text, t.pos);
		case VAR:
			next();
			return new ExprNode(ExprNode.VARIABLE, t.text, t.pos);
		case NAME:
			if(isSymAt(idx + 1, "(")) {
				next();
				next();
				ExprNode fn = new ExprNode(ExprNode.FUNCTION, t.text, t.pos);
				if(!isSym(")")) {
					fn.add(orExpr());
					while(isSym(",")) {
						next();
						if(isSym(")")) {
							throw new FormExpressionException("fe_x_arg", peek().pos, t.text);
						}
						fn.add(orExpr());
					}
				}
				if(!isSym(")") && startsOperand()) {
					// Two arguments with nothing between them
					throw new FormExpressionException("fe_x_comma", peek().pos, describe(peek()));
				}
				expect(")", "fe_x_close_fn", t.text);
				return fn;
			}
			break;
		case END:
			throw new FormExpressionException("fe_end", t.pos);
		default:
			if(isSym("(")) {
				next();
				if(isSym(")")) {
					throw unexpected(peek());
				}
				ExprNode g = new ExprNode(ExprNode.GROUP, "(", t.pos).add(orExpr());
				expect(")", "fe_x_close", "(");
				return g;
			}
		}
		throw unexpected(t);
	}

	/*
	 * Whether the next token could begin a value, so that it cannot follow another value
	 */
	private boolean startsOperand() {
		Token t = peek();
		return t.kind == NUM || t.kind == STR || t.kind == QREF || t.kind == CREF || t.kind == VAR
				|| (t.kind == NAME && !isWord("and") && !isWord("or") && !isWord("div") && !isWord("mod"))
				|| isSym("(") || isSym(".") || isSym("..") || isSym("@");
	}

	private void relativePath(ExprNode path) throws FormExpressionException {
		step(path);
		while(isSym("/") || isSym("//")) {
			next();
			if(!startsStep()) {
				throw expected("fe_x_step", peek());
			}
			step(path);
		}
	}

	private boolean startsStep() {
		Token t = peek();
		if(t.kind == SYM) {
			return t.text.equals(".") || t.text.equals("..") || t.text.equals("@") || t.text.equals("*");
		}
		if(t.kind == NAME) {
			if(isSymAt(idx + 1, "(")) {
				return isNodeType(t.text);
			}
			return true;
		}
		return false;
	}

	private void step(ExprNode path) throws FormExpressionException {
		if(isSym(".") || isSym("..")) {
			next();
			return;		// Not followed by predicates in XPath 1.0
		}
		if(isSym("@")) {
			next();
		} else if(peek().kind == NAME && isSymAt(idx + 1, "::")) {
			next();
			next();
		}
		Token t = peek();
		if(isSym("*")) {
			next();
		} else if(t.kind == NAME) {
			next();
			if(isNodeType(t.text) && isSym("(")) {
				next();
				expect(")", "fe_x_close_fn", t.text);
			}
		} else {
			throw expected("fe_x_step", t);
		}
		while(isSym("[")) {
			predicate(path);
		}
	}

	private void predicate(ExprNode path) throws FormExpressionException {
		next();
		if(isSym("]")) {
			throw unexpected(peek());
		}
		path.add(orExpr());
		expect("]", "fe_x_close", "[");
	}

	/*
	 * ======================= Helpers
	 */
	private ExprNode binary(Token op, ExprNode left, ExprNode right) {
		return new ExprNode(ExprNode.OP, op.text, op.pos).add(left).add(right);
	}

	private boolean isNodeType(String name) {
		for(String n : NODE_TYPES) {
			if(n.equals(name)) {
				return true;
			}
		}
		return false;
	}

	private Token peek() {
		return tokens.get(idx);
	}

	private Token next() {
		return tokens.get(idx++);
	}

	private boolean isSym(String s) {
		return isSymAt(idx, s);
	}

	private boolean isSymAt(int i, String s) {
		Token t = tokens.get(Math.min(i, tokens.size() - 1));
		return t.kind == SYM && t.text.equals(s);
	}

	private boolean isWord(String w) {
		Token t = peek();
		return t.kind == NAME && t.text.equals(w);
	}

	private void expect(String sym, String key, String param) throws FormExpressionException {
		if(!isSym(sym)) {
			Token t = peek();
			throw new FormExpressionException(key, t.pos, param, describe(t));
		}
		next();
	}

	private FormExpressionException expected(String key, Token t) {
		return new FormExpressionException(key, t.pos, describe(t));
	}

	private FormExpressionException unexpected(Token t) {
		if(t.kind == END) {
			return new FormExpressionException("fe_end", t.pos);
		}
		return new FormExpressionException("fe_unexpected", t.pos, describe(t));
	}

	/*
	 * A token as the person wrote it
	 */
	private String describe(Token t) {
		switch(t.kind) {
		case END: return "";
		case STR: return expr.substring(t.pos, Math.min(expr.length(), t.pos + t.text.length() + 2));
		case QREF: return "${" + t.text + "}";
		case CREF: return "#{" + t.text + "}";
		case VAR: return "$" + t.text;
		default: return t.text;
		}
	}
}
