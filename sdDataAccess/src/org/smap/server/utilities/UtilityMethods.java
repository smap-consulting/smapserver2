package org.smap.server.utilities;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.smap.model.FormDesc;
import org.smap.model.IE;
import org.smap.model.SurveyTemplate;
import org.smap.model.TableManager;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.NodesetFormDetails;
import org.smap.sdal.model.Search;
import org.smap.server.entities.Option;
import org.smap.server.entities.Question;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class UtilityMethods {

	private static Logger log = Logger.getLogger(GetHtml.class.getName());
	
	public static String getLastFromPath(String path) {
		if(path != null) {
			int startIdx = path.lastIndexOf('/') + 1;
			return path.substring(startIdx);
		} else {
			return null;
		}
	}
	
	public static String getGroupFromPath(String path) {
		if(path != null) {
			int startIdx = 0;
			if(path.startsWith("/main")) {
				startIdx = 5;
			}
			
			int endIdx = path.lastIndexOf('/');
			if(endIdx > startIdx) {
				return path.substring(startIdx, endIdx);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}
	
	static String getFirstFromPath(String path) {
		String firstForm = null;
		
		try {
			int startIdx = path.indexOf('/') + 1;
			int lastIdx = path.indexOf('/', startIdx);
			firstForm = path.substring(startIdx, lastIdx);
		} catch (StringIndexOutOfBoundsException e) {		
			
		}
		
		return firstForm; 
	}
	
	static String getQuotedText(String text) {
		int startIdx = text.indexOf('\'') + 1;
    	int endIdx = text.lastIndexOf('\'');
    	return text.substring(startIdx, endIdx);
	}
	
	// XML Parsing methods
	
    /*
     * Method to get the first child element
     * 
     * @param n the node
     * @return Node the first element node
     */
    static Node getFirstElement(Node n) {
    	Node firstElement = null;
    	NodeList children = n.getChildNodes();
    	for(int i = 0; i < children.getLength(); i++) {
    		if(children.item(i).getNodeType() == Node.ELEMENT_NODE) {
    			firstElement = children.item(i);
    		}
    	}
    	return firstElement;
    }
    
    static boolean hasChildElement(Node n) {
    	boolean result = false;
    	
    	NodeList eList = n.getChildNodes();
    	if (eList != null) {
			for (int i = 0; i < eList.getLength(); i++) {
				if(eList.item(i).getNodeType() == Node.ELEMENT_NODE) {
					result = true;
					break;
				}
			}
		}
    	
    	return result;
    }   
	
	/*
	 * TODO replace the next two functions by a single function that sorts any object that
	 * implements the appropriate interface
	 */
	static public void sortElements(List <IE> in) {
		
		java.util.Collections.sort(in, new Comparator<IE>() {
			@Override
			public int compare(IE object1, IE object2) {
				if (object1.getSeq() < object2.getSeq())
					return -1;
				else if (object1.getSeq() == object2.getSeq())
					return 0;
				else
					return 1;
			}
		});
	}
	
	static public void sortOptions(List <Option> in) {
		
		java.util.Collections.sort(in, new Comparator<Option>() {
			@Override
			public int compare(Option object1, Option object2) {
				if (object1.getSeq() < object2.getSeq())
					return -1;
				else if (object1.getSeq() == object2.getSeq())
					return 0;
				else
					return 1;
			}
		});
	}
	
	/*
	 * Convert names in xls format ${ } to xPath
	 */
	public static String convertAllxlsNames(
			String input, 
			boolean forLabel,
			HashMap<String, String> questionPaths,
			int f_id,
			boolean webform,
			String calledForQuestion,
			boolean relativePath) throws Exception {
		
		if(input == null) {
			return input;
		}
		
		StringBuffer output = new StringBuffer("");
		
		Pattern pattern = Pattern.compile("\\$\\{.+?\\}");
		java.util.regex.Matcher matcher = pattern.matcher(input);
		int start = 0;
		while (matcher.find()) {
			
			String matched = matcher.group();
			String qname = matched.substring(2, matched.length() - 1);
			
			// Add any text before the match
			int startOfGroup = matcher.start();
			output.append(input.substring(start, startOfGroup));
			
			// If for a label, add the wrapping html
			if(forLabel) {
				if(webform) {
					output.append("<span class=\"or-output\" data-value=\""); 
				} else {
					output.append("<output value=\"");
				}
			}
			
			// Make sure there is a space before the match
			if(output.length() > 0 && output.charAt(output.length() - 1) != ' ') {
				output.append(' ');
			}
			
			String qPath = null;
			if(qname.equals("the_geom")) {
				qPath = questionPaths.get(qname + f_id);
			} else {
				qPath = questionPaths.get(qname);
			}
			 			
			if(qPath == null) {
				throw new ApplicationException("Question path not found for question: " + qname + " in " + input + 
							" of " + calledForQuestion);

			
			} else if(relativePath && calledForQuestion != null && questionPaths.get(calledForQuestion) != null) {

				/*
				 * Return relative path if requested
				 * This is the relative path from the calledFromQuestion to the qPath
				 * Only used in fieldTask not Webforms - tentative
				 * 
				 * Examples:
				 *   called from:     /x1/x2/x3/qCalledFrom
				 *   qPath:           /x1/q
				 *   relative qPath:  current()/../../../q
				 *   
				 *   called from:     /x1/qCalledFrom
				 *   qPath:           /x1/x2/x3/q
				 *   relative qPath:  current()/../x2/x3/q
				 *   
				 *   called from:     /x1/qCalledFrom
				 *   qPath:           /x1/q
				 *   relative qPath:  current()/../q
				 */
				
				ArrayList<String> relPath = new ArrayList<> ();
				
				String cfPath = questionPaths.get(calledForQuestion);
				
				// Remove the first slash
				cfPath = cfPath.substring(1);
				qPath = qPath.substring(1);
				
				String [] cfSteps = cfPath.trim().split("/");
				String [] qSteps = qPath.trim().split("/");
				
				int idx;
				log.info("xxxxx: " + cfPath + " : " + qPath);
				
				for(idx = 0; idx < qSteps.length && idx < cfSteps.length && qSteps[idx].equals(cfSteps[idx]); idx++) {					
					relPath.add(qSteps[idx]);
				}
				int pathDepth = cfSteps.length - idx;
				
				StringBuffer path = new StringBuffer("");
				if(!webform) {
					path = path.append("current()");
				} 
				if(pathDepth == 0 && webform) {
					path.append(".")
;				}
				for(int i = 0; i < pathDepth; i++) {
					if(webform && i == 0) {
						path.append("..");
					} else {
						path.append("/..");
					}
				}
				for(int i = idx; i < qSteps.length; i++) {
					path.append("/").append(qSteps[i]);
				}
				qPath = path.toString();
				log.info("------------- Relative Path: " + qPath);
			}
			
			output.append(qPath);

			
			// If for a label close the wrapping html
			if(forLabel) {
				if(webform) {
					output.append(" \"/></span>");
				} else {
					output.append(" \"/>");
				}
			}
			
			// Reset the start
			start = matcher.end();

			// Make sure there is a space after the match or its the end of the string
			if(start < input.length()) {
				if(input.charAt(start) != ' ') {
					output.append(' ');
				}
			}
						
		}
		
		// Get the remainder of the string
		if(start < input.length()) {
			output.append(input.substring(start));	
		}
		
		return output.toString().trim();
	}
	
	/*
	 * Get a nodeset from the nodeset value stored in the database and the appearance of a question
	 */
    public static String getNodeset(boolean convertToXPath, 
    		boolean convertToXLSName, 
    		HashMap<String, String> questionPaths, 
    		boolean embedExternalSearch,
    		String nodeset,
    		String appearance,
    		int f_id,
    		String qName,
    		boolean relativePath) throws Exception {
		
		String out = nodeset;
		
		if(embedExternalSearch) {
			
			// Potentially add a filter using the appearance value to the nodeset
			Search search = GeneralUtilityMethods.getSearchFiltersFromAppearance(appearance);

			if(search.filters.size() > 0) {
				log.info("Add filter from: " + appearance + " to: " + nodeset);

				if(out != null) {
					// First remove any filter added through setting of choice_filter this is incompatible with the use of search()
					int idx = out.indexOf('[');
					if (idx >= 0) {
						out = out.substring(0, idx);
					}

					int count = 0;
					out += "[ ";
					for(KeyValueSimp kv : search.filters) {
						
						if(count++ > 0) {
							out += " and ";
						}
						
						if(count == 1 && search.fn.equals("in")) {	// Second filter is always 'matches'
							if(kv.v.trim().startsWith("${")) {				
								out += "selected(" + kv.v +", " + kv.k + ")"; 	// A question
							} else {
								out += "selected('" + kv.v +"', " + kv.k + ")";	// A string
							}
						} else if(count == 1 && search.fn.equals("not in")) {
							if(kv.v.trim().startsWith("${")) {				
								out += "not(selected(" + kv.v +", " + kv.k + "))"; 	// A question
							} else {
								out += "not(selected('" + kv.v +"', " + kv.k + "))";	// A string
							}
						} else if(count == 1 && search.fn.equals("endswith")) {
							if(kv.v.trim().startsWith("${")) {
								out += "ends-with(" + kv.k +", " + kv.v + ")";
							} else {
								out += "ends-with(" + kv.k +", '" + kv.v + "')";
							}
						} else if(count == 1 && search.fn.equals("startswith")) {
							if(kv.v.trim().startsWith("${")) {
								out += "starts-with(" + kv.k +", " + kv.v + ")";
							} else {
								out += "starts-with(" + kv.k +", '" + kv.v + "')";
							}
						} else if(count == 1 && search.fn.equals("contains")) {
							if(kv.v.trim().startsWith("${")) {
								out += "contains(" + kv.k +", " + kv.v + ")";
							} else {
								out += "contains(" + kv.k +", '" + kv.v + "')";
							}
						} else {	// Assume matches
							if(kv.v.trim().startsWith("${")) {
								out += kv.k + " = " + kv.v ;			// A question
							} else {
								out += kv.k + " = '" + kv.v + "'";		// A string
							}
						}
						
					}
					out += " ]";
				}
			}
	
		}		
		
		if(convertToXPath) {
			out = convertAllxlsNames(out, false, questionPaths, f_id, false, qName, relativePath);
		} else if(convertToXLSName) {
			out = GeneralUtilityMethods.convertAllXpathNames(out, true);
		}
		
		return out;
	}
    
    /*
     * Get a repeat nodeset
     */
    public static String getRepeatNodeset(SurveyTemplate template, HashMap<String, String> formRefs, 
    		HashMap<String, String> questionPaths, int formId, 
    		String qNodeset) throws Exception {
    	
    	String nodeset = null;
    	
    	int idx = qNodeset.indexOf('[');
    	if(idx > 0) {
			
    		String repQuestionXLS = qNodeset.substring(0, idx).trim();
			String filter = qNodeset.substring(idx);
    	

			NodesetFormDetails formDetails = getFormDetails(template, formRefs, repQuestionXLS, questionPaths, formId);
			nodeset = formDetails.formRef + UtilityMethods.convertAllxlsNames(filter, false, questionPaths,  
				formId, true, formDetails.formName, true);
    	}
    	return nodeset;
    }
    
    public static NodesetFormDetails getFormDetails(SurveyTemplate template, HashMap<String, String> formRefs, String repQuestionXLS, HashMap<String, String> questionPaths, int formId) throws Exception {
    	NodesetFormDetails details = new NodesetFormDetails();
		
		String repQuestionName = GeneralUtilityMethods.getNameFromXlsName(repQuestionXLS);
		String repQuestionPath = UtilityMethods.convertAllxlsNames(repQuestionXLS, false, questionPaths,  
				formId, false, null, false);
		
		if(template != null) {
			// Use the template to get the path of the repeat question (presumably called from get xform)
			Question repQuestion = template.getQuestion(repQuestionPath);
			details.formRef = repQuestion.getFormRef();
		} else {
			// Presumably called from getHtml
			details.formRef = formRefs.get(repQuestionName);
		}
		
		details.formRef = details.formRef.trim();
		if(details.formRef.charAt(details.formRef.length() - 1) == '/') {
			details.formRef = details.formRef.substring(0, details.formRef.length() - 1);
		}
		int idx2 = details.formRef.lastIndexOf('/');
		details.formName = null;
		if(idx2 > 0) {
			details.formName = details.formRef.substring(idx2 + 1);
		}
		
    	return details;
    }
    
	/*
	 * Create the results tables if they do not exist
	 */
	public static void createSurveyTables(Connection sd, Connection results, 
			ResourceBundle localisation, 
			int sId,
			ArrayList<FormDesc> formList,
			String sIdent,
			String tz) throws Exception {
		
		TableManager tm = new TableManager(localisation, tz);
		FormDesc topForm = formList.get(0);
		
		SurveyTemplate template = new SurveyTemplate(localisation); 
		template.readDatabase(sd, results, sIdent, false);	
		ArrayList<String> tablesCreated = tm.writeAllTableStructures(sd, results, sId, template,  0);
		
		boolean tableChanged = false;
		boolean tablePublished = false;
	
		// Apply any updates that have been made to the table structure since the last submission
		// If a table was newly created then just mark the changes as done	
		tableChanged = tm.applyTableChanges(sd, results, sId, tablesCreated);
	
		// Add any previously unpublished columns not in a changeset (Occurs if this is a new survey sharing an existing table)
		tablePublished = tm.addUnpublishedColumns(sd, results, sId, topForm.table_name);			
		if(tableChanged || tablePublished) {
			for(FormDesc f : formList) {
				tm.markPublished(sd, f.f_id, sId);		// only mark published if there have been changes made
			}
		}
	}
	
}
