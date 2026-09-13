package org.smap.sdal.Utilities;

/*
 * Reference data cached at organisation level and served to forms as a CSV
 *
 * These resources share a shape: rows live in a table in the csv schema, they belong to the
 * organisation rather than to a survey (so sId is 0), the name carries no .csv suffix, and the
 * file is served from the organisation media directory.  The only thing that differs between
 * them is where the rows came from.
 *
 * Keeping the prefixes in one place means adding a third source is a single line here rather than
 * another branch in each of the five places that ask the question.
 */
public class OrgCachedResource {

	public static final String SHAREPOINT_PREFIX = "sharepointlist_";
	public static final String DHIS2_PREFIX = "dhis2_";

	// Manifest types.  These reach FieldTask, so they are not free to change
	public static final String TYPE_SHAREPOINT = "sharepoint";
	public static final String TYPE_DHIS2 = "dhis2";

	/*
	 * True if this file name refers to an organisation level cached resource
	 */
	public static boolean isCached(String fileName) {
		if(fileName == null) {
			return false;
		}
		return fileName.startsWith(SHAREPOINT_PREFIX) || fileName.startsWith(DHIS2_PREFIX);
	}

	/*
	 * True if this manifest type is an organisation level cached resource
	 */
	public static boolean isCachedType(String type) {
		return TYPE_SHAREPOINT.equals(type) || TYPE_DHIS2.equals(type);
	}

	/*
	 * The manifest type for a file name, or null if it is not a cached resource
	 */
	public static String getType(String fileName) {
		if(fileName == null) {
			return null;
		}
		if(fileName.startsWith(SHAREPOINT_PREFIX)) {
			return TYPE_SHAREPOINT;
		}
		if(fileName.startsWith(DHIS2_PREFIX)) {
			return TYPE_DHIS2;
		}
		return null;
	}

	/*
	 * The name without a .csv suffix, which is how these are stored and how pulldata() refers to them
	 */
	public static String baseName(String fileName) {
		if(fileName == null) {
			return null;
		}
		return fileName.endsWith(".csv")
				? fileName.substring(0, fileName.length() - 4)
				: fileName;
	}

	/*
	 * The name with the source prefix removed, which is the name the mapping is stored under
	 */
	public static String withoutPrefix(String fileName) {
		String name = baseName(fileName);
		if(name == null) {
			return null;
		}
		if(name.startsWith(SHAREPOINT_PREFIX)) {
			return name.substring(SHAREPOINT_PREFIX.length());
		}
		if(name.startsWith(DHIS2_PREFIX)) {
			return name.substring(DHIS2_PREFIX.length());
		}
		return name;
	}
}
