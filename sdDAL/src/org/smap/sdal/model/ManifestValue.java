package org.smap.sdal.model;

import java.io.File;

/*
 * Form Class
 * Stores details on a manifest item
 */
public class ManifestValue {
	public String value;
	public String fileName;
	public String baseName;
	public String url;
	public String thumbsUrl;
	public File file;
	public String filePath;
	public String type;
	public String text_id;
	public String linkedSurveyIdent;	// The ident of the survey being linked to
	public int sId;						// The id of the survey doign the linking
	public int qId;
	public int oId;
	public int linkedRecords;	// The number of records in a linked form
}
