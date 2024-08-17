package org.smap.sdal.legacy;

/*
 * Contains form information used in import / export of survey data
 */
import java.util.ArrayList;
import java.util.HashMap;

import org.smap.sdal.model.TableColumn;

public class FormDesc {
	public int f_id;
	public String name;
	public int parent;
	public String table_name;
	public String columns = null;
	public ArrayList<FormDesc> children = null;
	public ArrayList<TableColumn> columnList = null;
	public HashMap<String, String> keyMap = new HashMap<>();
	public HashMap<String, String> instanceMap = new HashMap<>();
	public HashMap<String, String> parentKeyMap = new HashMap<>();
	public FormDesc parentForm = null;
	
	public String getInstanceId(String prikey) {
		String instanceId = null;
		
		if(parentForm == null) {
			instanceId = instanceMap.get(prikey);
		} else {
			instanceId = parentForm.getInstanceId(parentKeyMap.get(prikey));
		}
		return instanceId;
	}
}
