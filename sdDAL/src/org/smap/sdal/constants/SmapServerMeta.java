package org.smap.sdal.constants;

public class SmapServerMeta {


	public static final int UPLOAD_TIME_ID = -100;		// Pseudo question id for upload time
	public static final int SCHEDULED_START_ID = -101;		// Pseudo question id for scheduled start
	
	public static final String UPLOAD_TIME_NAME = "_upload_time"; 
	public static final String SCHEDULED_START_NAME = "_scheduled_start"; 

	public static boolean isServerMeta(String name) {
		boolean answer = false;
		if (name.equals("_hrk") 
				|| name.equals("_device") 
				|| name.equals("_user")
				|| name.equals("_start") 
				|| name.equals("_end") 
				|| name.equals(SmapServerMeta.UPLOAD_TIME_NAME)
				|| name.equals(SmapServerMeta.SCHEDULED_START_NAME)
				|| name.equals("_survey_notes")) {
			answer = true;
		}
		return answer;
	}
	
}
