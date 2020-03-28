package org.smap.sdal.model;

import java.util.ArrayList;

public class TranscribeResultSmap {
	
	public class Results {
		
		public class Transcript {
			public String transcript;
		}
		
		public ArrayList<Transcript> transcripts;
	}
	
	public Results results;
}
