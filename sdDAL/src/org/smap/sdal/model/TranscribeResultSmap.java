package org.smap.sdal.model;

import java.util.ArrayList;

public class TranscribeResultSmap {
	
	public class Results {
		
		public class Item {
			public String start_time;
			public String end_time;
		}
		
		public class Transcript {
			public String transcript;
		}
		
		public ArrayList<Transcript> transcripts;
		public ArrayList<Item> items;
	}
	
	public Results results;
}
