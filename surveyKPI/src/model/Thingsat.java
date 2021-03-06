package model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

/*
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

*/

/*
 * Data object for Neo4J / thingsat export
 */
public class Thingsat {
	private static Logger log =
			 Logger.getLogger(Thingsat.class.getName());
	
	public ArrayList<Neo4J> nodes = new ArrayList<Neo4J> ();
	public ArrayList<Neo4J> links  = new ArrayList<Neo4J> ();
	
	public String filename;	
	private File f;
	private PrintWriter w;
	String filepath;
	
	public Thingsat(ThingsatDO model) {
		/*
		 * Converting the JSON model directly into a thingsat object had a problem with recursion
		 *  rather than exclude the attributes that are causing the problem the JSON is converted into Data Objects
		 *  and then these are used to build the Thingsat object
		 */
		for(Neo4JDO n : model.nodes) {
			nodes.add(new Neo4J(n, false));
		}
		for(Neo4JDO r : model.links) {
			links.add(new Neo4J(r, true));
		}
	}
	public void createDataFiles(String filepath, String filename) throws FileNotFoundException {
		
		this.filepath = filepath;
		this.filename = filename;
		log.info("Creating cql file at: " + filepath + "/" + filename + ".cql");
		f = new File(filepath, filename + ".cql");
		w = new PrintWriter(f);
		
		if(nodes != null) {
			for(Neo4J n : nodes) {
				n.createDataFile(filepath);
			}
		}
		if(links != null) {
			for(Neo4J r : links) {
				r.createDataFile(filepath);
			}
		}
		
		/*
		 * Create shell script and batch file to load cql
		 */
		File fBatch = new File(filepath, "import.sh");
		PrintWriter wBatch = new PrintWriter(fBatch);
		wBatch.println("#/bin/sh");
		wBatch.println("cd $1");
		wBatch.println("$NEO4J_HOME/bin/neo4j-shell -file import.cql");
		wBatch.close();
		
	}
	
	public void writeLoaderFile() {
		
		// Write the nodes
		for(Neo4J n : nodes) {
			w.print("load csv with headers from \"file://" + n.f.getAbsolutePath() + "\"");
			w.print(" as csvLine MERGE ( n: " + n.label.value );
			
			if(n.properties != null && n.properties.size() > 0) {
				w.print(" {");
				w.print(" id: csvLine.id");		// All nodes have id
				for(Property p : n.properties) {
					w.print(", " + p.key + ": csvLine." + p.key);
				}
				w.print(" }");
			}
			w.println(" );");
		}
		
		// Write the relations

		for(Neo4J r : links) {
			w.println("");
			w.println("using periodic commit");
			w.print("load csv with headers from \"file://" + r.f.getAbsolutePath() + "\"");
			w.println(" as csvLine ");
			w.print("   ");
			w.print("match (from: " + nodes.get(r.source).name + " { id: csvLine.from })");
			w.print(", (to: " + nodes.get(r.target).name + " { id: csvLine.to })");
			w.println(" merge (from)-[:" + r.name + "]->(to);");
			/*
			 * TODO properties
			 *
			if(n.properties != null && n.properties.size() > 0) {
				w.print("{");
				int count = 0;
				for(Property p : n.properties) {
					if(count++ > 0) {
						w.print(",");
					}
					w.print(p.key + ": csvLine." + p.key);
				}
				w.print("}");
			}
			w.println(");");
			*/
		}
	}
	
	/*
	 * Write the data for a node or relation
	 */
	public void writeData(ResultSet rs) throws SQLException {
		
		if(nodes != null) {
			for(Neo4J n : nodes) {
				n.writeData(rs, this);
			}
		}
		if(links != null) {
			for(Neo4J r : links) {
				r.writeData(rs, this);
			}
		}
		

	}
	
	public void closeFiles() throws FileNotFoundException {
		
		w.close();
		
		if(nodes != null) {
			for(Neo4J n : nodes) {
				n.closeFile();
			}
		}
		if(links != null) {
			for(Neo4J r : links) {
				r.closeFile();
			}
		}
	}
	
	public void zip() throws IOException, InterruptedException {
		int code = 0;
		
		Process proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", "zip -rj " + 
				filepath + ".zip " + filepath});
		code = proc.waitFor();
		
		if(code > 0) {
			int len;
			if ((len = proc.getErrorStream().available()) > 0) {
				byte[] buf = new byte[len];
				proc.getErrorStream().read(buf);
				log.info("Command error:\t\"" + new String(buf) + "\"");
			}
		} else {
			int len;
			if ((len = proc.getInputStream().available()) > 0) {
				byte[] buf = new byte[len];
				proc.getInputStream().read(buf);
				log.info("Completed zip process:\t\"" + new String(buf) + "\"");
			}
		}
		
        log.info("Process create zip exitValue: " + code);
	};
	
	public void localLoad() throws IOException, InterruptedException {
		int code = 0;
		
		Process proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", "chmod +x " + 
				filepath + "/import.sh "});
		code = proc.waitFor();	
		if(code > 0) {
			int len;
			if ((len = proc.getErrorStream().available()) > 0) {
				byte[] buf = new byte[len];
				proc.getErrorStream().read(buf);
				log.info("Command error:\t\"" + new String(buf) + "\"");
			}
		} else {
			int len;
			if ((len = proc.getInputStream().available()) > 0) {
				byte[] buf = new byte[len];
				proc.getInputStream().read(buf);
				log.info("Completed chmod process:\t\"" + new String(buf) + "\"");
			}
		}
        
		proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", filepath + "/import.sh " + filepath});
		code = proc.waitFor();	
		if(code > 0) {
			int len;
			if ((len = proc.getErrorStream().available()) > 0) {
				byte[] buf = new byte[len];
				proc.getErrorStream().read(buf);
				log.info("Command error:\t\"" + new String(buf) + "\"");
			}
		} else {
			int len;
			if ((len = proc.getInputStream().available()) > 0) {
				byte[] buf = new byte[len];
				proc.getInputStream().read(buf);
				log.info("Completed import process:\t\"" + new String(buf) + "\"");
			}
		}
	};
	
	public void writeDataHeaders() {
		for(Neo4J n : nodes) {				
			n.writeHeader();
		}
		for(Neo4J r : links) {
			r.writeHeader();
		}
	}
	
	public void debug() {
		log.info(" thingsat model =====================");
		log.info(" Nodes ------------------");
		if(nodes != null) {
			for (int i = 0; i < nodes.size(); i++) {
				nodes.get(i).debug();
			}
		}
		log.info(" Relations ------------------");
		if(links != null) {
			for (int i = 0; i < links.size(); i++) {
				links.get(i).debug();
			}
		}
	}
}
