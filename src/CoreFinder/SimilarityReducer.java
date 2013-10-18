package CoreFinder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Record;


public class SimilarityReducer extends Reducer<Text, Text, Text, Text> {
	// FIXME output is always 1!
	private double similarity (String v1, String v2) {
		// Get info on the 1st vertex
		int v1_size = 0;
		ArrayList<String> vertex1 = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(v1);
		while (tokenizer.hasMoreTokens()) {
			++v1_size;
			vertex1.add(tokenizer.nextToken());			
		}
		// Get info on the 2nd vertex
		int v2_size = 0;
		ArrayList<String> vertex2 = new ArrayList<String>();
		tokenizer = new StringTokenizer(v2);
		while (tokenizer.hasMoreTokens()) {
			++v2_size;
			vertex2.add(tokenizer.nextToken());			
		}
		vertex1.retainAll(vertex2);
		return vertex1.size()/(Math.sqrt(v1_size*v2_size));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
		// Identity reducer for map testing
		/*
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			context.write(key, it.next());
		}
		*/
		// What is this key's attribute position?
		StringTokenizer tokenizer = new StringTokenizer(key.toString(), "-");
		int key_pos = Integer.valueOf(tokenizer.nextToken());
		
		Vector<Record> records = new Vector<Record>();
				
		Configuration conf = context.getConfiguration();
		// Get the similarity threshold from the job parameters
	    String par_epsilon = conf.get("epsilon");
	    double eps = Double.parseDouble(par_epsilon);    
    	Iterator<Text> it = values.iterator();
    	// Attributes already in the desired order (by attribute entropy) 
    	// thanks to the mapper
    	Record r = new Record(it.next().toString());
    	// Using this first run to read all data, while already checking
    	// the similarities for the first record
    	Text idr = new Text();
    	Text idr2 = new Text();
    	while (it.hasNext()) {
    		idr.set(r.getIdStr());
    		Record r2 = new Record(it.next().toString());
    		// evaluate similarity
			double sim = r.checkSimilarity(r2);
    		if (sim >= eps) {
    			int first_match = r.firstMatch(r2);
    			if (key_pos == first_match) {
    				idr2.set(r2.getIdStr());
    				context.write(idr, idr2);
    				context.write(idr2, idr);
    			}
    		}
    		// Store r2 for later checking
    		records.add(r2);
    	}
    	// Now do a loop to check the other records
    	for (int i = 0; i < records.size(); ++i) {
    		r = records.get(i);
    		idr.set(r.getIdStr());
    		// Getting similar records that were already checked
    		// For all records following it
    		for (int j = i+1; j < records.size(); ++j) {
    			Record r2 = records.get(j);
    			double sim = r.checkSimilarity(r2);
        		if (sim >= eps) {
        			int first_match = r.firstMatch(r2);
        			if (key_pos == first_match) {
        				idr2.set(r2.getIdStr());
        				context.write(idr, idr2);
        				context.write(idr2, idr);
        			}
        		}
    		}
    	}
    	/*
    	while (it.hasNext()) {
    		Record r2 = new Record(it.next().toString());
    		// evaluate similarity
			double sim = r.checkSimilarity(r2);
    		if (sim >= eps) {
				similar += " "+r2.getIdStr();
				// Add this similarity to r2 for later use
				String similar_r2 = new String();
				if (similar_records.containsKey(r2.getIdStr())) {
	    			similar_r2 = similar_records.get(r2.getIdStr());
	    		} else {
	    			similar_r2 = "";
	    		}
				similar_records.put(r2.getIdStr(), similar_r2+" "+r.getIdStr());
    		}
    		// Store r2 for later checking
    		records.add(r2);
    	}
    	// Print the similar records
    	if (!similar.trim().isEmpty())
    		context.write(new Text(r.getIdStr()), new Text(similar));
    		
    	// Now do a loop to check the other records
    	for (int i = 0; i < records.size(); ++i) {
    		r = records.get(i);
    		// Getting similar records that were already checked
    		if (similar_records.containsKey(r.getIdStr())) {
    			similar = similar_records.get(r.getIdStr());
    		} else {
    			similar = "";
    		}
    		// For all records following it
    		for (int j = i+1; j < records.size(); ++j) {
    			Record r2 = records.get(j);
    			double sim = r.checkSimilarity(r2);
        		if (sim >= eps) {
    				similar += " "+r2.getIdStr();
    				// Add this similarity to r2 for later use
    				String similar_r2 = new String();
    				if (similar_records.containsKey(r2.getIdStr())) {
    	    			similar_r2 = similar_records.get(r2.getIdStr());
    	    		} else {
    	    			similar_r2 = "";
    	    		}
    				similar_records.put(r2.getIdStr(), similar_r2+" "+r.getIdStr());
        		}
    		}
    		// Print the similar records
        	if (!similar.trim().isEmpty())
        		context.write(new Text(r.getIdStr()), new Text(similar));
    	}
    	*/
	}

}