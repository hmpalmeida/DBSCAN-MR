package Entropy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class EntropyReducer  extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
		String attr = key.toString();
		String line = new String();
		
		HashMap<String, Integer> value_counts =  new HashMap<String, Integer>();
		int total_count = 0;
		
    	Iterator<Text> i = values.iterator();
    	// Already checked records (I in the paper)
    	while (i.hasNext()) {
    		line = i.next().toString();
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			String val = tokenizer.nextToken();
			int count = Integer.parseInt(tokenizer.nextToken());
			total_count += count;
			if (value_counts.containsKey(val)) {
				// Add new count to val
				count += value_counts.get(val);
				value_counts.put(val, count);
			} else {
				// Create new entry
				value_counts.put(val, count);
			}
    	}
    	// Now calculate the entropy of attribute "key"
    	// Entropy = \sum{p(x_i) \log{p(x_i)}}
    	double entropy = 0.0;
    	Set<String> hmkeys = value_counts.keySet();
    	Iterator<String> it = hmkeys.iterator();
    	while (it.hasNext()) {
    		double p = value_counts.get(it.next())/(double)total_count;  
    		entropy += p * Math.log10(p);
    	}
    	context.write(key, new Text(String.valueOf(-1*entropy)));
	}

}
