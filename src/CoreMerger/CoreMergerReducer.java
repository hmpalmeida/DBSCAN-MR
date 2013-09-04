package CoreMerger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoreMergerReducer extends Reducer<Text, Text, Text, Text> {
	
	public enum UpdateCounter {
		  UPDATED
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
		int this_key = Integer.valueOf(key.toString());
		int MAXINT = 2147483647;
		Iterator<Text> it = values.iterator();
		Set<Long> elements = new TreeSet<Long>();
		Set<String> qry_set = new TreeSet<String>();
		int min_fwd = MAXINT;
    	while (it.hasNext()) {
    		String line = it.next().toString();
    		StringTokenizer tokenizer = new StringTokenizer(line);
    		String first = tokenizer.nextToken();
    		if (first.compareTo("!FWD!") == 0) {
    			int fwd_to = Integer.parseInt(tokenizer.nextToken());
    			if (fwd_to < min_fwd) min_fwd = fwd_to; 
    		} else if (first.compareTo("!QRY!") == 0) {
    			qry_set.add(tokenizer.nextToken());
    		} else {
    			elements.add(Long.valueOf(first));
    			while (tokenizer.hasMoreTokens())
    				elements.add(Long.valueOf(tokenizer.nextToken()));
    		}
    	}
    	// Generate the output elements, if any
    	String el_str = new String();
    	Iterator<Long> it2 = elements.iterator();
    	while (it2.hasNext()) {
    		el_str += " "+String.valueOf(it2.next());
    	}
    	Text output = new Text(el_str);
    	Text k = new Text();
    	// Everything read, now output results
    	if ((min_fwd == MAXINT || min_fwd == this_key) 
    			&& !el_str.isEmpty()) {
    		// No forwarding path. This is the "sink" node
    		context.write(key, output);
    	} else if (!el_str.isEmpty()) {
    		k.set(String.valueOf(min_fwd));
    		context.write(k, output);
    		// TODO Since I've sent info to another element, we must
    		// run another round
    		context.getCounter(UpdateCounter.UPDATED).increment(1);
    	}
    	// Print this element's default forward
    	if (min_fwd != MAXINT) {
    		output.set("!FWD! "+String.valueOf(min_fwd));
    		context.write(key, output);
    	}
    	// Print forward suggestions for each !QRY! received
    	Iterator<String> it3 = qry_set.iterator();
    	while (it3.hasNext()) {
    		k.set(it3.next());
    		context.write(k, output);
    	}
    	// Print !QRY!, if necessary
    	if (min_fwd != MAXINT && min_fwd != this_key) {
    		k.set(String.valueOf(min_fwd));
    		output.set("!QRY! "+key.toString());
    		context.write(k, output);
    	}
	}

}
