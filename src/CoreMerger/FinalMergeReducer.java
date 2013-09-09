package CoreMerger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalMergeReducer  extends Reducer<Text, Text, Text, Text> {
		
	public void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
		Set<Long> elements = new TreeSet<Long>();
    	for (Text val : values) {
    		String line = val.toString();
    		StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				elements.add(Long.valueOf(tokenizer.nextToken()));
			}
    	}
    	String vals = new String("");
    	Iterator<Long> it = elements.iterator();
    	while (it.hasNext()) {
    		vals += " " + String.valueOf(it.next());
    	}
    	context.write(key, new Text(vals));
	}

}
