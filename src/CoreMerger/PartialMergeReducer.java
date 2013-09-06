package CoreMerger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartialMergeReducer  extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
		Set<Long> elements = new TreeSet<Long>();
		String fwd_to = new String();
		String output = new String();
    	for (Text val : values) {
    		String line = val.toString();
    		StringTokenizer tokenizer = new StringTokenizer(line);
    		String first = tokenizer.nextToken();
    		if (first.compareTo("!FWD!") == 0) fwd_to = tokenizer.nextToken();
    		else output += line;
    	}
    	context.write(new Text(fwd_to), new Text(output));
	}
}
