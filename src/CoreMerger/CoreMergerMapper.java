package CoreMerger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoreMergerMapper  extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String line = value.toString();
		Text k = new Text();
		Text val = new Text();
		List<String> fields = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			fields.add(tokenizer.nextToken());
		} 
		// !FWD! and !QRY! signaled entries will be treated at the reducer
		if (fields.get(1).compareTo("!FWD!") == 0 ||
				fields.get(1).compareTo("!QRY!") == 0) {
			k.set(fields.get(0));
			val.set(fields.get(1)+" "+fields.get(2));
			context.write(k, val);						
		} else {
			// Notify that this element is a !CORE!
			k.set(fields.get(0));
			val.set("!CORE!");
			context.write(k, val);
			List<Long> lfields = new ArrayList<Long>();
			for (int i = 0; i < fields.size(); ++i) {
				lfields.add(Long.valueOf(fields.get(i)));				
			}
			Collections.sort(lfields);
			Iterator<Long> it = lfields.iterator();
			boolean first = true;
			String min = new String();
			while (it.hasNext()) {
				if (first) {
					min = String.valueOf(it.next());
					k.set(min);
					context.write(k, value);
					first = false;
				} else {
					k.set(String.valueOf(it.next()));
					val.set("!FWD! "+min);
					context.write(k, val);
				}
			}
		}
	}

}
