package CoreMerger;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergedCoresMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		String first = tokenizer.nextToken();
		String second = tokenizer.nextToken();
		// Value = signal to send things to the responsible ID
		if (second.compareTo("!FWD!") != 0 && 
				second.compareTo("!QRY!") != 0) {
			Text val = new Text("!FWD! " + first);
			Text k = new Text(second);
			context.write(k, val);
			while (tokenizer.hasMoreTokens()) {
				k.set(tokenizer.nextToken());
				context.write(k, val);
			}
		}
	}

}
