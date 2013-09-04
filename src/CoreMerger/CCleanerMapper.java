package CoreMerger;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CCleanerMapper  extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String line = value.toString();
		Text k = new Text();
		Text val = new Text();
		StringTokenizer tokenizer = new StringTokenizer(line);
		String first = new String(tokenizer.nextToken());
		String second = new String(tokenizer.nextToken());
		if (second.compareTo("!QRY!") != 0 &&
				second.compareTo("!FWD!") != 0) {
			k.set(first);
			while (tokenizer.hasMoreTokens()) {
				second += " "+tokenizer.nextToken();
			}
			val.set(second);
			context.write(k, val);
		}
	}

}
