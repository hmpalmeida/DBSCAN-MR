package CoreFinder;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CoreFinderMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{    
		// Read and tokenize text string.
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		String element = tokenizer.nextToken();
		String similars = new String();
		
		while (tokenizer.hasMoreTokens()) {
			similars += " "+tokenizer.nextToken();
		}
		
		if (!similars.isEmpty())
			context.write(new Text(element), new Text(similars));
	}
}
