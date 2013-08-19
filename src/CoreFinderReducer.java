import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CoreFinderReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int mi = Integer.parseInt(conf.get("mi"));
		Set<String> similars = new TreeSet<String>(); 
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			StringTokenizer tokenizer = new StringTokenizer(it.next().toString());
			while (tokenizer.hasMoreTokens()) {
				similars.add(tokenizer.nextToken());
			}
		}
		if (similars.size() >= mi) {
			String allsims = new String();
			for (String s : similars) {
				allsims += " "+s;
			}
			context.write(key, new Text(allsims));
		}
	}

}
