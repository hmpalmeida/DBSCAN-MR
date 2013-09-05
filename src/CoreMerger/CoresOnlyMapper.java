package CoreMerger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CoresOnlyMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private HashSet<String> cores = new HashSet<String>();
	
	public void setup(Context context) {
	    Configuration conf = context.getConfiguration();
	    // hardcoded or set it in the jobrunner class and retrieve via this key
	    String location = conf.get("job.core.file");
	    if (location != null) {
	    	BufferedReader br = null;
	        try {
	            FileSystem fs = FileSystem.get(conf);
	            Path path = new Path(location);
	            // Will read from a MR output
	            FileStatus[] fss = fs.listStatus(path);
			    for (FileStatus status : fss) {
			        Path new_path = status.getPath();
			        // Ignore _SUCCESS file
			        if (new_path.getName().contains("_SUCCESS")) continue; 
	                FSDataInputStream fis = fs.open(new_path);
	                br = new BufferedReader(new InputStreamReader(fis));
	                String line = null;
	                while ((line = br.readLine()) != null && line.trim().length() > 0) {
	                	StringTokenizer tokenizer = new StringTokenizer(line);
	                	String c = tokenizer.nextToken();
	                    cores.add(c);
	                }
	            }
	        }
	        catch (IOException e) {
	            //handle
	        } 
	        finally {
	            //IOUtils.closeQuietly(br);
	            IOUtils.closeStream(br);
	        }
	    }
	}
	
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		// Getting the record key and preparing the sorted record  
		String core = tokenizer.nextToken();
		String neighbors = new String("");
		// Tokenize the record
		while (tokenizer.hasMoreTokens()) {
			String element = tokenizer.nextToken();
			if (cores.contains(element)) neighbors += " "+element;
		}
		if (!neighbors.isEmpty()) {
			context.write(new Text(core), new Text(neighbors));
		}
	}
}
