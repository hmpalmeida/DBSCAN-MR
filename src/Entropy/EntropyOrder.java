package Entropy;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EntropyOrder {
	
	private String input;
	private Vector<Integer> order;
	
	public EntropyOrder(String input_file) {
		this.input = input_file;
		this.order = new Vector<Integer>();
	}
	
	public boolean generateOrder(String order_file) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "EntropyOrder");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(EntropyMapper.class);
        job.setReducerClass(EntropyReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(this.input));
		Path output = new Path(order_file);
		if (dfs.exists(output)) dfs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
            
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;		
	}

}
