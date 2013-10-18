package CoreFinder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoreFinder {
	
	public CoreFinder() {}
	
	public String run(double epsilon, int mi, String input_file, 
			String order_file, String cores_file) throws IOException {
		Path p = new Path(cores_file);
		String stats = new String();
		long sim_start_time = System.currentTimeMillis();
		String similarities_file = 
				new String(p.getParent().toString()+"/similarities-tmp.txt");
		stats = findSimilars(epsilon, input_file, order_file, similarities_file);
		long sim_time = System.currentTimeMillis() - sim_start_time;
		// May delete similarities_file here
		long cores_start_time = System.currentTimeMillis();
		stats += defineCores(mi, similarities_file, cores_file);
		long cores_time = System.currentTimeMillis() - cores_start_time;
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		Path file = new Path(similarities_file);
		if (dfs.exists(file)) dfs.delete(file, true);
		return stats + "( " + String.valueOf(sim_time) + " , " + String.valueOf(cores_time) + " )";
	}
	
	private String findSimilars(double epsilon, String input_file, 
			String order_file, String output_file) throws IOException {
		Configuration conf = new Configuration();
		// Similarity threshold
		conf.set("epsilon", String.valueOf(epsilon));
		// Minimum neighbor count for core
		//conf.set("mi", String.valueOf(mi));
		conf.set("job.customorder.path", order_file);
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "Similarities");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(SimilarityMapper.class);
        job.setReducerClass(SimilarityReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input_file));
		Path output = new Path(output_file);
		if (dfs.exists(output)) dfs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
            
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        // Get map reduce statistics
        Counters counters = job.getCounters();
        String count_acc = new String("Similarities  ");
        /*
        for (CounterGroup group : counters) {
        	count_acc += "* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")\n";
        	count_acc += "  number of counters in this group: " + group.size() + "\n";
			for (Counter counter : group) {
				count_acc += "  - " + counter.getDisplayName() + ": " + counter.getName() + 
						": " + counter.getValue() + "\n";
        	}
        }
        */
        count_acc +=
        counters.findCounter(org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS).getValue() + " " +
        counters.findCounter(org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES).getValue() + " " +
        counters.findCounter(org.apache.hadoop.mapred.Task.Counter.REDUCE_OUTPUT_RECORDS).getValue()+ " ";
        return count_acc;
	}
	
	private String defineCores(int mi, String similarities_file, String output_file) throws IOException {
		Configuration conf = new Configuration();
		// Minimum neighbor count for core
		conf.set("mi", String.valueOf(mi));
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "CoreFinder");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(CoreFinderMapper.class);
        job.setReducerClass(CoreFinderReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(similarities_file));
		Path output = new Path(output_file);
		if (dfs.exists(output)) dfs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
            
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        // Get map reduce statistics
        Counters counters = job.getCounters();
        String count_acc = new String("CoreFinder  ");
        count_acc +=
        counters.findCounter(org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS).getValue() + " " +
        counters.findCounter(org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES).getValue() + " " +
        counters.findCounter(org.apache.hadoop.mapred.Task.Counter.REDUCE_OUTPUT_RECORDS).getValue()+ " ";
        return count_acc;
	}
}