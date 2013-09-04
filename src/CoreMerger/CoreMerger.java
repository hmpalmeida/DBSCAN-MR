package CoreMerger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoreMerger {
	
	public CoreMerger() {}
	
	private long doMergeStep(String input_file, String output_file) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "MergeStep");
        
        long counter = 0;
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(CoreMergerMapper.class);
        job.setReducerClass(CoreMergerReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input_file));
		Path output = new Path(output_file);
		if (dfs.exists(output)) dfs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
            
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        counter = job.getCounters().findCounter(CoreMergerReducer.UpdateCounter.UPDATED)
       	     .getValue();
        
        return counter;
	}
	
	private void doFileCleanup(String input_file, String output_file) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "ClusterCleaner");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(CCleanerMapper.class);
        job.setReducerClass(CCleanerReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input_file));
		Path output = new Path(output_file);
		if (dfs.exists(output)) dfs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
            
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean run(String cores_file, String final_file) throws IOException {
		Path p = new Path(final_file);
		String merged_file_a = 
				new String(p.getParent().toString()+"/merged-cores-a.txt");
		String merged_file_b = 
				new String(p.getParent().toString()+"/merged-cores-b.txt");
		String[] files = new String[]{merged_file_a, merged_file_b};
		int MAX_STEPS = 2;//30;
		int step = 0;
		long counter = doMergeStep(cores_file, files[0]);
		while (counter > 0 && step < MAX_STEPS) {
			++step;
			counter = doMergeStep(files[(step-1) % 2], files[step % 2]);
		}
		// Clean up the file for final answer
		doFileCleanup(files[step % 2], final_file);
		// Remove temporary files
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");
		/*
		FileSystem dfs = FileSystem.get(conf);
		Path file = new Path(merged_file_a);
		if (dfs.exists(file)) dfs.delete(file, true);
		file = new Path(merged_file_b);
		if (dfs.exists(file)) dfs.delete(file, true);
		*/
		// Check if the algorithm converged
		if (counter == 0) {
			return true;
		} else {
			return false;
		}
	}

}
