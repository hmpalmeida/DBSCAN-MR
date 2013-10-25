package CoreMerger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CoreMerger {
	
	public CoreMerger() {}
	
	private long doMergeStep(String input_file, String output_file) throws IOException {
		Configuration conf = new Configuration();
		// FIXME SETTING THE FILESYSTEM?
		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
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
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        counter = job.getCounters().findCounter(CoreMergerReducer.UpdateCounter.UPDATED)
       	     .getValue();
        
        return counter;
	}
	
	private void doPartialMerge(String merged_cores_file, String cores_file, 
			String tmp_file) throws IOException {
		Configuration conf = new Configuration();
		// FIXME SETTING THE FILESYSTEM?
		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "PartialMerge");
              
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setReducerClass(PartialMergeReducer.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
                
		MultipleInputs.addInputPath(job, new Path(merged_cores_file), 
				TextInputFormat.class, MergedCoresMapper.class);
		MultipleInputs.addInputPath(job, new Path(cores_file), 
				TextInputFormat.class, CoresMapper.class);		
        
		Path temp = new Path(tmp_file);
		if (dfs.exists(temp)) dfs.delete(temp, true);
		FileOutputFormat.setOutputPath(job, temp);
        
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	// Like the Basic Record Join from the original paper.
	private void doMerge(String partial_merge_file, String output_file) throws IOException {
		Configuration conf = new Configuration();
		// FIXME SETTING THE FILESYSTEM?
		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "ClusterCleaner");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(FinalMergeMapper.class);
        job.setReducerClass(FinalMergeReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(partial_merge_file));
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
	

	private void generateCoresOnly(String cores_file, String cores_only_file) throws IOException {
		Configuration conf = new Configuration();
		// FIXME SETTING THE FILESYSTEM?
		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		conf.set("job.core.file", cores_file);
		
        Job job = new Job(conf, "CoresOnly");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(CoresOnlyMapper.class);
        job.setReducerClass(CoresOnlyReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(cores_file));
		Path output = new Path(cores_only_file);
		if (dfs.exists(output)) dfs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
            
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public boolean run(String cores_file, String final_file) throws IOException {
		Path p = new Path(final_file);
		// First, we must obtain a file with only the cores
		String cores_only_file = 
				new String(p.getParent().toString()+"/cores-only.txt");
		generateCoresOnly(cores_file, cores_only_file);
		String merged_file_a = 
				new String(p.getParent().toString()+"/merged-cores-a.txt");
		String merged_file_b = 
				new String(p.getParent().toString()+"/merged-cores-b.txt");
		String[] files = new String[]{merged_file_a, merged_file_b};
		int MAX_STEPS = 30;
		int step = 0;
		long counter = doMergeStep(cores_only_file, files[0]);
		while (counter > 0 && step < MAX_STEPS) {
			++step;
			counter = doMergeStep(files[(step-1) % 2], files[step % 2]);
		}
		// Clean up the file for final answer
		String tmp_file = new String(p.getParent().toString()+"/merged_tmp.txt");
		doPartialMerge(files[step % 2], cores_file, tmp_file);
		doMerge(tmp_file, final_file);
		// Remove temporary files
		Configuration conf = new Configuration();
		// FIXME SETTING THE FILESYSTEM?
		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");
		FileSystem dfs = FileSystem.get(conf);
		Path file = new Path(merged_file_a);
		if (dfs.exists(file)) dfs.delete(file, true);
		file = new Path(merged_file_b);
		if (dfs.exists(file)) dfs.delete(file, true);
		file = new Path(tmp_file);
		if (dfs.exists(file)) dfs.delete(file, true);
		file = new Path(cores_file);
		if (dfs.exists(file)) dfs.delete(file, true);
		file = new Path(cores_only_file);
		if (dfs.exists(file)) dfs.delete(file, true);
		// Check if the algorithm converged
		if (counter == 0) {
			return true;
		} else {
			return false;
		}
	}


}
