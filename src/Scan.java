import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
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


import Entropy.EntropyOrder;

/*
 * TODO O que vai mudar?
 * - A entrada não é mais um grafo, então teremos:
 * 		ID Atributo1 ... Atributo N
 *   Ao invés de:
 *   	ID ID_Vizinho 1 ... ID_Vizinho N
 *   
 *   E a função de similaridade vai mudar também
 * 
 */

public class Scan {
	
	private String input_file;

	
	private void findSimilars(double epsilon, String order_file, String output_file) throws IOException {
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
        // FIXME temporary output file
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
	
	private void defineCores(int mi, String similarities_file, String output_file) throws IOException {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void coreFinder(double epsilon, int mi, String order_file, String cores_file) throws IOException {
		String similarities_file = "/user/helio/outputs/similarities-tmp.txt";
		findSimilars(epsilon, order_file, similarities_file);
		defineCores(mi, similarities_file, cores_file);
	}
	
	private void coreVoter() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		
        Job job = new Job(conf, "CoreFinder");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(SimilarityMapper.class);
        job.setReducerClass(SimilarityReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input_file));
        // FIXME temporary output file
		Path output = new Path("/user/helio/outputs/result-SCAN.txt");
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
	
	private void runScan(double epsilon, int mi, String order_file) throws IOException {
		String cores_file = "/user/helio/outputs/cores.txt";
		coreFinder(epsilon, mi, order_file, cores_file);
		coreVoter();
	}
	
	public Scan(String filename) {
		this.input_file = filename;
	}

	
	public static void main(String[] args) throws Exception {
	    /*
		String line = "1 \t   aaa  zzz  qqq  ppp  lll  mmm";
		Record r = new Record(line, order);
		System.out.println(r.getAttrStr());
		*/
		
		// First, obtain the entropy order of the attributes
		EntropyOrder eo = new EntropyOrder(args[0]);
		String order_file = "/user/helio/outputs/field-entropy-tmp.txt";
		eo.generateOrder(order_file);
		// Run scan with the order found
		Scan s = new Scan(args[0]);
		//Vector<Long> c_order = s.getCustomAttributeOrder(order_file);
		s.runScan(Double.parseDouble(args[1]), 
				Integer.parseInt(args[2]), order_file);
		
	}
	
}
