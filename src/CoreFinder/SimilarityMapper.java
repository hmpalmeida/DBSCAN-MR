package CoreFinder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
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

import Utils.Record;
import Utils.AttrEntropy;


public class SimilarityMapper  extends Mapper<LongWritable, Text, Text, Text> {
	
	private Vector<Integer> custom_order = new Vector<Integer>();
	
	public void setup(Context context) {
	    Configuration conf = context.getConfiguration();
	    // hardcoded or set it in the jobrunner class and retrieve via this key
	    //String location = conf.get("job.customorder.path");
	    
	    // FIXME should not be hardwired!
	    String location = "/user/helio/outputs/field-entropy-tmp.txt";
	    
		Vector<AttrEntropy> vae = new Vector<AttrEntropy>();
		
	    if (location != null) {
	    	BufferedReader br = null;
	        try {
	            FileSystem fs = FileSystem.get(conf);
	            Path path = new Path(location);
	            // Considering that the result of the custom order is partitioned as a reducer
	            // output, we need to get all partial results in order to load custom_order
	            
	    	    // FIXME TESTING
	    	    /////
	            //    FSDataOutputStream out = fs.create(new Path("/user/helio/outputs/file.txt"));
	    	    /////

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
	                	int attr = Integer.parseInt(tokenizer.nextToken());
	    				double entropy = Double.parseDouble(tokenizer.nextToken());
	    				AttrEntropy ae = new AttrEntropy(attr, entropy);
	    				vae.add(ae);
	    		    	//out.writeUTF(String.valueOf(vae.size())+" "+String.valueOf(attr)+"\n");
	                }
	            }
			    //out.close();
	        }
	        catch (IOException e) {
	            //handle
	        } 
	        finally {
	            //IOUtils.closeQuietly(br);
	            IOUtils.closeStream(br);
	        }
	    }
	    
	    //try{
	    //FileSystem fs = FileSystem.get(conf);
        //FSDataOutputStream out = fs.create(new Path("/user/helio/outputs/file.txt"));
	    
	    Collections.sort(vae, AttrEntropy.EntropyComparator);
	    for(int i = 0; i < vae.size(); ++i) {
	    	this.custom_order.add(vae.get(i).attribute);
	    	//out.writeUTF(String.valueOf(vae.get(i))+"\n");
	    }
	    //out.close();
        //}catch (IOException e) {
        //    //handle
        //}
	    
	}
	
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		// Get the similarity threshold from the job parameters
		Configuration conf = context.getConfiguration();
	    String par_epsilon = conf.get("epsilon");
	    double eps = Double.parseDouble(par_epsilon);
	    
		// Read and tokenize text string. 
		String line = value.toString();
		Record r = new Record(line, this.custom_order);
	    
	    // Compute prefix size
		int prefix_size = (int) (custom_order.size() - 
				Math.ceil(eps * custom_order.size()) + 1);
		
		// Use individual to generate output pairs
		for (int i = 0; i < prefix_size; ++i) {
			Text outkey = new Text();
			outkey.set(String.valueOf(i)+"-"+r.attributes.get(i));
			context.write(outkey, new Text(r.toString()));
		}
	}

}
