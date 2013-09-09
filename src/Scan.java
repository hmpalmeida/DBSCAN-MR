import java.io.IOException;

import org.apache.hadoop.fs.Path;

import CoreFinder.CoreFinder;
import CoreMerger.CoreMerger;
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
	private String output_file;
	
	private String runScan(double epsilon, int mi, String order_file) throws IOException {
		Path p = new Path(this.output_file);
		String cores_file = 
				new String(p.getParent().toString()+"/cores.txt");
		CoreFinder cf = new CoreFinder();
		// 2nd part (finding the cores)
		long startTime = System.currentTimeMillis();
		String stats = cf.run(epsilon, mi, this.input_file, order_file, cores_file);
		long endTime = System.currentTimeMillis();
		String times = new String(" " + String.valueOf(endTime - startTime));
		CoreMerger cm = new CoreMerger();
		// 3rd part (merging the cores)
		startTime = System.currentTimeMillis();
		boolean converge = cm.run(cores_file, this.output_file);
		endTime = System.currentTimeMillis();
		times += " " + String.valueOf(endTime - startTime);
		times += "\n------------------------------\n" + stats +"\n------------------------------\n";
		return times;
	}
	
	public Scan(String input, String output) {
		this.input_file = input;
		this.output_file = output;
	}

	public static void main(String[] args) throws Exception {
		// Adding legibility to the inputs
		double epsilon = Double.parseDouble(args[0]);
		int mi = Integer.parseInt(args[1]);
		String input_file = args[2];
		String output_file = args[3];
	    // First, obtain the entropy order of the attributes
		EntropyOrder eo = new EntropyOrder(input_file);
		Path p = new Path(output_file);
		String order_file = 
				new String(p.getParent().toString()+"/field-entropy-tmp.txt");
		// 1st step (creating entropy-based order)
		long startTime = System.currentTimeMillis();
		eo.generateOrder(order_file);
		long endTime = System.currentTimeMillis();
		String times = new String();
		times = input_file + " " + args[0] + " " + args[1] + 
				" " + String.valueOf(endTime - startTime);
		// Run scan with the order found
		Scan s = new Scan(input_file, output_file);
		times += s.runScan(epsilon, mi, order_file);
		System.out.println(times);
	}
	
}
