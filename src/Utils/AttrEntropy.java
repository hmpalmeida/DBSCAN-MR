package Utils;

import java.util.Comparator;

public class AttrEntropy {
	public double entropy;
	public int attribute;
	//public AttrEntropy() {}
	public AttrEntropy(int attr, double ent) {
		this.attribute = attr; this.entropy = ent;
	}
	public String toString() {
		return String.valueOf(this.attribute)+" "+String.valueOf(this.entropy);
	}
	
	public static Comparator<AttrEntropy> EntropyComparator = new Comparator<AttrEntropy>() {

		public int compare(AttrEntropy ae1, AttrEntropy ae2) {

			int value = 0;
			if (ae1.entropy > ae2.entropy) value = -1; 
			else if (ae1.entropy < ae2.entropy) value = 1; 
			else if (ae1.entropy == ae2.entropy) value = 0; 
			return value; 
		}

	};
}