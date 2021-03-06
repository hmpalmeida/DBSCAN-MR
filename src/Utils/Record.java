package Utils;
import java.util.StringTokenizer;
import java.util.Vector;


public class Record {
	
	private long id;
	public Vector<String> attributes;
	
	public boolean loadData(String data) {
		// read and tokenize text string
		StringTokenizer tokenizer = new StringTokenizer(data);
		this.id = Long.parseLong(tokenizer.nextToken());
		this.attributes = new Vector<String>();
		while (tokenizer.hasMoreTokens()) {
			String tmp = tokenizer.nextToken().trim();
			if (!tmp.isEmpty()) this.attributes.add(tmp);			
		}
		return true;
	}
	
	public boolean loadData(String data, Vector<Integer> custom_order) {
		if (custom_order == null) {
			return false;
		}
		if (custom_order.size() == 0) {
			return this.loadData(data);
		}
		// read and tokenize text string
		StringTokenizer tokenizer = new StringTokenizer(data);
		this.id = Integer.parseInt(tokenizer.nextToken());
		Vector<String> tmp = new Vector<String>();
		while (tokenizer.hasMoreTokens()) {
			tmp.add(tokenizer.nextToken());			
		}
		this.attributes = new Vector<String>();
		for (int i = 0; i < tmp.size(); ++i) this.attributes.add("");
		if (tmp.size() != custom_order.size()) {
			// # of attributes read differs from the # of elements
			// in the custom order
			return false;
		} else {
			for (int i = 0; i < custom_order.size(); ++i) {
				this.attributes.set(i, tmp.get(custom_order.get(i)));
			}
			return true;
		}
	}
	
	public Record(String data) {
		loadData(data);		
	}
	
	public Record(String data, Vector<Integer> custom_order) {
		loadData(data, custom_order);		
	}
	
	public Record() {		
	}
	
	public String toString() {
		String output = new String("");
		output += String.valueOf(this.id);
		for (int i = 0; i < this.attributes.size(); ++i) {
			output += " " + this.attributes.get(i);
		}
		return output;
	}
	
	public String getIdStr() {
		return String.valueOf(this.id);
	}
	
	public String getAttrStr() {
		String output = new String("");
		for (int i = 0; i < this.attributes.size(); ++i) {
			output += " " + this.attributes.get(i);
		}
		return output;
	}
	
	public double checkSimilarity(Record r) {
		if (r == null || 
				r.attributes.size() != this.attributes.size()) 
			return 0.0;
		int equals = 0;
		for (int i = 0; i < this.attributes.size(); ++i) {
			if (this.attributes.get(i).compareTo(r.attributes.get(i)) == 0) {
				++equals;
			}
		}
		return equals/(double)this.attributes.size();
	}
	
	public int firstMatch(Record r) {
		if (r == null || 
				r.attributes.size() != this.attributes.size()) 
			return -1;
		int match = 0;
		boolean found = false;
		while (match < this.size() && !found) {
			if (this.attributes.get(match).compareTo(r.attributes.get(match)) == 0) {
				found = true;				
			} else {
				match++;
			}
		}
		if (found) {
			return match;
		} else {
			return -1;
		}
	}
	
	public int size() {
		return this.attributes.size();
	}

}