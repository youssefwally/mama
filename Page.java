package mama;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class Page implements Serializable {
	/**
	 * 
	 */
	
	private Vector<Hashtable<String, Object>> vec;
	String dir = "data\\";
	final static String dirmeta = "data\\";
	String dirI = "data\\";

	String filename = "p";
	static int i = -1;
	Hashtable<String, String> ht;
	final static int length = DBApp.length;
	int k = 0;
	boolean full;
	String all = "";
	String max;
	String min;
	private static final long serialVersionUID = 1L;

	public Vector<Hashtable<String, Object>> getVec() {
		return vec;
	}

	public void setVec(Vector<Hashtable<String, Object>> vec) {
		this.vec = vec;
	}

	public Page(Hashtable<String, String> ht,String tableName) {
		dir+=tableName+"\\pages\\";
		dirI+=tableName+"\\indexes\\";
		i++;
		filename += i;
		this.ht = ht;
		vec = new Vector<Hashtable<String, Object>>();
		full=false;
		// vec.setSize(length);
	}

	public int insert(Hashtable<String, Object> row, String strkey) throws FileNotFoundException, IOException {
		// System.out.println(k);
		int z=0;
		row.putIfAbsent("TouchDate", LocalDateTime.now());
		if ((row.get(strkey) instanceof Integer)) {
			if (full) {
				// vec.set(length-1,row);
				int i;
				for (i = 0; i < vec.size(); i++) {
					if ((int) (row.get(strkey)) <= (int) vec.get(i).get(strkey)) {
						break;
					}
				}
				vec.add(i, row);
				vec.remove(length);
				z=i;
			} else {
				int i;
				for (i = 0; i < vec.size(); i++) {
					if ((int) (row.get(strkey)) <= (int) vec.get(i).get(strkey)) {
						break;
					}
				}
				vec.add(i, row);
				z=i;
			}
		} else if ((row.get(strkey) instanceof Double)) {
			if (full) {
				// vec.set(length-1,row);
				int i;
				for (i = 0; i < vec.size(); i++) {
					if ((double) (row.get(strkey)) < (double) vec.get(i).get(strkey)) {
						break;
					}
				}
				vec.add(i, row);
				vec.remove(length);
				z=i;
			//	System.out.println(69);
			} else {
				int i;
				for (i = 0; i < vec.size(); i++) {
					if ((double) (row.get(strkey)) < (double) vec.get(i).get(strkey)) {
						break;
					}
				}
				vec.add(i, row);
				z=i;
			}
		} else {
			if (full) {
				// vec.set(length-1,row);
				int i;
				for (i = 0; i < vec.size(); i++) {
					if ((("" + row.get(strkey)).compareTo("" + vec.get(i).get(strkey)) < 0)) {
						break;
					}
				}
				z=i;
				vec.add(i, row);
				vec.remove(length);
			} else {
				int i;
				for (i = 0; i < vec.size(); i++) {
					if ((("" + row.get(strkey)).compareTo("" + vec.get(i).get(strkey)) < 0)) {
						break;
					}
				}
				vec.add(i, row);
				z=i;

			}
		}

		k++;
	//	System.out.println(vec);
	//	System.out.println(full);
		

		if (vec.size() >= length)
		{
			full = true;
			}
	//	System.out.println(i);
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dir + filename));
		out.writeObject(this);
		out.close();
		return z;
	}

	public boolean check(String strkey, Hashtable<String, Object> in) {

		if ((in.get(strkey) instanceof Integer)) {
			for (int i = 0; i < vec.size(); i++) {
				if ((int) in.get(strkey) < (int) vec.get(i).get(strkey)) {
					return true;
				}
			}
		} else if ((in.get(strkey) instanceof Double)) {
			for (int i = 0; i < vec.size(); i++) {
				if ((double) in.get(strkey) < (double) vec.get(i).get(strkey))
					return true;
			}
		} else {
			for (int i = 0; i < vec.size(); i++) {
				if ((("" + in.get(strkey)).compareTo("" + vec.get(i).get(strkey)) < 0))
					return true;
			}
		}

		return false;
	}

	public void update(String strKey, Hashtable<String, Object> in, String col)
			throws FileNotFoundException, IOException {
		for (int i = 0; i < vec.size(); i++) {
			
			if (table.Equal2(strKey,vec.get(i).get(col))) {
				vec.set(i, in);
				vec.get(i).put("TouchDate", LocalDateTime.now());
			}
		}
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dir + filename));
		out.writeObject(this);
		out.close();

		if (vec.size() >= length)
			full = true;

	}

	public ArrayList<Integer> delete(Hashtable<String, Object> in) throws ClassNotFoundException, IOException {
		ArrayList<Integer> positions=new ArrayList<>();
			for (int j = 0; j < vec.size(); j++) {
				boolean rem=false;
				for (Map.Entry<String, Object> entry : in.entrySet()) {
					String value_from_vec = vec.get(j).get(entry.getKey()).toString();
					String value_from_in = entry.getValue().toString();
					rem=false;
					if (value_from_in != null && value_from_vec != null && value_from_in.equals(value_from_vec)) {
						rem=true;
					}
					else {
						break;
					}
				}
				if(rem) {
				positions.add(j);
				vec.remove(j);
				j--;
				}
			}
			if (vec.size() >= length)
				full = true;
			else full=false;
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dir + filename));
			out.writeObject(this);
			out.close();
		return positions;
	}

	
	public ArrayList<Integer> delete2(Hashtable<String, Object> in) throws ClassNotFoundException, IOException {
		ArrayList<Integer> positions=new ArrayList<>();
			for (int j = 0; j < vec.size(); j++) {
				boolean rem=false;
				for (Map.Entry<String, Object> entry : in.entrySet()) {
					String value_from_vec = vec.get(j).get(entry.getKey()).toString();
					String value_from_in = entry.getValue().toString();
					rem=false;
					if (value_from_in != null && value_from_vec != null && value_from_in.equals(value_from_vec)) {
						rem=true;
					}
					else {
						break;
					}
				}
				if(rem) {
				positions.add(j);
				vec.remove(j);
				j--;
				break;
				}
			}
			if (vec.size() >= length)
				full = true;
			else full=false;
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dir + filename));
			out.writeObject(this);
			out.close();
		return positions;
	}
	public static void main(String[] args[]) {

	}



}
