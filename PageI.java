package mama;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class PageI implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2453284975721691303L;


	
	String dir = "data\\";
	final static String dirmeta = "data\\";
	String dirI = "data\\";
	
	int lengthNow=0;
	int noC=0;
	String columnName;
	private Vector<Entry> vec;
	String filename = "i";
	static int i = -1;
	Hashtable<String, String> ht;
	final static int length = DBApp.lengthI;
	int k = 0;
	boolean full;
	String all = "";
	String max;
	String min;
	
	
	public PageI(String colName,String tableName) {
		dirI+=tableName+"\\indexes\\";
		columnName=colName;
		this.vec=new Vector<>();
		i++;
		filename =filename+ i;
		full=false;
		// vec.setSize(length);
	}
	
	public Vector<Entry> getVec() {
		return vec;
	}
	public Boolean insert(Object row,int pos, int total, Boolean ins) throws FileNotFoundException, IOException {
		Boolean k=ins;
		
		for(int i=0;i<vec.size();i++) {
			if(table.Equal2(row,vec.get(i).getValue())) {
		//		System.out.println(999);
				k=true;
				vec.get(i).put(true,pos);
				System.out.println("here");
				//vec.get(i).
			}
			else {
			vec.get(i).put(false,pos);
		
			}
		}
		noC++;
		if(vec.size()==length) {
			full=true;
		
		}

		//System.out.println(this.i+"**");
		for(int i = 0 ;i<vec.size();i++)
		System.out.println(vec.get(i).getValue()+" "+vec.get(i).getBitmap().getBits().toString());
		System.out.println();
		write();
		return k;
	}

	public void insert2(Object row, int pos,int total) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		this.vec.add(new Entry(row,total-1));
		insert(row,pos,total-1,false);
		write();
	}

	public void remove(int pos) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		
		for(int i=0;i<vec.size();i++) {
			
			
				vec.get(i).remove(pos);
			
				}
		write();
			
	}
	
	public void remove2(int pos) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		int k=vec.size();
		for(int i=0;i<vec.size();i++) {
			
			vec.get(i).remove(pos);	
			if(!vec.get(i).getBitmap().getBits().contains("1")) {
				vec.remove(i);
				i--;
				}
			
		}
		write();	
			
	}
	
	public void setVec(Vector<Entry> vec) {
		this.vec = vec;
	}


	public boolean checkPos(Object inserted) {
		// TODO Auto-generated method stub
		for(int i=0;i<vec.size();i++) {
			if(!table.compareTo2(vec.get(i).getValue(),inserted)) {
				return true;
			}

		}
		return false;
	}

	public void insertEntry(Object inserted, int pos, int total) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		int tot=vec.size();
		boolean putted=true;
		for(int i=0;i<tot;i++) {
		//	System.out.println("loop");
			if(!table.compareTo2(vec.get(i).getValue(),inserted)) {
				Entry here=new Entry(inserted,total-1);
				this.vec.add(i,here);
				vec.get(i).put(true,pos);
			//	insert(inserted,pos,total-1,false);
				putted=false;
				break;
				//here.put(true, pos);
			}
		}
		if(vec.size()==0) {
			vec.add(new Entry(inserted,total-1));
			vec.get(0).put(true,pos);
			putted=false;
		}
		if(putted) {
			Entry here=new Entry(inserted,total-1);
			vec.add(length-1,here);
			vec.get(length-1).put(true, pos);
		}
		for(int i = 0 ;i<vec.size();i++)
			System.out.println(vec.get(i).getValue()+" "+vec.get(i).getBitmap().getBits().toString());
			System.out.println();
			if(vec.size()>length) {
				full=true;
			}
			write();	
	}

	public Entry getLastEntry() throws FileNotFoundException, IOException {
	//	System.out.println(vec.size()+"               aaaaaaaaaaaaaa");
		if(vec.size()<length) {
			return vec.get(vec.size()-1);
		}
		// TODO Auto-generated method stub
		Entry a=vec.get(length-1);
		vec.remove(a);
	// System.out.println(a.getValue());
		lengthNow--;
		full=false;
		write();	
		return a;
	}

	public void insertfirst(Entry lastEntry) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		vec.add(0,lastEntry);
		if(vec.size()>length-1) {
			full=true;
		}
		write();
	}
	
	public void put(Object value) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		vec.add((Entry) value);
		for(int k = 0 ;k<vec.size();k++)
			System.out.println(vec.get(k).getValue()+" "+vec.get(k).getBitmap().getBits().toString());
		if(vec.size()>=length)
			full=true;
		System.out.println();
		write();	
	}

	public Entry removeLastEntry() throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		Entry temp=vec.get(vec.size()-1);
		vec.remove(vec.size()-1);
		if(vec.size()<length) {
			full=false;
		}
		write();
		return temp;
	}

	public void insertLast(Object object, int pos, int total) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		this.vec.add(new Entry(object,total-1));
		this.vec.get(vec.size()-1).put(true, pos);
		write();
	}

	public String toString() {
		String x="";
		for(int k = 0 ;k<vec.size();k++)
			x+=vec.get(k).getValue()+" "+vec.get(k).getBitmap().getBits().toString()+" \n";
		x=x+"\n";
		return x;
	}

	public int isHere(Object value) {
		Object max=vec.get(vec.size()-1).getValue();
		Object min =vec.get(0).getValue();
		System.out.println(min+" "+value+" "+max);
		if(table.compareTo2(min, value)&&table.compareTo2(value, max)) {
			System.out.println("*********************");
			return 0;
		}else {
			if(table.compareTo2(max, value)) {
				return 1;
			}else {
				return -1;
			}
		}

	}

	public void overwrite(String myBitmap) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
	
		for(int i=0;i<vec.size();i++) {
			int pos=0;
			for(int j=0;j<myBitmap.length();j++) {
				
				//System.out.println(myBitmap);

				if(myBitmap.charAt(j)=='1') {
					vec.get(i).remove(pos);
					pos--;
					}
				pos++;
			}
			System.out.println(vec.get(i).getBitmap().getBits());
			if(!vec.get(i).getBitmap().getBits().contains("1")) {
				vec.remove(i);
				i--;
				}

			
		}	
		write();
	}
	
	public void write() throws FileNotFoundException, IOException {

		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dirI +columnName+"/"+ filename));
		out.writeObject(this);
		out.close();
	}

}