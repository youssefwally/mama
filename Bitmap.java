package mama;

import java.io.Serializable;
import java.util.BitSet;

public class Bitmap implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient String bits="";
	private String rle="";
	
	
	public void encode() {
		String src =bits;
	 //     System.out.println("Input: " + src);
		StringBuilder dest = new StringBuilder();
		
		for (int i = 0; i < src.length(); i++) {
			dest.append(src.charAt(i));
			int cnt = 1;
			while (i + 1 < src.length() && src.charAt(i) == src.charAt(i + 1)) { // fixed order
				i++;
				cnt++;
			}
			dest.append(cnt);
		}
	      rle=dest.toString();
	}
	
	public void decode() {
		String src = rle;
		StringBuilder dest = new StringBuilder();
	//	System.out.println("before decoding: "+rle);
		for (int i = 0; i < src.length() - 1; i = i + 2) {
			char charAt = src.charAt(i);
			int cnt = src.charAt(i + 1) - '0';
			for (int j = 0; j < cnt; j++) {
				dest.append(charAt);
			}
		}
	//	System.out.println("decoded: "+dest.toString());
		bits=dest.toString();
		    
	}
	public Bitmap(int start) {
		for(int i=0;i<start;i++) {
			setBits(getBits() + "0");
		}
		this.encode();
	}
	
	public Bitmap(String start) {
		this.decode();
		setBits(start);
		this.encode();
	}
	

	public void put(boolean b, int pos) {
		
		this.decode();
		// TODO Auto-generated method stub
		String k="";
		if(b) {
			k="1";
		}
		else {
			k="0";
		}
		if(pos>getBits().length()) {
			setBits(getBits()+k);
		}
		else
		setBits(getBits().substring(0, pos)+k+getBits().substring(pos));
		this.encode();
	}

	public void remove(int pos) {
		this.decode();
		// TODO Auto-generated method stub
		System.out.println("position "+pos+" "+bits);
		setBits(getBits().substring(0, pos)+getBits().substring(pos+1));
	//	decode();
		this.encode();
	//	System.out.println(99998);
	}

	public String getBits() {
		this.decode();
		return bits;
	}

	public void setBits(String bits) {
		this.decode();
		this.bits = bits;
	//	decode();
		this.encode();
	}
}
