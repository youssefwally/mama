package mama;

import java.io.Serializable;

public class Entry implements Serializable,Comparable{
private Object value;
private Bitmap bitmap;



public Object getValue() {
	return value;
}

public void setValue(String value) {
	this.value = value;
}

public Bitmap getBitmap() {
	return bitmap;
}

public void setBitmap(Bitmap bitmap) {
	this.bitmap = bitmap;
}

public Entry(Object object,String k) {
	this.value=object;
	bitmap=new Bitmap(k);
}

	
	public Entry(Object object,int k) {
		this.value=object;
		bitmap=new Bitmap(k);
	}

	public void put(boolean b, int pos) {
		// TODO Auto-generated method stub
		bitmap.put(b,pos);
	}

	public void remove(int pos) {
		// TODO Auto-generated method stub
		bitmap.remove(pos);
	}
	
	
	public int compareTo(Object a) {
		if (a instanceof Integer) {
			if ((Integer) this.value > (Integer) a)
				return 1;
			else if (((Integer) this.value - (Integer) a) == 0)
				return 0;
			else
				return -1;

		}
		if (a instanceof Double) {
			if ((Double) this.value > (Double) a)
				return 1;
			else if (((Double) this.value - (Double) a) == 0)
				return 0;
			else
				return -1;
		}

		if (a instanceof String) {
			if (((String) this.value).compareToIgnoreCase((String) a) > 0)
				return 1;
			else if (((String) this.value).compareToIgnoreCase((String) a) == 0)
				return 0;
			else
				return -1;

		}
		System.out.println(99);
		return 99;
	}

}


