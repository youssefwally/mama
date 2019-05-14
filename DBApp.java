

package mama;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Stack;
import java.util.Vector;
import java.io.InputStream;


public class DBApp {
	static FileWriter fileWriter;
	static Vector<table> tab = new Vector<table>();
	static int length;
	static int lengthI;

	public static void init() throws IOException {
		fileWriter = new FileWriter(Page.dirmeta + "metadata.csv");
		Properties prop = new Properties();
		String propFileName = "config\\DBApp.properties";
		InputStream inputStream =new FileInputStream(propFileName);
		prop.load(inputStream);
		length=Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		lengthI=Integer.parseInt(prop.getProperty("BitmapSize"));
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws IOException{
		try{
			table k = new table(htblColNameType, strTableName, strClusteringKeyColumn);
			tab.add(k);
			k.writeCsvFile();
		}catch (DBAppException e){
			e.print();
		}
	}

	public static void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, FileNotFoundException, IOException {
		try{
			boolean exists = false;
			for (int i = 0; i < tab.size(); i++) 
			{
				if (tab.elementAt(i).tableName.equals(strTableName))
				{
					exists = true;	
					tab.elementAt(i).insert(htblColNameValue);
				}
			}
			
			if(!exists)
				throw new DBAppException("Table does not exists");
		}catch (DBAppException e){
			e.print();
		}
	}

	public static void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try 
		{
			boolean exists = false;
			for (int i = 0; i < tab.size(); i++) 
			{
				if (tab.elementAt(i).tableName.equals(strTableName)) {
					tab.elementAt(i).update(strKey, htblColNameValue);
					exists = true;
				}
			}
			if(!exists)
				throw new DBAppException("The table you are looking for does not exist");
		}catch(DBAppException e )
		{
			e.print();
		}
	}

	public static void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, FileNotFoundException, IOException {
		try {
			boolean exists = false;
			for (table t : tab) 
			{
					if (t.tableName.equals(strTableName)) 
					{
						t.delete(htblColNameValue);
						exists = true;
					}
			}
			if(!exists)
				throw new DBAppException("The Table you are looking for does not exist");
		}catch(DBAppException e)
		{
			e.print();
		}
			
	}
	
	public static void createBitmapIndex(String strTableName,String strColName) throws DBAppException, FileNotFoundException, IOException {
		try
		{
			if(strColName.equals(""))
				throw new DBAppException("strColName Cannot be Empty");
			boolean exists = false;
			for (table t : tab) {
			if (t.tableName.equals(strTableName)) {
				try {
					t.createbitmap(strColName);
					exists = true;
				} catch (DBAppException e) {
					e.print();
				}
			}
		}
			if(!exists)
				throw new DBAppException("table does not exist");
	}catch(DBAppException e)
		{
		 e.print();
		}
	}
	
	

	// -------------------------------------------Reyad DOWN------------------------------------------------------------------------------------------
		public static int getTotal(String tableName) {
			int i = 0;
			for (table tb : tab) {
				if (tb.tableName.equals(tableName))
					i = tb.total;
			}
			return i;
		}

		static int Prec(String operand) {
			switch (operand) {
			case "AND":
				return 3;
			case "XOR":
				return 2;
			case "OR":
				return 1;
			}
			return -1;
		}

		public static Stack<String> toPostfix(ArrayList<String> a) {
			Stack<String> resultStack = new Stack<String>();
			Stack<String> stack = new Stack<String>();

			for (int i = 0; i < a.size(); i++) {
				String top = a.get(i);
				if (!(top.equals("AND") || a.get(i).equals("OR") || top.equals("XOR")))
					resultStack.push(top);

				if (top.equals("AND") || a.get(i).equals("OR") || top.equals("XOR")) {
					while (!stack.isEmpty() && Prec(top) <= Prec(stack.peek()))
						resultStack.push(stack.pop());
					stack.push(top);
				}

			}
			while (!stack.isEmpty())
				resultStack.push(stack.pop());
			return resultStack;
		}

		static String evaluated(Stack<String> a) {
			Stack<String> stack = new Stack<String>();
			int length = a.peek().length();
			while (!a.isEmpty()) {
				String top = a.pop();
				if (!(top.equals("AND") || top.equals("OR") || top.equals("XOR")))
					stack.push(top);
				else {
					ArrayList<String> operands = new ArrayList<String>();
					operands.add(stack.pop());
					operands.add(stack.pop());
					switch (top) {
					case "AND":
						stack.push(AND(operands, length));
						break;
					case "OR":
						stack.push(OR(operands, length));
						break;
					case "XOR":
						stack.push(XOR(operands, length));
						break;
					}
				}
			}
			return stack.peek();
		}

		public static Iterator<Hashtable<String, Object>> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
			if(strarrOperators.length <= 0)
				throw new DBAppException("starrOperators cannot be empty");
			boolean exists = false;
			ArrayList<String> infix = new ArrayList<String>();
			ArrayList<String> resultedOperands = new ArrayList<String>();
			try{
				for (table t : tab) {
					for (int i = 0; i < arrSQLTerms.length; i++) {
						if (t.tableName == arrSQLTerms[i]._strTableName) {
							resultedOperands.add(t.selectFromPage(arrSQLTerms[i]));
							exists = true;
						}
					}
				}
				if(!exists)
					throw new DBAppException("Table does not exist");
			for (int i = 0; i < strarrOperators.length; i++) {
				infix.add(resultedOperands.get(i));
				infix.add(strarrOperators[i]);
			}
			infix.add(resultedOperands.get(arrSQLTerms.length - 1));
			System.out.println("Query "+"\n"+infix+"\n");
			Stack<String> postfix = toPostfix(infix);
			System.out.println("Postfix notation "+"\n"+postfix+"\n");
			Stack<String> postfixReversed = new Stack<String>();
			while (!postfix.isEmpty()) {
				postfixReversed.push(postfix.pop());
			}
			String bitmapResult = evaluated(postfixReversed);
			System.out.println("bitmap=" + bitmapResult+"\n");
			ArrayList<Hashtable<String, Object>> m = findRecords(bitmapResult, arrSQLTerms[0]._strTableName);
			return m.iterator();
		}catch(Exception e)
			{
			if(e instanceof IOException)
			{
				DBAppException z = new DBAppException("Faulty Input");
				z.print();
			}
			if(e instanceof FileNotFoundException || e instanceof ClassNotFoundException)
			{
				DBAppException z = new DBAppException("The file you are looking for cannot be located");
				z.print();
			}
			if(e instanceof DBAppException)
				((DBAppException)e).print();
			}
			return null;
		}
		static ArrayList<Hashtable<String, Object>> findRecords(String bitmap, String tableName1)
				throws ClassNotFoundException, IOException {
			table table1 = null;
			for (table t : tab) {
				if (t.tableName.equals(tableName1))
					table1 = t;
			}
			ArrayList<Hashtable<String, Object>> result = new ArrayList<Hashtable<String, Object>>();
			int tillNow = table1.countPerP.get(0);
			int currentPage = 0;
			int offset = 0;
			for (int i = 0; i < bitmap.length(); i++) {
				if (i >= tillNow) {
					currentPage++;
					tillNow += table1.countPerP.get(currentPage);
				}
				if ((bitmap.charAt(i) + "").equals("1")) {
					result.add(table.deSerialization(table1.tab.get(currentPage)).getVec().get(offset));
				}
				offset = (offset + 1 >= table1.countPerP.get(currentPage)) ? 0 : ++offset;
			}
			return result;
		}

		public static String OR(ArrayList<String> a, int total) {
			int i = 0;
			String out = "";
			if (a.size() == 0) {
				for (int m = 0; m < total; m++) {
					out += "0";
				}
				return out;
			} else
				out = a.get(0);

			for (int j = 0; j < a.get(0).length(); j++) {
				while (i < a.size() - 1) {
					int k = ((Integer.parseInt((out.charAt(j) + "")) + Integer.parseInt((a.get(i + 1).charAt(j) + ""))) >= 1
							? 1
							: 0);
					out = out.substring(0, j) + k + out.substring(j + 1, a.get(0).length());
					i++;
				}
				i = 0;
			}
			return out;
		}

		public static String XOR(ArrayList<String> a, int total) {
			int i = 0;
			String out = "";
			if (a.size() == 0) {
				for (int m = 0; m < total; m++) {
					out += "0";
				}
				return out;
			} else
				out = a.get(0);
			for (int j = 0; j < a.get(0).length(); j++) {
				while (i < a.size() - 1) {
					int k = (((Integer.parseInt((out.charAt(j) + "")) + Integer.parseInt((a.get(i + 1).charAt(j) + "")))
							% 2 != 0) ? 1 : 0);
					out = out.substring(0, j) + k + out.substring(j + 1, a.get(0).length());
					i++;
				}
				i = 0;
			}
			return out;
		}

		public static String AND(ArrayList<String> a, int total) {
			int i = 0;
			String out = "";
			if (a.size() == 0) {
				for (int m = 0; m < total; m++) {
					out += "0";
				}
				return out;
			} else
				out = a.get(0);
			for (int j = 0; j < a.get(0).length(); j++) {
				while (i < a.size() - 1) {
					if (Integer.parseInt(a.get(i).charAt(j) + "") == 0) {
						int k = 0;
						k = 0;
						out = out.substring(0, j) + k + out.substring(j + 1, a.get(0).length());
						break;
					} else {
						int k = (Integer.parseInt((out.charAt(j) + ""))
								+ Integer.parseInt((a.get(i + 1).charAt(j) + "")) >= 2 ? 1 : 0);
						out = out.substring(0, j) + k + out.substring(j + 1, a.get(0).length());
						i++;
						break;
					}
				}
				i = 0;
			}
			return out;
		}

		// -----------------------------------------Reyad UP----------------------------------------------------------------------------------------------

		public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException {
			init();
			String strTableName = "Student";
			Hashtable htblColNameType = new Hashtable();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			createTable(strTableName, "id", htblColNameType);
			createTable(strTableName, "id", htblColNameType);
			

		//	createBitmapIndex( strTableName, "name" );

			createBitmapIndex( strTableName, "name" );

			createBitmapIndex( strTableName, "gpa" );
			System.out.println("is bitmap working?");
			
			createBitmapIndex( strTableName, "" );
			System.out.println("Wareeni keda");
			
			Hashtable htblColNameType90 = new Hashtable();
			htblColNameType90.put("id2", "java.lang.Integer");
			htblColNameType90.put("name2", "java.lang.String");
			htblColNameType90.put("gpa2", "java.lang.double");
			createTable(strTableName+"1", "id", htblColNameType90);
			
			System.out.println("TAb w keda?");
			deleteFromTable(strTableName, new Hashtable());
			System.out.println("GAmed ya ged3an");

			Hashtable htblColNameValue3 = new Hashtable();
			htblColNameValue3.put("id", new Integer(7));
			htblColNameValue3.put("name", new String("k"));
			htblColNameValue3.put("gpa", new Double(0.95));
			insertIntoTable(strTableName, htblColNameValue3);
			Hashtable htblColNameValue4 = new Hashtable();
			htblColNameValue4.put("id", new Integer(3));
			htblColNameValue4.put("name", new String("c"));
			htblColNameValue4.put("gpa", new Double(1.25));
			insertIntoTable("Wassup", htblColNameValue4);
					
			
			System.out.println("Hngrb insert error keda");
			Hashtable htblColNameValue6 = new Hashtable();
			htblColNameValue6.put("id", new Integer(1));
			htblColNameValue6.put("name", new String("a"));
			htblColNameValue6.put("gpa", new Double(0.95));
			insertIntoTable(strTableName, new Hashtable());
			Hashtable htblColNameValue5 = new Hashtable();
			htblColNameValue5.put("id", new Integer(8));
			htblColNameValue5.put("name", new String("f"));
			htblColNameValue5.put("gpa", new Double(0.95));
			insertIntoTable(strTableName, htblColNameValue5);
			System.out.println(table.deSerialization("data\\Student\\pages\\p0").getVec().toString());
			System.out.println(table.deSerialization("data\\Student\\pages\\p1").getVec().toString());
			Hashtable htblColNameValue2 = new Hashtable();
			htblColNameValue2.put("id", new Integer(9));
			htblColNameValue2.put("name", new String("i"));
			htblColNameValue2.put("gpa", new Double(0.95));
			insertIntoTable(strTableName, htblColNameValue2);
			Hashtable htblColNameValue7 = new Hashtable();
			htblColNameValue7.put("id", new Integer(10));
			htblColNameValue7.put("name", new String("g"));
			htblColNameValue7.put("gpa", new Double(1.9));
			insertIntoTable(strTableName, htblColNameValue7);
			Hashtable htblColNameValue9 = new Hashtable();
			htblColNameValue9.put("id", new Integer(4));
			htblColNameValue9.put("name", new String("d"));
			htblColNameValue9.put("gpa",new Double(1.1));
			insertIntoTable(strTableName, htblColNameValue9);
			Hashtable htblColNameValue10 = new Hashtable(); 
		
		
			htblColNameValue10.put("id", new Integer(5352));
			htblColNameValue10.put("name", new String("waly"));
			htblColNameValue10.put("gpa", new Double("0.7"));
			updateTable(strTableName,"1", htblColNameValue10);
		
		//	 System.out.println(tab.elementAt(0).tab.elementAt(1).vec);

			Hashtable htblColNameValue99 = new Hashtable();
			htblColNameValue99.put("id", new Integer(1));
			htblColNameValue99.put("name", new String("waly"));
			htblColNameValue99.put("gpa", new Double(0.7));
		insertIntoTable(strTableName, htblColNameValue99);

			

		System.out.println(" =============================*==================================");
			System.out.println(table.deSerialization("data\\Student\\pages\\p0").getVec().toString());
			System.out.println(table.deSerialization("data\\Student\\pages\\p1").getVec().toString());
		System.out.println(table.deSerialization("data\\Student\\pages\\p2").getVec().toString());
			System.out.println(" ==============================*=================================");
			System.out.println(table.deSerializationI("data\\Student\\indexes\\gpa\\i0").toString());
		System.out.println(table.deSerializationI("data\\Student\\indexes\\gpa\\i3").toString());
			System.out.println(" ===============================================================");
			Hashtable htblColNameValue8 = new Hashtable();
			htblColNameValue8.put("name", new String("a"));		
			htblColNameValue8.put("gpa", new Double(0.95));		
			deleteFromTable(strTableName, htblColNameValue8);

		
			
		
			System.out.println(table.deSerialization("data\\Student\\pages\\p0").getVec().toString());
			System.out.println(table.deSerialization("data\\Student\\pages\\p1").getVec().toString());
			System.out.println(table.deSerialization("data\\Student\\pages\\p2").getVec().toString());
			System.out.println();
			System.out.println(table.deSerializationI("data\\Student\\indexes\\gpa\\i0").toString());
			//System.out.println(table.deSerializationI("data\\Student\\indexes\\gpa\\i3").toString());
			//System.out.println(table.deSerializationI("data\\Student\\indexes\\id\\i2").toString());
			
			
			

			//----------------------------------------Selection Testing-----------------------------------------------------------
			SQLTerm[] arrSQLTerms;
			String[] strarrOperators = new String[2];
			strarrOperators[0] = "AND";
			strarrOperators[1] = "AND";
//			strarrOperators[2] = "AND";
			arrSQLTerms = new SQLTerm[3];
			arrSQLTerms[0] = new SQLTerm();
			arrSQLTerms[1] = new SQLTerm();
			arrSQLTerms[2] = new SQLTerm();
//			arrSQLTerms[3] = new SQLTerm();
			arrSQLTerms[0]._strTableName = "Student";
			arrSQLTerms[0]._strColumnName = "gpa";
			arrSQLTerms[0]._strOperator = "<=";
			arrSQLTerms[0]._objValue = new Double(1.0);
			arrSQLTerms[1]._strTableName = "Student";
			arrSQLTerms[1]._strColumnName = "id";
			arrSQLTerms[1]._strOperator = "!=";
			arrSQLTerms[1]._objValue = new Integer(1);
			arrSQLTerms[2]._strTableName = "Student";
			arrSQLTerms[2]._strColumnName = "id";
			arrSQLTerms[2]._strOperator = "!=";
			arrSQLTerms[2]._objValue = new Integer(7);
//			arrSQLTerms[3]._strTableName = "Student";
//			arrSQLTerms[3]._strColumnName = "name";
//			arrSQLTerms[3]._strOperator = "=";
//			arrSQLTerms[3]._objValue = "mortada";
			Iterator<?> iterator = selectFromTable(arrSQLTerms, strarrOperators);
			System.out.println("select result:");
			while (iterator.hasNext()) {
				System.out.println(iterator.next());
				iterator.remove();
		//--------------------------------------End of Selection Testing-------------------------------------------------------
				}
		}
}

