package mama;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import mama.DBApp;
import mama.DBAppException;

public class table implements Serializable{
	Vector<String> tab = new Vector<String>();
	static Vector<String> tabI = new Vector<String>();
	Vector<Integer> countPerP = new Vector<>();
	static int i = 0;
	int total = 0;
	String tableName;
	String strClusteringKeyColumn;
	Hashtable<String, String> info;

	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
	private static final String FILE_HEADER = "Table Name, Column Name, Column Type, Key, Indexed";
	String dir = "data\\";
	static String dirmeta = "data\\";
	String dirI = "data\\";

	public table(Hashtable<String, String> info, String tableName, String strClusteringKeyColumn) throws DBAppException{
		
		String res = "";
		
		if(tableName.isEmpty())
			throw new DBAppException("Table Name Cannot Be Empty");
		if(strClusteringKeyColumn.isEmpty())
			throw new DBAppException("No Clustering Key");
		if(info.isEmpty())
			throw new DBAppException("Column Type must be Defined");
		for(int i = 0; i<DBApp.tab.size(); i++)
		{
			res+= DBApp.tab.get(i).tableName;
			if(res.contains(tableName))
				throw new DBAppException("Table Already Exists");
		}
		
		this.info = info;
		this.tableName = tableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		dir += tableName + "\\";
		dirI += tableName + "\\";
		File folder = new File(dirI);
		dir += "pages\\";
		dirI += "indexes\\";
		File folder2 = new File(dir);
		File folder3 = new File(dirI);
		if (folder.mkdir()) {
			//For the sake of testing no errors are thrown
			System.out.println("Directory Created");
		} else {
			System.out.println("Directory is not created");
			//throw new DBAppException("Directory is not Created");
		}
		if (folder2.mkdir()) {
			System.out.println("Directory Created");
		} else {
			System.out.println("Directory is not created");
			//throw new DBAppException("Directory is not Created");
		}
		if (folder3.mkdir()) {
			System.out.println("Directory Created");
		} else {
			System.out.println("Directory is not created");
			//throw new DBAppException("Directory is not Created");
		}
		System.out.println(dirI);
		// File folder2 = new File(dir);
	}

	public static Page deSerialization(String file) throws IOException, ClassNotFoundException {
		FileInputStream fileInputStream = new FileInputStream(file);
		BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
		ObjectInputStream objectInputStream = new ObjectInputStream(bufferedInputStream);
		Page vector = (Page) objectInputStream.readObject();
		objectInputStream.close();

		return vector;

	}

	public static PageI deSerializationI(String file) throws IOException, ClassNotFoundException {
		FileInputStream fileInputStream = new FileInputStream(file);
		BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
		ObjectInputStream objectInputStream = new ObjectInputStream(bufferedInputStream);
		PageI vector = (PageI) objectInputStream.readObject();
		objectInputStream.close();

		return vector;

	}

	public void insert(Hashtable<String, Object> row) throws DBAppException {
		try {
			if(row.isEmpty())
				throw new DBAppException("Values cannot be empty");
			ArrayList<String[]> tableChars=table.readCsvFile(this.tableName);
			Enumeration<String> b = row.keys();
			boolean accept = true;
			for(int i=0;i<tableChars.size();i++) {
				String k = tableChars.get(i)[1];
				b = row.keys();
				boolean found = false;
				while (b.hasMoreElements()) {
					String z=b.nextElement();
					if (k.equals(z)) {
						if(!(row.get(z).getClass()+"").substring(6).toLowerCase().equals(tableChars.get(i)[2].toLowerCase())) {
							throw new DBAppException("Column Types Do not Match");
							// here we throw exception
						}
						found = true;
					}
				}
				if (!found) {
					row.put(k, "");
				}
				found = false;
				// System.out.println(k);
				
			}
			int f=0;
			int i=0;
			boolean put=false;
			boolean remove=false;
			Hashtable<String, Object> last=null ;
			
			boolean primaryIndexed = false;
			for(int j=0;j<tableChars.size();j++) {
				if(tableChars.get(j)[3].equals("true")&&tableChars.get(j)[4].equals("true")) {
					primaryIndexed = true;
				}
			}
			if(primaryIndexed) {
				String bitmap="";
				boolean lastPage=true;
//			for(int z=0;z<tabI.size();z++) {
//				if(tabI.get(z).contains(strClusteringKeyColumn)) {
//					PageI here=deSerializationI(tabI.get(z));
//					Vector<Entry> values=here.getVec();
//					for(int j=0;j<values.size();j++) {
//						if(!compareTo2(values.get(j).getValue(),row.get(strClusteringKeyColumn))) {
//							bitmap=values.get(j).getBitmap().bits;
//							lastPage=false;
//							break;
//						}
//					}
//				}
//				if(!lastPage)
//					break;
//				
//		}
				System.out.println(row.get(strClusteringKeyColumn)+"   lolll");
				int indexedPageIndex=binarySearch(strClusteringKeyColumn, row.get(strClusteringKeyColumn));
				System.out.println("indexedid "+indexedPageIndex +" *************");
				ArrayList<String> myPathes=new ArrayList<>();
				for(int p=0;p<tabI.size();p++) {
					if(tabI.get(p).contains(strClusteringKeyColumn)) {
						myPathes.add(tabI.get(p));
					}
				}
				System.out.println(myPathes+"   hereeeeeeeeeeee");
				if(indexedPageIndex!=-1) {
					PageI here=deSerializationI(myPathes.get(indexedPageIndex));
					System.out.println(" here "+ here);
					Vector<Entry> values=here.getVec();
					for(int j=0;j<values.size();j++) {
						System.out.println(values.get(j).getValue() +"              "+row.get(strClusteringKeyColumn));
						if(!compareTo2(values.get(j).getValue(),row.get(strClusteringKeyColumn))) {
							System.out.println("loooooooooooooooooooool");
							bitmap=values.get(j).getBitmap().getBits();
							lastPage=false;
							break;
						}
					}
				}
				int position=0;
				System.out.println("Bitmap:     "+ bitmap);
				if(lastPage) {
					position=total;
				}
				else 
					for(position=0;position<bitmap.length();position++) {
						if(bitmap.charAt(position)=='1') {
							break;
						}
					}
				System.out.println(countPerP);
				//System.out.println(total);
				System.out.println(position);
				int pageNum=0;
				boolean isItReally=false;
				for(pageNum=0;pageNum<countPerP.size();pageNum++) {
					position=position-countPerP.get(pageNum);
					if(position<0) {
						isItReally=true;
						break;
					}
				}
				if(!isItReally&&pageNum>0&&countPerP.get(countPerP.size()-1)!=DBApp.length) {
					
					pageNum--;
				}
				System.out.println(pageNum+" lerelerelerlerllelrler");
				i=pageNum;
				
				if(pageNum<tab.size()) {
					Page now = deSerialization(tab.get(pageNum));
					// System.out.println(now.full);
					
					if (now.check(strClusteringKeyColumn, row)) {
						if (now.full) {
							last = now.getVec().get(Page.length - 1);
							f=now.insert(row, strClusteringKeyColumn);
							remove=true;
							accept=false;
							put=true;
							total--;
						} else {
							countPerP.set(pageNum, countPerP.get(pageNum)+1);
							System.out.println(countPerP.toString()+"a");
							f=now.insert(row, strClusteringKeyColumn);
							accept=false;
						}
						
					} else {
						if (i == tab.size() - 1 && !now.full) {
							f=	now.insert(row, strClusteringKeyColumn);
							accept=false;
							countPerP.set(pageNum, countPerP.get(pageNum)+1);
						}
						
					}
					//	if(accept) i=tab.size();
				}
			}
			else
				for ( i = 0; i < tab.size(); i++) {
					Page now = deSerialization(tab.get(i));
					// System.out.println(now.full);
					
					if (now.check(strClusteringKeyColumn, row)) {
						if (now.full) {
							last = now.getVec().get(Page.length - 1);
							f=now.insert(row, strClusteringKeyColumn);
							remove=true;
							//	this.insert(last);
							accept=false;
							put=true;
							total--;
							break;
						} else {
							countPerP.set(i, countPerP.get(i)+1);
							//	System.out.println(countPerP.toString()+"a");
							f=now.insert(row, strClusteringKeyColumn);
							accept=false;
							break;
						}
						
					} else {
						if (i == tab.size() - 1 && !now.full) {
							f=	now.insert(row, strClusteringKeyColumn);
							accept=false;
							countPerP.set(i, countPerP.get(i)+1);
							break;
						}
						
					}
				}
			
			//	int pos=(i)*3+f;
			int pos=f;
			for(int p=0;p<i;p++) {
				pos=pos+countPerP.get(p);
			}
			if(accept) {
				countPerP.add(1);
				Page ne = new Page(info,this.tableName);
				ne.insert(row, strClusteringKeyColumn);
				tab.add(dir + ne.filename);
				i++;
			}
			
			//from here---------------------------------------------------------------------------
			
			ArrayList<String[]> tableChars2 = table.readCsvFile(this.tableName);
			boolean found = false;
			for (int r = 0; r < tableChars2.size(); r++) {
				String k = tableChars2.get(r)[1];
				String a = tableChars2.get(r)[4];
				
				
				if (((a.equals("true")))) {
					//	System.out.println("indexed");
					found = true;
					
					// here we throw exception
				} else {
					//	System.out.println("not indexed");
					found = false;
					
				}
				Boolean ins=false;
				if(found) {
					//	System.out.println(69);
					//	Boolean ins=false;
					for(int j=0;j<tabI.size();j++) {
						if(tabI.get(j).contains(tableChars2.get(r)[1])) {
							PageI now=deSerializationI(tabI.get(j));
							if(remove) {
								//edit pos to real position
								int position=0;
								for(int z=0;z<i+1;z++) {
									//		System.out.println(countPerP.toString());
									position=position+countPerP.get(z);
									//		System.out.println(countPerP.get(z)+" kk ");
								}
								System.out.println(position+"*(");
								now.remove(position-1);
							}
							
							ins=now.insert( row.get(tableChars2.get(r)[1]),pos,total,ins);
						}
					}
					boolean inss =true;
					boolean a5eran=false;
					if(!ins) {
						for(int z=0;z<tabI.size();z++) {
							if(tabI.get(z).contains(tableChars2.get(r)[1])) {
								PageI here=deSerializationI(tabI.get(z));
								if(here.checkPos(row.get(tableChars2.get(r)[1]))) {
									
									if(!here.full) {
										inss=false;
										here.insertEntry(row.get(tableChars2.get(r)[1]),pos,total+1);
										a5eran=true;
										break;
									}else {
										boolean please=false;
										PageI temp=here;
										boolean overflow=false;
										PageI here2=deSerializationI(tabI.get(z));
										for(int pNum=z+1;pNum<tabI.size();pNum++) {
											if(tabI.get(pNum).contains(tableChars2.get(r)[1])) {
												overflow=false;
												please=true;
												temp=here2;
												here2=deSerializationI(tabI.get(pNum));
												if(!here2.full) {
													here2.insertfirst(temp.getLastEntry());
													a5eran=true;
													break;
												}else {
													overflow=true;
													System.out.println("heeere");
													here2.insertfirst(temp.removeLastEntry());
												}
											}
										}
										if(overflow) {
											PageI ne = new PageI(tableChars2.get(r)[1],this.tableName);
											ne.put(here2.getVec().get(3));
											here2.removeLastEntry();
											
											a5eran=true;
											tabI.add(dirI + tableChars2.get(r)[1]+"/"+ne.filename);
										}
										if(please) {
											here.removeLastEntry();
											here.insertEntry(row.get(tableChars2.get(r)[1]), pos, total+1);
											a5eran=true;
											break;
										}
									}
									
									break;
									
								}
							}
						}
						
						if(!a5eran) {
							int lastocc=-1;
							boolean wegotit=false;
							if(inss) {
								for(int z=0;z<tabI.size();z++) {
									if(tabI.get(z).contains(tableChars2.get(r)[1])) {
										lastocc=z;
									}
								}
								if(lastocc!=-1) {
									PageI here=deSerializationI(tabI.get(lastocc));
									if(!here.full) {
										here.insertLast(row.get(tableChars2.get(r)[1]), pos, total+1);
										wegotit=true;
									}else {
										PageI ne = new PageI(tableChars2.get(r)[1],this.tableName);
										if(here.checkPos(row.get(tableChars2.get(r)[1]))) {
											ne.put(here.getVec().get(2));
											here.removeLastEntry();
											
											here.insertEntry(row.get(tableChars2.get(r)[1]), pos, total+1);
											
											wegotit=true;
											tabI.add(dirI + tableChars2.get(r)[1]+"/"+ne.filename);
										}
										else {
											ne.insertEntry(row.get(tableChars2.get(r)[1]), pos, total+1);
											wegotit=true;
											tabI.add(dirI + tableChars2.get(r)[1]+"/"+ne.filename);
										}
										
									}
									
								}
							}
							if(!wegotit) {
								PageI ne = new PageI(tableChars2.get(r)[1],this.tableName);
								ne.insert2(row.get(tableChars2.get(r)[1]),pos,total+1);
								tabI.add(dirI + tableChars2.get(r)[1]+"/"+ne.filename);
							}
						}
						
					}
				}
				
			}
			if(put) {
				System.out.println("error here");
				this.insert(last);
			}
			
			total++;
		}catch(Exception e)
		{
			if(e instanceof IOException)
				throw new DBAppException("Faulty Input");
			if(e instanceof FileNotFoundException || e instanceof ClassNotFoundException)
				throw new DBAppException("The file you are looking for cannot be located");
			if(e instanceof DBAppException)
				throw new DBAppException(e.getMessage());
			
		}
	}
		


	public void update(String strKey, Hashtable<String, Object> in) throws DBAppException {
		try
		{
			if(in.isEmpty())
				throw new DBAppException("Must Supply a value");
			for (int i = 0; i < tab.size(); i++) {
			Page now = deSerialization(tab.get(i));
			Object[] a;
			int i1 = -1;
			String col = strClusteringKeyColumn;
			Vector<Hashtable<String, Object>> vec = now.getVec();
			for (int i11 = 0; i11 < vec.size(); i11++) {
				if (("" + vec.get(i11).get(col)).equals("" + strKey)) {
					Enumeration<String> keyss = vec.get(i11).keys();
					a = vec.get(i11).values().toArray();
					while(keyss.hasMoreElements()) {
						i1++;
						String c = keyss.nextElement();
						if(!(in.containsKey(c))) {
							in.put((c),a[i1]);
							vec.set(i11, in);
						//	vec.get(i11).put("TouchDate", LocalDateTime.now());
							System.out.println(in.toString());
						}
						//System.out.println(i1); 
					}
				}
			}
		}
		Hashtable htblColNameValue88 = new Hashtable();
		htblColNameValue88.put("id", new Integer(strKey));
		System.out.println(htblColNameValue88);
		System.out.println(in);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		delete(htblColNameValue88);
		insert(in);
	} catch(Exception e)
		{
		if(e instanceof IOException)
			throw new DBAppException("Faulty Input");
		if(e instanceof FileNotFoundException || e instanceof ClassNotFoundException)
			throw new DBAppException("The file you are looking for cannot be located");
		if(e instanceof DBAppException)
			throw new DBAppException(e.getMessage());
		}
	}
	
	public void delete2(Hashtable<String, Object> in) throws ClassNotFoundException, IOException {
		ArrayList<String[]> tableChars2 = table.readCsvFile(this.tableName);
		boolean found = false;
		int totalBefore=0;
		for (int i = 0; i < tab.size(); i++) {
			Page now = deSerialization(tab.get(i));
			ArrayList<Integer> poss = new ArrayList<>();
			poss = now.delete(in);
			for(int j=0;j<poss.size();j++) {
				System.out.println("*");
				poss.set(j,poss.get(j)+totalBefore);
			}
			for (int r = 0; r < tableChars2.size(); r++) {

				String k = tableChars2.get(r)[1];
				String a = tableChars2.get(r)[4];

				if (((a.equals("true")))) {
					System.out.println("indexed");
					found = true;
				} else {
					System.out.println("not indexed");
					found = false;

				}
				System.out.println(totalBefore);
				System.out.println(poss);
				Boolean ins = false;
				if (found) {
					for (int j = 0; j < tabI.size(); j++) {
						if (tabI.get(j).contains(tableChars2.get(r)[1])) {
							PageI now2 = deSerializationI(tabI.get(j));
							for (int z = 0; z < poss.size(); z++) {
								System.out.println("loooooooooool");
								now2.remove2(poss.get(z)-z);
							}
							if(now2.getVec().isEmpty()) {
								Files.deleteIfExists(Paths.get(tabI.get(j)));
								tabI.remove(j);
							}

						}
					}

				}
			

			}
			totalBefore+=countPerP.get(i);
			if (now.getVec().size() == 0) {
				Files.deleteIfExists(Paths.get(tab.get(i)));
				tab.remove(i);
			}
		}
	}
	
	public void delete(Hashtable<String, Object> in) throws DBAppException 
	{
		try{
			if(in.isEmpty())
				throw new DBAppException("Must supply values");
			ArrayList<String[]> specifiedTable = table.readCsvFile(this.tableName);
			boolean isIndexed = false;
			String indexChecker;
			String inString = in.toString();
			ArrayList<Integer> positionsOfRowsToBeDeleted = new ArrayList<>();
			ArrayList<String> indexedColumns = new ArrayList<String>();
			ArrayList<String> indexedPages = new ArrayList<String>();
			ArrayList<String> candidateRows = new ArrayList<String>();
			
			//Checks which columns are indexed if any.
			for(int r = 0; r<specifiedTable.size();r++) 
			{
				indexChecker = specifiedTable.get(r)[4];
				//	System.out.println(indexChecker + r);
				if (indexChecker.equals("true"))
				{
					//	System.out.println(specifiedTable.get(r)[1]);
					//	System.out.println(in.toString());
					if(inString.contains(specifiedTable.get(r)[1]))
					{
						indexedColumns.add(specifiedTable.get(r)[1]);
						isIndexed = true;
					}
				}
			}
			
			if(isIndexed)
			{
				//gets serialized indexed pages and adds them to indexedPages
				for(int j = 0; j<tabI.size(); j++)
				{
					for(int r = 0; r<specifiedTable.size();r++)
					{
						if(tabI.get(j).contains(specifiedTable.get(r)[1]));
						indexedPages.add(tabI.get(j));
					}
				}
				
				//get bits from PageI so that they could be anded together
				for(int ict = 0; ict<indexedColumns.size(); ict++) //ict = indexedColumnTraverser
				{
					for(int i = 0; i<indexedPages.size(); i++)
					{
						if(indexedPages.get(i).contains(indexedColumns.get(ict))) {
							PageI indexedPage = deSerializationI(indexedPages.get(i));
							for(int entries = 0; entries<indexedPage.getVec().size(); entries++)
							{
								if(Equal2(in.get(indexedColumns.get(ict)),(indexedPage.getVec().get(entries).getValue())))
									candidateRows.add(indexedPage.getVec().get(entries).getBitmap().getBits());	
							}
						}
					}
				}
				System.out.println(candidateRows);
				String myBitmap =	andBitsTogether(candidateRows);
				String theReal="";
				Vector<Integer> countPerP2= (Vector<Integer>) countPerP.clone();
				//	deleteUsingBits(candidateRows.get(0), in);
				for(int i=0;i<myBitmap.length();i++) {
					if(myBitmap.charAt(i)=='1') {
						int pageNum=0;
						int position=i;
						for(pageNum=0;pageNum<countPerP.size();pageNum++) {
							position=position-countPerP.get(pageNum);
							if(position<0) 
								break;
						}
						
						Page now = deSerialization(tab.get(pageNum));
						
						ArrayList<Integer> poss = new ArrayList<>();
						poss = now.delete2(in);
						if(poss.size()>0) {
							theReal+="1";
							countPerP2.set(pageNum, countPerP2.get(pageNum)-1);
							total--;
						}else {
							theReal+="0";
						}
						System.out.println(poss);
						if(now.getVec().isEmpty()) {
							Files.deleteIfExists(Paths.get(tab.get(pageNum)));
							tab.remove(pageNum);
							countPerP2.remove(pageNum);
							
						}
						
					}else {
						theReal+="0";
					}
				}
				countPerP=countPerP2;
				System.out.println(countPerP +"                 999999999999999999");
				System.out.println(tab);
				Boolean ins = false;
				for (int j = 0; j < tabI.size(); j++) {
					PageI now2 = deSerializationI(tabI.get(j));
					System.out.println("first timeee");
					//for (int z = 0; z < poss.size(); z++) {
					now2.overwrite(theReal);
					//		i--;
					//}
					if(now2.getVec().isEmpty()) {
						Files.deleteIfExists(Paths.get(tabI.get(j)));
						tabI.remove(j);
						j--;
					}
				}
				
				
			}
			
			else
			{
				ArrayList<String[]> tableChars2 = table.readCsvFile(this.tableName);
				boolean found = false;
				int totalBefore=0;
				for (int i = 0; i < tab.size(); i++) 
				{
					Page now = deSerialization(tab.get(i));
					ArrayList<Integer> poss = new ArrayList<>();
					poss = now.delete(in);
					for(int j=0;j<poss.size();j++)
					{
						System.out.println("*");
						poss.set(j,poss.get(j)+totalBefore);
					}
					for (int r = 0; r < tableChars2.size(); r++)
					{
						
						String k = tableChars2.get(r)[1];
						String a = tableChars2.get(r)[4];
						
						if (((a.equals("true")))) 
						{
							System.out.println("indexed");
							found = true;
						} else {
							System.out.println("not indexed");
							found = false;
							
						}
						System.out.println(totalBefore);
						System.out.println(poss);
						Boolean ins = false;
						if (found) 
						{
							for (int j = 0; j < tabI.size(); j++) 
							{
								if (tabI.get(j).contains(tableChars2.get(r)[1]))
								{
									PageI now2 = deSerializationI(tabI.get(j));
									for (int z = 0; z < poss.size(); z++) 
									{
										now2.remove2(poss.get(z));
										countPerP.set(j, countPerP.get(j)-1);
										total--;
									}
									if(now2.getVec().isEmpty())
									{
										Files.deleteIfExists(Paths.get(tabI.get(j)));
										tabI.remove(j);
										countPerP.remove(j);
									}
									
								}
							}
							
						}
						
						
					}
					totalBefore+=countPerP.get(i);
					if (now.getVec().size() == 0)
					{
						Files.deleteIfExists(Paths.get(tab.get(i)));
						tab.remove(i);
					}
				}
		} 
		}catch(Exception e)
		{
			if(e instanceof IOException)
				throw new DBAppException("Faulty Input");
			if(e instanceof FileNotFoundException || e instanceof ClassNotFoundException)
				throw new DBAppException("The file you are looking for cannot be located");
			if(e instanceof DBAppException)
				throw new DBAppException(e.getMessage());
		}
	}
	
	//Helper Method
	private void deleteUsingBits(String bits, Hashtable<String, Object> in)
	{
		int end;
		int start = 0;
		
		
		for(int p = 0; p<tabI.size(); p++)
		{
			for(int j = 0; j<countPerP.size(); j++)
			{
				
			}
		}
		
	}
	

	public void createbitmap(String strColName) throws DBAppException {
		try{
			if(strColName.equals(""))
				throw new DBAppException("strColName cannot be empty");
		BufferedReader fileReader = null;
		ArrayList<String[]> table = new ArrayList();

		// Create a new list of student to be filled by CSV file data
		String last = "";

		String line = "";

		// Create the file reader
		fileReader = new BufferedReader(new FileReader(Page.dirmeta + "metadata.csv"));

		File folder = new File(dirI + strColName);
		if (folder.mkdir()) {
			System.out.println("Directory Created");
		} else {
			System.out.println("Directory is not created");
		}

		// Read the file line by line starting from the second line
		while (true) {
			line = fileReader.readLine();
			if (line == null) {
				break;
			}

			// Get all tokens available in line
			String[] tokens = line.split(COMMA_DELIMITER);
			// System.out.println(tokens[0]);
			if (tokens[0].equals(tableName) && tokens[1].equals(strColName)) {
				// Create a new student object and fill his data
				line = line.substring(0, line.length() - 5);
				line = line + "true";
			}
			last = last + "\n" + line;
		}
		last = last + "\n";
		PrintWriter writer = new PrintWriter(Page.dirmeta + "metadata.csv");
		writer.print(last);
		writer.close();
		if(total>0)
		createIndexColumn2(this.tableName,strColName);
		}catch(Exception e){
			if(e instanceof IOException)
				throw new DBAppException("Faulty Input");
			if(e instanceof FileNotFoundException || e instanceof ClassNotFoundException)
				throw new DBAppException("The files needed for BitMap creation cannot be located");
			if(e instanceof DBAppException)
				throw new DBAppException(e.getMessage());
		
		}
	}


	

	public void writeCsvFile() {
		Enumeration<String> a = info.keys();
		Collection<String> b = info.values();
		Object[] c = b.toArray();
		int i = 0;
		try {
			DBApp.fileWriter.append(FILE_HEADER.toString());
			DBApp.fileWriter.append(NEW_LINE_SEPARATOR);
			while (a.hasMoreElements()) {
				String d = "false";
				String f = this.strClusteringKeyColumn;
				String g = a.nextElement();
				if (f.equals(g)) {
					d = "true";
				} else
					d = "false";
				DBApp.fileWriter.append(String.valueOf(this.tableName));
				DBApp.fileWriter.append(COMMA_DELIMITER);
				DBApp.fileWriter.append(g);
				DBApp.fileWriter.append(COMMA_DELIMITER);
				DBApp.fileWriter.append(String.valueOf(c[i]));
				DBApp.fileWriter.append(COMMA_DELIMITER);
				DBApp.fileWriter.append(d);
				DBApp.fileWriter.append(COMMA_DELIMITER);
				DBApp.fileWriter.append(String.valueOf("false"));
				DBApp.fileWriter.append(NEW_LINE_SEPARATOR);
				i++;
			}

			System.out.println("CSV file was created successfully !!!");

		} catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {

			try {
				DBApp.fileWriter.flush();

			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter !!!");
				e.printStackTrace();
			}

		}
	}

	@SuppressWarnings({ "resource" })
	public static ArrayList<String[]> readCsvFile(String tableName) throws IOException {

		BufferedReader fileReader = null;
		ArrayList<String[]> table = new ArrayList<String[]>();

		// Create a new list of student to be filled by CSV file data

		String line = "";

		// Create the file reader
		fileReader = new BufferedReader(new FileReader(Page.dirmeta + "metadata.csv"));

		// Read the CSV file header to skip it
		fileReader.readLine();

		// Read the file line by line starting from the second line
		while ((line = fileReader.readLine()) != null) {
			// Get all tokens available in line
			String[] tokens = line.split(COMMA_DELIMITER);
			// System.out.println(tokens[0]);
			if (tokens[0].equals(tableName)) {
				// Create a new student object and fill his data
				table.add(tokens);
				// System.out.println(table);
				// System.out.println(69);
			}
		}

		return table;

	}
	
	

	
	public Vector<ArrayList<Object>> createIndexColumn2(String tableName , String colName) throws IOException, ClassNotFoundException {
		ArrayList<Object> values = new ArrayList();
		//Loop on table to load pages
		for(int i=0 ; i<tab.size();i++) {
		
			Page page = deSerialization(tab.get(i));
			
			//Loop on each row to get the value of the colName
			for(int y=0;y<page.getVec().size();y++) {
				if(page.getVec().get(y).get(colName)==null)
					System.out.println("Column not found");
				else
					values.add(page.getVec().get(y).get(colName));
			}
			
		}
		//Eliminate duplicate values 
		ArrayList<Object> noDuplicates = new ArrayList();
		for (Object element : values) {
			if(!noDuplicates.contains(element))
				noDuplicates.add(element);
		}
		
		//Sort values in the array
		noDuplicates=this.sort2(noDuplicates);
		//System.out.println(noDuplicates);
		
		Vector<ArrayList<Object>> output = new Vector();
		for(int a =0; a<noDuplicates.size();a++) {
			String index = "";
			//Load all tuples to check on the current value
			for(int b=0 ; b<tab.size();b++) {
				Page page = deSerialization(tab.get(b));
				for(int c=0;c<page.getVec().size();c++) {
					//If value found append 1
					System.out.println(page.getVec().get(c).get(colName)+"      "+noDuplicates.get(a));
					if(Equal2(page.getVec().get(c).get(colName),noDuplicates.get(a)))
						index+="1";
					//If value is not found append 0
					else
						index+="0";
				}
			}
			//Insert (value,index) in the output Hashtable
			ArrayList<Object> ent=new ArrayList<>();
			ent.add(noDuplicates.get(a));
			ent.add(index);
			output.add(ent);
		}
		System.out.println(output);
		for(int i=0;i<output.size();i=i+DBApp.lengthI) {
			if(i%DBApp.lengthI==0) {
				
				PageI ne=new PageI(colName, tableName);
				for(int j=i;j<i+DBApp.lengthI&&j<output.size();j++) {
					Entry k=new Entry(output.get(j).get(0), ""+output.get(j).get(1));
					ne.getVec().add(k);
				}
				tabI.add(dirI + colName+"/"+ne.filename);
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dirI +colName+"/"+ ne.filename));
				out.writeObject(ne);
				out.close();
			}
		}
		return output;
		
	}
	
	private ArrayList<Object> sort2(ArrayList<Object> noDuplicates) {
		// TODO Auto-generated method stub
		ArrayList<Object> result=new ArrayList<>();
		if(noDuplicates.get(0) instanceof Integer) {
			ArrayList<Integer> noDups=new ArrayList<>();
			for(int i=0;i<noDuplicates.size();i++) {
				noDups.add((Integer) noDuplicates.get(i));
			}
			Collections.sort(noDups);
			result.addAll(noDups);
		}else if(noDuplicates.get(0) instanceof Double) {
			ArrayList<Double> noDups=new ArrayList<>();
			for(int i=0;i<noDuplicates.size();i++) {
				noDups.add((Double) noDuplicates.get(i));
			}
			Collections.sort(noDups);
			result.addAll(noDups);
		}else {
			ArrayList<String> noDups=new ArrayList<>();
			for(int i=0;i<noDuplicates.size();i++) {
				noDups.add(""+noDuplicates.get(i));
			}
			Collections.sort(noDups);
			result.addAll(noDups);
		}
		return result;
	}

	public static boolean compareTo2(Object a, Object b) {
		if (a instanceof Integer ) {
			if ((int) a < (int) b) {
				return true;
				}
		}
			else if (a instanceof Double ){
						if ((double) a < (double) b) 
							return true;
					
			} else {
						if ((("" + a).compareTo("" + b) < 0)) {
							return true;
						}
				
			}
		return false;				
	}

	public static boolean Equal2(Object a, Object b) {
		if (a instanceof Integer) {
			if ((int) a == (int) b) {
				return true;
				}
		}
			else if (a instanceof Double) {
						if ((double) a == (double) b) 
							return true;
					
			} else {
						if ((((String) a).compareTo((String)b)== 0)) {
							return true;
						}
				
			}
		return false;				
	}
	
	public int binarySearch(String colName,Object value) throws ClassNotFoundException, IOException{
		Vector<String> myIndices=new Vector<>();
		for(int i=0;i<tabI.size();i++) {
			if(tabI.get(i).contains(colName)) {
				myIndices.add(tabI.get(i));
			}
		}
		
		int l = 0, r = myIndices.size() - 1; 
		while(l<=r) {
			int m = l+(r-1)/2;
			PageI now=deSerializationI(myIndices.get(m));
			if(now.isHere(value)==0) {
				System.out.println("mah ahooo");
				return m;
			}
			else {
				if(now.isHere(value)==1) {
					l=m+1;
				}
				else {
					r=m-1;
				}
			}
			
		}
		if(r<0&& tab.size()>0) {
			return 0;
		}
		System.out.println(myIndices.size() +" mashyyyyyyyy");
		
		return myIndices.size()-1;
	}
	
	
	public String andBitsTogether(ArrayList<String> listOfBits)
	{	
		String x;
		String y;
		System.out.println(listOfBits.toString());
		//System.out.println(listOfBits.get(0));
		while(listOfBits.size()>=2)
		{
			System.out.println(listOfBits.toString() + " before anding");
			x = listOfBits.get(0);
			
			y = listOfBits.get(1);
			ArrayList<String> k=new ArrayList<>();
			k.add(x);
			k.add(y);
			String res=DBApp.AND(k, total);
			listOfBits.remove(0);
			listOfBits.remove(0);
			System.out.println(res + " before anding kollo");
			listOfBits.add(0,res);
			
			System.out.println(listOfBits.toString() + " after anding");
		}
		return listOfBits.get(0);
		
	}
	
	//---------------------------------Reyad DOWN--------------------------------------------------------
		public String selectFromPage(SQLTerm arrSQLTerms) throws IOException, ClassNotFoundException, DBAppException {
			ArrayList<String[]> e = readCsvFile(tableName);
			ArrayList<String> newIndex = new ArrayList<String>();
			ArrayList<String> positions = new ArrayList<String>();
			boolean index = false;
			for (int r = 0; r < e.size(); r++) {
				if ((arrSQLTerms._strColumnName.equals(e.get(r)[1])) && ((e.get(r)[4]).equals("false"))) {
						createbitmap(arrSQLTerms._strColumnName);
						newIndex.add(arrSQLTerms._strColumnName);
				}
			}
			for (int r = 0; r < e.size(); r++) {
				if (arrSQLTerms._strColumnName.equals(e.get(r)[1])) {
					String a = e.get(r)[4];
					if (((a.equals("true")))) {
						index = true;
					} else {
						index = false;
					}
					if (index) {
						for (int j = 0; j < tabI.size(); j++) {
							if (tabI.get(j).contains(arrSQLTerms._strColumnName)) {
								PageI now2 = deSerializationI(tabI.get(j));
								for (int i = 0; i < now2.getVec().size(); i++) {
									int compare = now2.getVec().get(i).compareTo(arrSQLTerms._objValue);
									switch (arrSQLTerms._strOperator) {
									case ("="):
										if (compare == 0) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case (">="):
										if (compare == 0 || compare == 1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case ("<="):
										if (compare == 0 || compare == -1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case ("!="):
										if (compare != 0) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case (">"):
										if (compare == 1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case ("<"):
										if (compare == -1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										if (compare == 0) {
											break;
										}
										break;
									}
								}
							}
						}
					}
/////////////////////////////turn to index/////////////////////////////////
					else {
						for (int j = 0; j < tabI.size(); j++) {
							if (tabI.get(j).contains(arrSQLTerms._strColumnName)) {
//								System.out.println("********************wwwaly************************");
//								System.out.println(tabI.get(j));
//								System.out.println(arrSQLTerms._strColumnName);
//								System.out.println("******************wwwaly**************************");
								PageI now2 = deSerializationI(tabI.get(j));
								for (int i = 0; i < now2.getVec().size(); i++) {
									int compare = now2.getVec().get(i).compareTo(arrSQLTerms._objValue);
									switch (arrSQLTerms._strOperator) {
									case ("="):
										if (compare == 0) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case (">="):
										if (compare == 0 || compare == 1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case ("<="):
										if (compare == 0 || compare == -1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case ("!="):
										if (compare != 0) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case (">"):
										if (compare == 1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										break;
									case ("<"):
										if (compare == -1) {
											positions.add(now2.getVec().get(i).getBitmap().getBits());
										}
										if (compare == 0) {
											break;
										}
										break;
									}
								}
							}
						}
					}
/////////////////////////////turn to index/////////////////////////////////
				}
			}
//			for(int i = 0;i<newIndex.size();i++) {
//			String directoryName = "F:\\GUC\\DB\\Project 1\\M2 10h left\\DB\\data\\Student\\indexes\\" + newIndex.get(i);
//			File directory = new File(directoryName);
//			File[] files = directory.listFiles();
//			for (File file : files){
//			if (!file.delete())
//			{
//			System.out.println("Failed to delete "+file);
//			}
//			}
//			directory.delete();
//			newIndex.clear();
//			}
			return OR(positions);
		}

		public static int getTotal(String tableName) {
			int i = 0;
			for (table tb : DBApp.tab) {
				if (tb.tableName.equals(tableName))
					i = tb.total;
			}
			return i;
		}

		public String OR(ArrayList<String> a) {
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
	//---------------------------------Reyad UP----------------------------------------------------------
}
