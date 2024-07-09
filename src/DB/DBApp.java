package DB;

/** * @author Wael Abouelsaadat */ 

import java.util.Iterator;
import java.util.Stack;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;


public class DBApp {
	
	private static Vector<Table> db;
	
	public DBApp( ){
		setDb(new Vector<Table>());
	}
	
	public static Vector<Table> getDb() {
		return db;
	}

	public static void setDb(Vector<Table> db) {
		DBApp.db = db;
	}
	
	public void init( ){
	}

	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException, IOException{
		
		String fileName = "./src/DB/"+strTableName+".ser"; // Assuming your file naming convention
	    File file = new File(fileName);
	    if (file.exists()) {
	    	throw new DBAppException("a table with the same table name has already been created");
	    }
        
        Stack<Object> s= new Stack<>();
        
        boolean clusterExists=false;
        boolean wrongDataType=false;
    	for (Enumeration<String> keys = htblColNameType.keys(); keys.hasMoreElements();) {
    		String column=(String)keys.nextElement();
    		s.push(column);
    		if ( htblColNameType.get(column).compareTo("java.lang.String")!=0 && htblColNameType.get(column).compareTo("java.lang.string")!=0 &&  htblColNameType.get(column).compareTo("java.lang.Integer")!=0 &&  htblColNameType.get(column).compareTo("java.lang.integer")!=0 &&  htblColNameType.get(column).compareTo("java.lang.Double")!=0 &&  htblColNameType.get(column).compareTo("java.lang.double")!=0) {
    			wrongDataType=true;
    		}
    		if (column.equals(strClusteringKeyColumn)) {
    			clusterExists=true;
    		}
    	}
    	
    	if (clusterExists==false) {
    		throw new DBAppException("the column you're trying to make a clustering key does not exist");
    	}
    	
    	if (wrongDataType==true) {
    		throw new DBAppException("you're trying to create a column of a data type other than string, integer or double");
    	}
    	
    	String csvFile = System.getProperty("user.dir")+"/src/DB/metadata.csv";
    	
    	ArrayList<String[]> meta = metaReader2();
		if (meta.size()==1) {
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
				for (int t = 0; t < htblColNameType.size(); t++) {
	            	String st=(String)s.pop();
	            	if (st.equals(strClusteringKeyColumn)) {
	            		writer.write(strTableName+","+st+","+htblColNameType.get(st)+",True,null,null");
	            		SerialCType.ser(htblColNameType.get(st),strTableName);
	            		SerialCId.ser(t, strTableName);
	            		SerialCName.ser(strClusteringKeyColumn, strTableName); //omar new delete
	            		
	        		}
	        		else {
	        			writer.write(strTableName+","+st+","+htblColNameType.get(st)+",False,null,null");
	        		}
	        		writer.newLine();
	            }
			}
			catch (IOException e) {
        		e.printStackTrace();
        	}
		}
		else {
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile,true))) {
				for (int t = 0; t < htblColNameType.size(); t++) {
	            	String st=(String)s.pop();
	            	if (st.equals(strClusteringKeyColumn)) {
	            		writer.write(strTableName+","+st+","+htblColNameType.get(st)+",True,null,null");
	            		SerialCType.ser(htblColNameType.get(st),strTableName);
	            		SerialCId.ser(t, strTableName);
	            		SerialCName.ser(strClusteringKeyColumn, strTableName);
	        		}
	        		else {
	        			writer.write(strTableName+","+st+","+htblColNameType.get(st)+",False,null,null");	
	        		}
	        		writer.newLine();
	            }
			}
			catch (IOException e) {
        		e.printStackTrace();
        	}
		}
    	
    	Table t= new Table(strTableName);
    	db.add(t);
    	SerialCount.ser(0, strTableName);
    	SerialTable.ser(t,strTableName);
	}


	public void createIndex(String strTableName,String strColName,String strIndexName) throws DBAppException, IOException{
				
	        ArrayList<String[]> meta = metaReader2();
	        
	        int i = 0;//i is the line in the csv holding the column that will be indexed
	        int j = 0;//j is the index of the column in the organized tuple
	        for (; i < meta.size(); i++) { //checks each array within meta
	            if (meta.get(i)[0].equals(strTableName)) {
	                if (meta.get(i)[1].equals(strColName)) {
	                    break;
	                }
	                j++;
	            }
	        }
	      
	        String fileName2 = "./src/DB/"+strTableName+".ser"; 
		    File file2 = new File(fileName2);
		    if (! (file2.exists())) {
		    	throw new DBAppException("you are trying to create an index on a table that doesn't exist");
		    }
		    
		    if (i==meta.size()) {
		    	throw new DBAppException("you are trying to create an index on column that doesn't exist in the table");
		    }
		    
		    String fileName3 = "./src/DB/"+strTableName+strColName+"B+Tree"+".ser"; // Assuming your file naming convention
		    File file3 = new File(fileName3);
		    if (file3.exists()) {
		    	throw new DBAppException("an index on this column has already been created");
		    }

	        BTree<String,String> bTree = new BTree<String,String>(strIndexName);	        
     
	        String fileName = "./src/DB/"+strTableName+"Count"+".ser";
	        File file = new File(fileName);
	        int size=0;
	        if (file.exists()) {
		    	size= SerialCount.deser(strTableName);
		    }
	        
	        for (int p = 0; p < size; p++) { 
	        	Page page=Serial.deser(p, strTableName);//deserialize all pages
	            for (int q = 0; q < page.getSize(); q++) { //inserts tuples into btree 
	                bTree.insert(""+(Comparable)page.getTuple(q).getElement(j), p+"-"+page.getTuple(q).getElement(SerialCId.deser(strTableName)));
	            }
	            Serial.ser(page, p, strTableName);
	        }
	        
	        SerialBTree.ser(bTree, strColName , strTableName);
	        
	        meta.get(i)[4] = strIndexName;
	        meta.get(i)[5] = "B+tree";
	        
	        String csvFile = System.getProperty("user.dir")+"/src/DB/metadata.csv";

	        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
	            for (int t = 0; t < meta.size(); t++) {
	                writer.write(meta.get(t)[0] + "," + meta.get(t)[1] + "," + meta.get(t)[2] + "," + meta.get(t)[3] + "," + meta.get(t)[4] + "," + meta.get(t)[5]);
	                writer.newLine();
	            }
	        }
	        catch (IOException e) {
	        	throw new DBAppException("not implemented yet");
	        }
	        
	}
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object>  htblColNameValue) throws DBAppException, IOException{
		String fileName = "./src/DB/"+strTableName+".ser"; //Assuming your file naming convention
		File file = new File(fileName);
		if (file.exists()) {
			Table.insertTable(strTableName, htblColNameValue); //recheck
		}
		else {
			throw new DBAppException("you are trying to insert into a table that doesn't exist");
		}
	}
	
	public void updateTable(String strTableName,String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException, IOException {
		String fileName = "./src/DB/"+strTableName+".ser"; // Assuming your file naming convention
	    File file = new File(fileName);
		if (file.exists()) {
			Table.updateInTable(strTableName, strClusteringKeyValue, htblColNameValue); //recheck
		}
		else {
			throw new DBAppException("you are trying to update into a table that doesn't exist");
		}
	}
	
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException{	
		String fileName = "./src/DB/"+strTableName+".ser"; // Assuming your file naming convention
	    File file = new File(fileName);
		if (file.exists()) {
			Table.deleteInTable(strTableName, htblColNameValue); //recheck
		}
		else {
			throw new DBAppException("you are trying to delete from a table that doesn't exist");
		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[]  strarrOperators) throws DBAppException, IOException{
//		if(arrSQLTerms.length==0||strarrOperators.length==0) {
//		System.out.println("erro occured");
//		return new Vector<Tuple>();
//	}
	
	if (arrSQLTerms.length==1 && strarrOperators.length==0 && (arrSQLTerms[0]._strColumnName.equals("null") || arrSQLTerms[0]._strColumnName.equals("")) &&  (arrSQLTerms[0]._strOperator.equals("null") || arrSQLTerms[0]._strOperator.equals("")) &&  arrSQLTerms[0]._objValue==null) {
		String TableName=arrSQLTerms[0]._strTableName;
		String fileName2 = "./src/DB/"+TableName+".ser"; 
	    File file2 = new File(fileName2);
	    if (! (file2.exists())) {
	    	throw new DBAppException("you are trying to select from a table that doesn't exist");
	    }
	    else {
	    	return select.tabletoVector(TableName).iterator();
	    }
	}
		
	ArrayList<String[]> meta=null;
	if(arrSQLTerms.length!=strarrOperators.length+1){
		throw new DBAppException("The number or operators should be less than the number of operands by 1");
	}
	String TableName=arrSQLTerms[0]._strTableName;
	meta = Table.metaReader();//do i use try and catch instead??
	for(int i=0;i<arrSQLTerms.length;i++) {
		String tableName = arrSQLTerms[i]._strTableName;
		String columnName = arrSQLTerms[i]._strColumnName;
		String operator = arrSQLTerms[i]._strOperator;
		Comparable value = arrSQLTerms[i]._objValue;
 	    int f = select.getI(tableName,columnName); 
 	    if(f==-1) {
 	    	throw new DBAppException("Table does not exits/ Trying to work on 2 different tables/ Trying to work on a column that doesn't exist in your table");
 	    }
		if(!(TableName.equals(tableName)&&meta.get(f)[1].equals(columnName)&&select.validOpperator(operator)&&meta.get(f)[2].equals(value.getClass().getName()))){
			throw new DBAppException("You inserted an operator other >,>=,<,<=,=,!=/ You inserted the wrong data type for a column");
		}
	}
	
	for (int i=0;i<strarrOperators.length;i++) {
		if (!(strarrOperators[i].equals("and") || strarrOperators[i].equals("or") || strarrOperators[i].equals("xor"))) {
			throw new DBAppException("You inserted an operator other and, or, xor");
		}
	}
		return select.selectFromTable2(arrSQLTerms, strarrOperators).iterator();
	}
	
	public static ArrayList<String []> metaReader2() throws IOException{
		String filePath = System.getProperty("user.dir")+"/src/DB/metadata.csv";
		ArrayList<String []> ret= new ArrayList<String []>();
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = br.readLine();
		while (line != null) {
			String[] sp = line.split(",");
			line = br.readLine();
			ret.add(sp);
		}
		br.close();
		return ret;
	}

	public static Table retTable(String tableName) { // 4.4 recheck
		return SerialTable.deser(tableName);
	}

	public static void main( String[] args ) throws DBAppException, IOException{
		
		SQLTerm[] a =new SQLTerm[1];
	    String[] b= {};
	    //a[1]=new SQLTerm("Student","id",">", 20);
	    //a[0]=new SQLTerm("Student","name","<","Ahmed");
	    //a[2]=new SQLTerm("Student","gpa",">",1.5);
	    //a[3]=new SQLTerm("Student","gpa","<",3.0);
	    a[0]=new SQLTerm("Student","id",">",35);
	    /*try {
			select.printF(selectFromTable(a,b));//sort the outputs
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		/*try {
			boolean Flag= false;
			table t= null;
			for(int i=0;i<5;i++) {
				if("student".equals("student")) {
					//t=db.get(i);
					
					String csvFile = "C:\\Users\\Dell\\Desktop\\metadata.csv";
					BufferedReader br = new BufferedReader(new FileReader(csvFile));
					String line = br.readLine();
					//while (line != null) {
						String[] sp = line.split(",");
						for (int j=0;j<sp.length;j++)
							System.out.println(sp[j]);
						line = br.readLine();
					//}
					br.close();

					Flag=true;
					break;
				}
				if(!Flag) {
					//exception
				}
				else {
					
				}
			}
		}
		catch (IOException e){
			 e.printStackTrace();
		}*/
		
		String strTableName = "Student";
		DBApp dbApp = new DBApp( );
		Hashtable htblColNameType = new Hashtable( );
		//htblColNameType.put("id", "java.lang.Double");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("age", "java.lang.Double");
		dbApp.createTable( strTableName, "gpa", htblColNameType);
		
		Hashtable htblColNameType2 = new Hashtable( );
		htblColNameType2.put("empId", "java.lang.Integer");
		htblColNameType2.put("empName", "java.lang.String");
		htblColNameType2.put("age", "java.lang.Integer");
		dbApp.createTable( "Employee", "empId", htblColNameType2);
		
		//ArrayList<String[]> meta = metaReader2();
		//System.out.println(meta.size());
		
		//System.out.println(metaReader2("C:\\\\Users\\\\Dell\\\\eclipse-workspace\\\\DBProject\\\\src\\\\DB\\\\metadata.csv"));
		
		dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
		//dbApp.createIndex( strTableName, "name", "nameIndex" );
		//dbApp.createIndex( strTableName, "age", "ageIndex" );
		//System.out.println(SerialBTree.deser("name", "Student"));
		//System.out.println(SerialBTree.deser("gpa", "Student"));
		//dbApp.createIndex( strTableName, "name", "nameIndex" );
		//System.out.println(SerialBTree.deser("name", "Student"));
		//dbApp.createIndex( strTableName, "id", "idIndex" );
		//dbApp.createIndex( "Employee", "empId", "empIdIndex" );
		
		//meta = metaReader2();
		//System.out.println(meta.size());
		
		//metaReader2("C:\\Users\\Dell\\eclipse-workspace\\DBProject\\src\\DB\\metadata.csv");
		
		//System.out.println(SerialCId.deser("Student"));
		//System.out.println(SerialCType.deser("Student"));
		
		//System.out.println(SerialCType.deser("Student"));
		//System.out.println(SerialCType.deser("Student"));
		//System.out.println(SerialCId.deser("Student"));*/
		
		
		/*Page p= new Page("test");
		Tuple t= new Tuple();
		t.addElement(100);
		t.addElement("hana");
		t.addElement(4);
		p.addElement(t);
		
		Tuple t2= new Tuple();
		t2.addElement(300);
		t2.addElement("hana");
		t2.addElement(4);
		p.addElement(t2);
		
		Tuple t3= new Tuple();
		t3.addElement(200);
		t3.addElement("hana");
		t3.addElement(4);
		p.addElement(t3);
		
		Table table= DBApp.retTable("Student");
		table.addPage(p);
		
		Page p1= new Page("test1");
		Tuple t1= new Tuple();
		t1.addElement(800);
		t1.addElement("omar");
		t1.addElement(4);
		p1.addElement(t1);
		table.addPage(p1);*/
		
		
	
		
		Hashtable htblColNameValue2 = new Hashtable( );
		htblColNameValue2.put("name", new String("d" ) );
		htblColNameValue2.put("id", new Integer( 20 ));
		htblColNameValue2.put("gpa", new Double( 4.0 ) ); //if Double(4) not Double(4.0) will work, ask
		//dbApp.insertIntoTable( "Student" , htblColNameValue2 );
		
		Hashtable htblColNameValue5 = new Hashtable( );
		htblColNameValue5.put("gpa", new Double( 5.0 ) );
		htblColNameValue5.put("name", new String("b" ) );
		htblColNameValue5.put("id", new Integer( 40 ));
		//dbApp.insertIntoTable( strTableName , htblColNameValue5 );
		
		Hashtable htblColNameValue3 = new Hashtable( );
		htblColNameValue3.put("id", new Integer(30));
		htblColNameValue3.put("gpa", new Double( 3.0 ) );
		htblColNameValue3.put("name", new String("c" ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue3 );
		
		/////////////////////////////////////
		Hashtable htblColNameValue222 = new Hashtable( );
		htblColNameValue222.put("name", new String("gewsaw" ) );
		htblColNameValue222.put("id", new Integer( 20 ));
		htblColNameValue222.put("gpa", new Double( 4.0 ) ); //if Double(4) not Double(4.0) will work, ask
		//dbApp.insertIntoTable( strTableName , htblColNameValue222 );
		
		Hashtable htblColNameValue2223 = new Hashtable( );
		htblColNameValue2223.put("name", new String("gewsaw" ) );
		htblColNameValue2223.put("id", new Integer( 10 ));
		htblColNameValue2223.put("gpa", new Double( 4.0 ) ); //if Double(4) not Double(4.0) will work, ask
		//dbApp.insertIntoTable( strTableName , htblColNameValue2223 );
		
		Hashtable htblColNameValue22234 = new Hashtable( );
		htblColNameValue22234.put("name", new String("gewsaw" ) );
		htblColNameValue22234.put("id", new Integer( 100 ));
		htblColNameValue22234.put("gpa", new Double( 4.0 ) ); //if Double(4) not Double(4.0) will work, ask
		//dbApp.insertIntoTable( strTableName , htblColNameValue22234 );
		
		Hashtable htblColNameValue222345 = new Hashtable( );
		htblColNameValue222345.put("name", new String("woookskds" ) );
		htblColNameValue222345.put("id", new Integer( 30 ));
		htblColNameValue222345.put("gpa", new Double( 9.0 ) ); //if Double(4) not Double(4.0) will work, ask
		//dbApp.insertIntoTable( strTableName , htblColNameValue222345 );
		/////////////////////////////////////
		
		Hashtable htblColNameValue4 = new Hashtable( );
		htblColNameValue4.put("id", new Integer( 50 ));
		htblColNameValue4.put("name", new String("z" ) );
		htblColNameValue4.put("gpa", new Double( 6.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue4 );
		
		Hashtable htblColNameValue41 = new Hashtable( );
		htblColNameValue41.put("id", new Integer( 60 ));
		htblColNameValue41.put("name", new String("zz" ) );
		htblColNameValue41.put("gpa", new Double( 6.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue41 );
		
		Hashtable htblColNameValue42 = new Hashtable( );
		htblColNameValue42.put("id", new Integer( 210 ));
		htblColNameValue42.put("name", new String("zzz" ) );
		htblColNameValue42.put("gpa", new Double( 6.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue42 );
		
		Hashtable htblColNameValue43 = new Hashtable( );
		htblColNameValue43.put("id", new Integer( 420 ));
		htblColNameValue43.put("name", new String("zzzz" ) );
		htblColNameValue43.put("gpa", new Double( 6.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue43 );
		
		Hashtable htblColNameValue44 = new Hashtable( );
		htblColNameValue44.put("id", new Integer( 200 ));
		htblColNameValue44.put("name", new String("za" ) );
		htblColNameValue44.put("gpa", new Double( 6.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue44 );
		
		Hashtable htblColNameValue6 = new Hashtable( );
		htblColNameValue6.put("id", new Integer( 190 ));
		htblColNameValue6.put("name", new String("g" ) );
		htblColNameValue6.put("gpa", new Double( 7.0 ) );
	    //dbApp.insertIntoTable( strTableName , htblColNameValue6 );
		
		Hashtable htblColNameValue = new Hashtable( );
		htblColNameValue.put("id", new Integer( 450));
		htblColNameValue.put("name", new String("aa" ) );
		htblColNameValue.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		Hashtable htblColNameValue100 = new Hashtable( );
		htblColNameValue100.put("id", new Integer( 500 ));
		htblColNameValue100.put("name", new String("omar" ) );
		htblColNameValue100.put("gpa", new Double( 8.0) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue100 );
		
		Hashtable htblColNameValue101 = new Hashtable( );
		htblColNameValue101.put("id", new Integer( 600 ));
		htblColNameValue101.put("name", new String("ab" ) );
		htblColNameValue101.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue101 );
		
		Hashtable htblColNameValue102 = new Hashtable( );
		htblColNameValue102.put("id", new Integer( 120 ));
		htblColNameValue102.put("name", new String("as" ) );
		htblColNameValue102.put("gpa", new Double( 8.0 ) );
        //dbApp.insertIntoTable( strTableName , htblColNameValue102 );
		
		Hashtable htblColNameValue103 = new Hashtable( );
		htblColNameValue103.put("id", new Integer( 125 ));
		htblColNameValue103.put("name", new String("ad" ) );
		htblColNameValue103.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue103 );
		
		Hashtable htblColNameValue105 = new Hashtable( );
		htblColNameValue105.put("id", new Integer( 250));
		htblColNameValue105.put("name", new String("ag" ) );
		htblColNameValue105.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue105 );
		
		Hashtable htblColNameValue106 = new Hashtable( );
		htblColNameValue106.put("id", new Integer( 455 ));
		htblColNameValue106.put("name", new String("ah" ) );
		htblColNameValue106.put("gpa", new Double( 1.0 ) );
	    //dbApp.insertIntoTable( strTableName , htblColNameValue106 );
		
		Hashtable htblColNameValue107 = new Hashtable( );
		htblColNameValue107.put("id", new Integer( 1000 ));
		htblColNameValue107.put("name", new String("ae" ) );
		htblColNameValue107.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue107 );
		
		Hashtable htblColNameValue108 = new Hashtable( );
		htblColNameValue108.put("id", new Integer( 320 ));
		htblColNameValue108.put("name", new String("omar" ) );
		htblColNameValue108.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue108 );
		
		Hashtable htblColNameValue109 = new Hashtable( );
		htblColNameValue109.put("id", new Integer( 1250 ));
		htblColNameValue109.put("name", new String("ahhh" ) );
		htblColNameValue109.put("gpa", new Double( 8.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue109 );
		
		Hashtable htblColNameValue110 = new Hashtable( );
		htblColNameValue110.put("id", new Integer(185));
		htblColNameValue110.put("name", new String("ayyy" ) );
		htblColNameValue110.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue110 );
		
		Hashtable htblColNameValue112 = new Hashtable( );
		htblColNameValue112.put("id", new Integer(230));
		htblColNameValue112.put("name", new String("alll" ) );
		htblColNameValue112.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue112 );
		
		Hashtable htblColNameValue113 = new Hashtable( );
		htblColNameValue113.put("id", new Integer(105));
		htblColNameValue113.put("name", new String("afds" ) );
		htblColNameValue113.put("gpa", new Double( 1.0 ) );
	    //dbApp.insertIntoTable( strTableName , htblColNameValue113 );
		
		Hashtable htblColNameValue115 = new Hashtable( );
		htblColNameValue115.put("id", new Integer(103));
		htblColNameValue115.put("name", new String("adfs" ) );
		htblColNameValue115.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue115 );
		
		Hashtable htblColNameValue116 = new Hashtable( );
		htblColNameValue116.put("id", new Integer(99));
		htblColNameValue116.put("name", new String("aggg" ) );
		htblColNameValue116.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue116);
		
		Hashtable htblColNameValue1160 = new Hashtable( );
		htblColNameValue1160.put("id", new Integer(990));
		htblColNameValue1160.put("name", new String("aggg" ) );
		htblColNameValue1160.put("gpa", new Double( 1.0 ) );
		//dbApp.insertIntoTable( strTableName , htblColNameValue1160);
		
		Hashtable htblColNameValue1090 = new Hashtable( );
		htblColNameValue1090.put("gpa", new Double( 9.0 ) );
		htblColNameValue1090.put("name", new String("hana" ) );
		htblColNameValue1090.put("id", new Integer( 60 ));
		//dbApp.updateTable("Student","60",htblColNameValue1090 );
		
		/*Hashtable htblColNameValue11600 = new Hashtable( );
		htblColNameValue11600.put("gpa", new Double( 1.0 ) );
		htblColNameValue11600.put("id", new Integer(990));
		dbApp.insertIntoTable( strTableName , htblColNameValue11600);*/
		
		/*Hashtable htblColNameValue116000 = new Hashtable( );
		htblColNameValue116000.put("gpa", new Double( 1.0 ) );
		htblColNameValue116000.put("id", new Integer(80876));
		htblColNameValue116000.put("name", new String("hana"));
		dbApp.insertIntoTable( strTableName , htblColNameValue116000);*/
		
		/*Hashtable htblColNameValue116000 = new Hashtable( );
		//htblColNameValue116000.put("gpa", new Double( 1.0 ) );
		htblColNameValue116000.put("id", new Integer(80876));
		htblColNameValue116000.put("name", new String("omar"));
		dbApp.updateTable(strTableName,"80876",htblColNameValue116000 );*/
		
		Hashtable htblColNameValue116000 = new Hashtable( );
		//htblColNameValue116000.put("gpa", new Double( 1.0 ) );
		//htblColNameValue116000.put("id", new Integer(80876));
		//htblColNameValue116000.put("name", new String("hana"));
		//htblColNameValue116000.put("gpa", new Double( 1.0 ) );
		//dbApp.deleteFromTable("Student" , htblColNameValue116000);
		
		/*Hashtable htblColNameValue1130 = new Hashtable( );
		htblColNameValue1130.put("id", new Integer(60));
		htblColNameValue1130.put("gpa", new Double( 7.0 ) );
		htblColNameValue1130.put("name", new String("c" ) );
		dbApp.updateTable(strTableName,"c",htblColNameValue1130 );*/
		
		//dbApp.deleteFromTable(strTableName , htblColNameValue109);
		//dbApp.deleteFromTable(strTableName , htblColNameValue2);
		
		/*Hashtable htblColNameValue11600 = new Hashtable( );
		htblColNameValue11600.put("name", new String("d" ) );
		//htblColNameValue11600.put("gpa", new Double( 6.0 ) );
		htblColNameValue11600.put("id", new Integer(20));
		dbApp.deleteFromTable( strTableName , htblColNameValue11600);*/
		
		/*Hashtable htblColNameValue109 = new Hashtable( );
		htblColNameValue109.put("name", new String("ae" ) );
		htblColNameValue109.put("id", new Integer( 1000 ));
		dbApp.deleteFromTable(strTableName , htblColNameValue109);*/
		
		//Hashtable htblColNameValue12041 = new Hashtable();
		//htblColNameValue12041.put("id", new Integer( 99 ));
		  //htblColNameValue12041.put("gpa", new Double(1.0));
		//htblColNameValue12041.put("name", new String("aggg"));
		//htblColNameValue12041.put("id", new Integer(1250));
		//dbApp.deleteFromTable(strTableName, htblColNameValue12041);
		
		/*Hashtable htblColNameValue103 = new Hashtable( );
		htblColNameValue103.put("id", new Integer( 1300 ));
		htblColNameValue103.put("name", new String("ap" ) );
		htblColNameValue103.put("gpa", new Double( 7.0 ) );
		dbApp.insertIntoTable( strTableName , htblColNameValue103 );*/
		
		  /*Hashtable htblColNameValue441 = new Hashtable( );
		htblColNameValue441.put("gpa", new Double( 7.0 ) );
		htblColNameValue441.put("id1", new Integer( 200 ));
		htblColNameValue441.put("id1", new Integer( 200 ));
		htblColNameValue441.put("id1", new Integer( 200 ));
		htblColNameValue441.put("gpa2", new Double( 7.0 ) );
		dbApp.updateTable(strTableName,"200",htblColNameValue441 );*/
		
		Hashtable htblColNameValue21 = new Hashtable( );
		htblColNameValue21.put("empId", new Integer(2 ) );
		htblColNameValue21.put("age", new Integer( 21 ));
		htblColNameValue21.put("empName", new String( "employee hana" ) ); //if Double(4) not Double(4.0) will work, ask
		//dbApp.insertIntoTable( "Employee" , htblColNameValue21 );
		
		/*Hashtable htblColNameValue1090 = new Hashtable( );
		htblColNameValue1090.put("name", new String("Mohamed" ) );
		htblColNameValue1090.put("id", new Integer(40)) ;
		htblColNameValue1090.put("gpa", new Double( 5.0 ) );*/
	    //dbApp.updateTable("Student","40",htblColNameValue1090 );
		
		Hashtable htblColNameValue51 = new Hashtable( );
		//htblColNameValue51.put("name", new String("hsnssssssa" ) );
		//htblColNameValue51.put("id", new Integer(40)) ;
		//htblColNameValue51.put("gpa", new Double( 8.0) );
		//dbApp.deleteFromTable(strTableName , htblColNameValue51);

		
		
		
		Hashtable htblColNameValue10129 = new Hashtable( );
		htblColNameValue10129.put("name", new String("Hana" ) );
		htblColNameValue10129.put("id", new Integer( 15 ));
		htblColNameValue10129.put("gpa", new Double( 2.0 ));
		htblColNameValue10129.put("age", new Double( 20.0 ));
		//dbApp.insertIntoTable("Student" , htblColNameValue10129);
		
		Hashtable htblColNameValue101291 = new Hashtable( );
		htblColNameValue101291.put("name", new String("David" ) );
		htblColNameValue101291.put("id", new Integer( 20 ));
		htblColNameValue101291.put("gpa", new Double( 3.0 ));
		htblColNameValue101291.put("age", new Double( 20.0 ));
		//dbApp.insertIntoTable("Student" , htblColNameValue101291);
		
		Hashtable htblColNameValue101292 = new Hashtable( );
		htblColNameValue101292.put("name", new String("Adham" ) );
		htblColNameValue101292.put("id", new Integer( 30 ));
		htblColNameValue101292.put("gpa", new Double( 0.7 ));
		htblColNameValue101292.put("age", new Double( 21.0 ));
		//dbApp.insertIntoTable("Student" , htblColNameValue101292);
		
		Hashtable htblColNameValue101290 = new Hashtable( );
		htblColNameValue101290.put("name", new String("Biko" ) );
		htblColNameValue101290.put("id", new Integer( 35 ));
		htblColNameValue101290.put("gpa", new Double( 5.0 ));
		htblColNameValue101290.put("age", new Double( 21.0 ));
		//dbApp.insertIntoTable("Student" , htblColNameValue101290);
		
		Hashtable htblColNameValue101293 = new Hashtable( );
		htblColNameValue101293.put("name", new String("Ahmed" ) );
		htblColNameValue101293.put("id", new Integer( 5 ));
		htblColNameValue101293.put("gpa", new Double( 0.9 ));
		htblColNameValue101293.put("age", new Double( 19.0 ));
		//dbApp.insertIntoTable("Student" , htblColNameValue101293);
		
		Hashtable htblColNameValue1012910 = new Hashtable( );
		htblColNameValue1012910.put("name", new String("Omar" ) );
		htblColNameValue1012910.put("id", new Integer( 25 ));
		htblColNameValue1012910.put("gpa", new Double( 3.1 ));
		htblColNameValue1012910.put("age", new Double(19.0 ));
		//dbApp.insertIntoTable("Student" , htblColNameValue1012910);
		
		Hashtable htblColNameValue101294 = new Hashtable( );
		htblColNameValue101294.put("name", new String("Mohsen" ) );
		htblColNameValue101294.put("id", new Integer( 10 ));
		htblColNameValue101294.put("gpa", new Double( 1.5 ));
		htblColNameValue101294.put("age", new Double( 22.0 ));
	    //dbApp.insertIntoTable("Student" , htblColNameValue101294);
		
		Hashtable htblColNameValue1012901 = new Hashtable( );
		htblColNameValue1012901.put("name", new String("Mohamed" ) );
		htblColNameValue1012901.put("id", new Integer( 40 ));
		htblColNameValue1012901.put("gpa", new Double( 5.1 ));
		htblColNameValue1012901.put("age", new Double( 22.0 ));
		//dbApp.insertIntoTable("Student" , htblColNameValue1012901);
		
		Hashtable htblColNameValue10129012 = new Hashtable( );
		htblColNameValue10129012.put("age", new Double( 17.5 ));
		//dbApp.updateTable("Student","5.1",htblColNameValue10129012 );
		
		Hashtable htblColNameValue510 = new Hashtable( );
		htblColNameValue510.put("age", new Double( 21.0 ));
		htblColNameValue510.put("name", new String("Moh"));
		htblColNameValue510.put("gpa", new Double( 0.77) );
		//dbApp.deleteFromTable(strTableName , htblColNameValue510);
		//exception because pageAndIndex is null bec age which has a btree does not exist
		
		//if  i have a cluster with an index then i have to check the rest of the inputs
		
		
		
		
		
		
		//System.out.println("*************************************************************\n"+test3("Student")+"****************************************");
		
		//Hashtable htblColNameValue5112 = new Hashtable( );
		//htblColNameValue5112.put("name", new String("hsnssssssa" ) );
		//htblColNameValue5112.put("id", new Integer(40)) ;
		//htblColNameValue5112.put("gpa", new Double( 8.0) );
		//dbApp.deleteFromTable("Student" , htblColNameValue5112);
		
		/*SQLTerm[] aaa =new SQLTerm[3];
	    String[] bbb= {"and","and"};
	    aaa[1]=new SQLTerm("Student","name",">","Ahmed");
	    aaa[0]=new SQLTerm("Student","id",">",20);
	    aaa[2]=new SQLTerm("Student","gpa",">=",1.5);*/
	    
	    SQLTerm[] aaa =new SQLTerm[3];
	    String[] bbb= {"xor", "xor"};
	    //aaa[0]=new SQLTerm("Student","gpa",">",0.0);
	    aaa[0]=new SQLTerm("Student","age",">=",19.0);
	    aaa[1]=new SQLTerm("Student","age","<=", 21.0);
	    aaa[2]=new SQLTerm("Student", "gpa",">", 0.9);
	    
	    //System.out.println(System.getProperty("user.dir"));
		//test5(selectFromTable(aaa,bbb));
		
		//test4("Student");
		System.out.println("*************************************************************\n"+test3("Student")+"****************************************");
		//System.out.println("*************************************************************\n"+test3("Employee")+"****************************************");
		//System.out.println(SerialBTree.deser("name", "Student"));
		//System.out.println(SerialBTree.deser("empId", "Employee"));
		//System.out.println(SerialBTree.deser("gpa", "Student"));
		//System.out.println(SerialBTree.deser("id", "Student"));
		//System.out.println(Serial.deser(11, "Student"));
		//System.out.println("hhh"+Serial.deser(12, "Student").getTuple(0));
		//System.out.println(Serial.deser(3, "Student"));
		//System.out.println(Serial.deser(12, "Student"));
		
		
		
		
		//System.out.println(SerialCount.deser("Student"));
		//System.out.println(Serial.deser(2,"Student"));
		
		/*Hashtable htblColNameValue41 = new Hashtable( );
		htblColNameValue41.put("id", new Integer( 60 ));
		htblColNameValue41.put("name", new String("zz" ) );
		htblColNameValue41.put("gpa", new Double( 6.0 ) );
		dbApp.deleteFromTable(strTableName , htblColNameValue41);
		System.out.println(SerialCount.deser("Student"));*/
		
		
		/*Hashtable htblColNameValue5 = new Hashtable( );
		htblColNameValue5.put("id", new Integer( 40 ));
		htblColNameValue5.put("name", new String("b" ) );
		htblColNameValue5.put("gpa", new Double( 5.0 ) );*/
		//dbApp.deleteFromTable(strTableName , htblColNameValue5);
		//System.out.println("*************************************************************\n"+test3("Student")+"****************************************");
		//System.out.println(SerialCount.deser("Student"));
		
		
		//System.out.println("*************************************************************\n"+test3("Student")+"****************************************");
		/*dbApp.deleteFromTable(strTableName , htblColNameValue2);
		dbApp.deleteFromTable(strTableName , htblColNameValue6);
		dbApp.deleteFromTable(strTableName , htblColNameValue4);
		dbApp.deleteFromTable(strTableName , htblColNameValue8);
		dbApp.deleteFromTable(strTableName , htblColNameValue7);
		dbApp.deleteFromTable(strTableName , htblColNameValue);
		dbApp.deleteFromTable(strTableName , htblColNameValue11);
		dbApp.deleteFromTable(strTableName , htblColNameValue5);
		//dbApp.deleteFromTable(strTableName , htblColNameValue3);
		 */
		//System.out.println("*************************************************************\n"+test2("Student")+"****************************************");
		
		//System.out.println("after deletion");
		//System.out.println("******"+test("Student")+"**********");*/
		
		
		
		/*Hashtable htblColNameValue41 = new Hashtable( );
		htblColNameValue41.put("id", new Integer( 60 ));
		htblColNameValue41.put("name", new String("Hana" ) );
		htblColNameValue41.put("gpa", new Double( 4.0 ) );
		dbApp.updateTable(strTableName,"60",htblColNameValue41 );*/
		
		/*htblColNameValue5.put("id", new Integer( 40 ));
		htblColNameValue5.put("name", new String("fffff" ) );
		htblColNameValue5.put("gpa", new Double( 5.0 ) );
		dbApp.updateTable(strTableName,"40",htblColNameValue5 );*/
		
		
		//System.out.println("*************************************************************\n"+test3("Student")+"****************************************");
		
		/*Hashtable htblColNameValue80 = new Hashtable( );
		htblColNameValue80.put("id", new Integer( 30 ));
		htblColNameValue80.put("name", new String("Omar" ) );
		htblColNameValue80.put("gpa", new Double( 3.7 ) );
		dbApp.updateTable(strTableName,"zzzz",htblColNameValue80 );
		
		Hashtable htblColNameValue90 = new Hashtable( );
		htblColNameValue90.put("id", new Integer( 600 ));
		htblColNameValue90.put("name", new String("Adham" ) );
		htblColNameValue90.put("gpa", new Double( 5 ) );
		dbApp.updateTable(strTableName,"g",htblColNameValue90 );*/
		
		//System.out.println("*************************************************************\n"+test2("Student")+"****************************************");
		//System.out.println("*************************************************************\n"+test("Student")+"****************************************");
		
		//SerialNo.ser(7, "test");
		//Serial.deleteFile("./src/DB/"+"test"+".ser");
		//System.out.println(SerialNo.deser("test"));
		//System.out.println(SerialNo.deser("test"));
		//System.out.println(SerialNo.deser("test"));
		
		
		
	}
	
	public static void test5(Iterator i) {
		 while (i.hasNext()) {
	            System.out.println(i.next());
	        }
	}

	public static void test4(String tableName) {
		for(int i=0; i<200; i++) {
			Object[] array= SerialMinMaxCount.deser(i, tableName);
			for(int j=0; j<200; j++) {
				System.out.println(array[j]);
			}
		}
	}
	
	public static String test3(String tableName) {
		String ret="";
		//for (int i=0;i<0;i++) {
		for (int i=0;i<SerialCount.deser("Student");i++) {//no pages of table
			ret=ret+"Page "+(i)+":\n";
			Page p=Serial.deser(i, tableName);
			for (int j=0;j<p.size;j++) {//no tuples of each page
				ret=ret+"Tuple "+(j+1)+": "+p.getTuple(j)+"\n";
			}
			Serial.ser(p, i, tableName);
			ret=ret+"\n";
		}
		return ret;
	}
}