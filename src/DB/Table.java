package DB;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;


public class Table implements Serializable{
	private static Vector<Page> table;
	private String tableName;
	public static int clusterId;
	public static String clusterDataType;
	public static int tableSize;
	
	
	private static HashMap<String, Table> tableMap = new HashMap<>(); //9.4 new
	
	//public table() {
		//v = new Vector<page>();
	//}
	
	public Table(String tableName) {
		table = new Vector<Page>();
		this.tableName=tableName;
		tableMap.put(tableName, this);
	}
	// type as value 
	
	//new 9.4
	public static Table getTableByName(String tableName) {
        return tableMap.get(tableName);
    }
	
	//new 9.4
	public static int getTableSize(String tableName) {
        Table table = tableMap.get(tableName);
        if (table != null) {
            return Table.tableSize;
        }
        return -1; // Return -1 or throw an exception if the table doesn't exist
    }
	
	public void removePage(int index) {
		table.remove(index);
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public static int getSize(String t) {
		return SerialCount.deser(t);
		//return table.size();	
	}
	
	public static ArrayList<String []> metaReader() throws IOException{
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
	
	public static String getCluster(String tableName, ArrayList<String []> meta) {
		String ret="";
		int i=0;
		while(i<meta.size()) {
			if(meta.get(i)[0].equals(tableName)) {
				if (meta.get(i)[3].equals("True")) {
					ret= meta.get(i)[1];
				}
			}
			i++;
		}
		return ret; 
	}
	
	public static String getClusterDataType(String tableName, ArrayList<String []> meta) {
		String ret="";
		int i=0;
		while(i<meta.size()) {
			if(meta.get(i)[0].equals(tableName)) {
				if (meta.get(i)[3].equals("True")) {
					ret= meta.get(i)[2];
				}
			}
			i++;
		}
		return ret; 
	}
	
	
	
	
	
	public static void updateInTable(String strTableName, String strClusteringKeyValue,Hashtable<String, Object> htblColNameValue) throws IOException, DBAppException {
		ArrayList<String []> meta= metaReader();
		//ArrayList<String []> meta= metaReader("C:\\Users\\omara\\eclipse-workspace\\DBProject\\src\\DB\\metadata.csv");
		Stack<Object> sV= new Stack<>();
		Stack<Object> sK= new Stack<>();
    	for (Enumeration<String> keys = htblColNameValue.keys(); keys.hasMoreElements();) {
    		String key = keys.nextElement();
            Object value = htblColNameValue.get(key);
            sV.push(value);
            sK.push(key);
        }
    	ArrayList<Object> aValues=new ArrayList<>();
    	ArrayList<Object> aKeys=new ArrayList<>();
    	while(sV.isEmpty()!=true) {
    		aKeys.add(sK.pop());
    		aValues.add(sV.pop());
    	}//unhashing values aValues stores the values and aKeys stores the keys
    	
    	/*
    	boolean flag= true;//starting from this line we're validating that the input data types are matching those in the meta data
    	int i=0;
    	while(i<meta.size() && flag==true) {
    		if(meta.get(i)[0].equals(strTableName)) {
    			for(int j=0;j<aKeys.size();j++) {
    				if(!((meta.get(i)[1].equals(aKeys.get(j))) && (meta.get(i)[2].equals(aValues.get(j).getClass().getName())))) {
    					flag= false;
    				}
    				else {
    					flag= true;
    					break;
    				}
    			}
    		}
    		else {
    			flag=false;
    		}
    		i++;
    	}
    	
    	if (flag==false) {
			System.out.println("You are trying to insert wrong data types into the table");
			return;
		}
    	*/
    	
    	////////////////////////////////
    	ArrayList<ArrayList<Object>> keyAndValue= new ArrayList<ArrayList<Object>>();
        
    	Set<String> keys= htblColNameValue.keySet();
    	List<String> keysList = new ArrayList<>(keys); // Convert the Set to a List
    	
    	Collection<Object> values = htblColNameValue.values();
    	List<Object> valuesList = new ArrayList<>(values);
    	
    	ArrayList<String> testing= new ArrayList<String>();
    	for(int g=0; g < meta.size(); g++) {
    		if(meta.get(g)[0].equals(strTableName)) {
        		testing.add(meta.get(g)[1]);
    		}
    	}
    	
    	for(int x=0; x < testing.size(); x++) { //creates an arrayList of arrayLists where each arrayList inside the large arrayList contains the name of the aValue at index 0, index of it in tuple at index 1 and clusterName at index 2
    		ArrayList<Object> temp= new ArrayList<Object>();
    		for(int y=0; y < htblColNameValue.size(); y++) {
    			if((testing.get(x)).equals(keysList.get(y))) {
    				temp.add(valuesList.get(y));
    				temp.add(x);
    				temp.add(testing.get(x));
    				temp.add(valuesList.get(y).getClass().getName()); //example: java.lang.Integer
    				keyAndValue.add(temp);
    			}
    		}
    	}
    	
    	//////////////////////////////
    	
    	ArrayList<String> metaDataTypes= new ArrayList<String>();
    	for(int g=0; g < meta.size(); g++) {
    		if(meta.get(g)[0].equals(strTableName)) {
        		metaDataTypes.add(meta.get(g)[2]);
    		}
    	}
    	//System.out.println(metaDataTypes);
    	
    	ArrayList<String> usedColumnsInDelete= new ArrayList<String>();
    	for(int x=0; x < testing.size(); x++) {
    		for(int y=0; y < keyAndValue.size(); y++) {
    			if((testing.get(x)).equals((String)keyAndValue.get(y).get(2))) {
    				usedColumnsInDelete.add(testing.get(x));
    			}
    		}
    	}
    	//System.out.println(usedColumnsInDelete);
    	
    	ArrayList<Integer> indexOfUsedColumnsInDelete= new ArrayList<Integer>();
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		for(int j=0; j < testing.size(); j++) {
    			if((usedColumnsInDelete.get(i)).equals(testing.get(j))) {
    				indexOfUsedColumnsInDelete.add(j);
    			}
    		}
    	}
    	//System.out.println("4?: "+htblColNameValue.size());
    	//System.out.println("3?: "+testing.size());
    	if (htblColNameValue.size()>testing.size()) {
    		throw new DBAppException("the tuple you're trying to update has more columns than that of the table");
    	}
    	
    	boolean foundCluster=false;
    	ArrayList<Object> hashtableKeys= new ArrayList<Object>();
    	ArrayList<Object> hashtableValues= new ArrayList<Object>();
    	
    	System.out.println(htblColNameValue.size());
    	
    	for (Entry<String, Object> entry : htblColNameValue.entrySet()) {
    		String key=entry.getKey();
    		if (key.equals(SerialCName.deser(strTableName))) {
    			foundCluster=true;
    		}
    		hashtableKeys.add(key);
    	    hashtableValues.add(entry.getValue());
    	}
    	System.out.println(hashtableKeys.size());
    	System.out.println(hashtableValues.size());
    	
    	
    	/*if (foundCluster==false) {
    		throw new DBAppException("the tuple you're trying to update is missing the cluster key");
    	}*/
    	

    	boolean foundColName=false;
    	boolean correctColType=false;
    	for(int i=0; i < hashtableKeys.size(); i++) {
    		foundColName=false;
    		correctColType=false;
    		for(int j=0; j < testing.size(); j++) {
    			if (((hashtableKeys.get(i))).equals((String)testing.get(j))){
    				foundColName= true;
    				if ((hashtableValues.get(i).getClass().getTypeName().equals(metaDataTypes.get(j)))) {
        				correctColType=true;
    				}
    			}	
    		}
			if (foundColName==false) {
				throw new DBAppException("you are trying to update a value to a column that doesn't exist in the table");
			}
			if (foundColName==true && correctColType==false) {
				throw new DBAppException("you are trying to update the wrong data type of a column of the table");
			}
    	}
    	
    	boolean clusterGiven=false;
    	Comparable cluster=null;
    	for (int i=0;i<hashtableKeys.size();i++) {
    		if (hashtableKeys.get(i).equals(SerialCName.deser(strTableName))) {
    			cluster=(Comparable)hashtableValues.get(i);
    			clusterGiven=true;
    		}
    	}
    	
    	if (clusterGiven==true) {
    		String cldt2=cluster.getClass().getName();
        	if (cldt2.equals("java.lang.String") || cldt2.equals("java.lang.string")) {
        		cluster=cluster;
            }
            else if (cldt2.equals("java.lang.Integer") ||cldt2.equals("java.lang.integer")){
            	cluster = ""+cluster;
            }
            else {
            	cluster = ""+cluster;
            }

        	if ( cluster.compareTo(strClusteringKeyValue) !=0) {
    			throw new DBAppException("you are trying to update the cluster value of a tuple in the table");
        	}	
    	}
	
    	/*boolean wrongDataType= true;
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		wrongDataType= true;
    		for(int j=0; j < keyAndValue.size(); j++) {
    			if(((usedColumnsInDelete.get(i)).equals((String)keyAndValue.get(j).get(2))) && (indexOfUsedColumnsInDelete.get(i) == (int)keyAndValue.get(j).get(1)) && ((metaDataTypes.get(indexOfUsedColumnsInDelete.get(i))).equals(keyAndValue.get(j).get(3)))) {
        			wrongDataType= false;
    			}
    		}
    		if(wrongDataType == true) {
    			throw new DBAppException ("you are trying to update a tuple with the wrong data type of a column");
    		}
    	}*/
    	
    	boolean btreeOnCluster=false;
    	ArrayList<ArrayList<Object>> columnsWithIndexAndLocation= new ArrayList<ArrayList<Object>>();
    	int counter= 0;
    	for(int k=0; k < meta.size(); k++) {
    		ArrayList<Object> helper= new ArrayList<Object>();
    		if(meta.get(k)[0].equals(strTableName)) {
    			if(meta.get(k)[5].equals("B+tree")) {
    				helper.add(meta.get(k)[1]);
    				helper.add(counter);
    				columnsWithIndexAndLocation.add(helper);
    				if(meta.get(k)[1].equals(SerialCName.deser(strTableName))){
    					btreeOnCluster= true;
    				}
    			}
    			counter++;
    		}
    		
    	}
    	
    	/////////////////////////////
    	
    	int indexOfCluster= SerialCId.deser(strTableName);
    	String nameOfCluster= SerialCName.deser(strTableName); //omar new delete
    	
    	boolean clusterFound= false;
    	//checks if the input hashtable goes in with the clusterId
    	for(int x=0; x < keyAndValue.size(); x++) {
    		if(((int)keyAndValue.get(x).get(1) == indexOfCluster) && (keyAndValue.get(x).get(2).equals(nameOfCluster))) { //indexOfCluster is always int therefore can type cast to int
    			clusterFound= true;
    		}
    	}
    	
    	//String cluster=getCluster(strTableName,meta);//i got the name of the cluster key 
    	//System.out.println(cluster);
    	/*int clId=0;
    	for (int j=0;j<aKeys.size();j++) {//getting cluster id (position) in akeys and avalues
    		if (aKeys.get(j).equals(cluster)) {
    			clId=j;
    		
    		}
    	}*/
    	
    	Comparable num; //num is the cluster key of the tuple to update
    	//Comparable clusterOfTupleNeed;
    	for(int i=0; i < keyAndValue.size(); i++) {
    		if(((String)keyAndValue.get(i).get(2)).equals(nameOfCluster)) {
    			num= (Comparable)keyAndValue.get(i).get(0);
    			break;
    		}
    	}
    	
    	Table table= DBApp.retTable(strTableName);
    	 
    	String cldt=SerialCType.deser(strTableName);
    	//System.out.println(cldt);
    	//Comparable num;
    	
    	if (cldt.equals("java.lang.String") || cldt.equals("java.lang.string")) {
			try {
				num = strClusteringKeyValue;
			}
			catch(Exception e) {
				throw new DBAppException("Wrong input data type of cluster");
			}
		}
		else if (cldt.equals("java.lang.Integer") ||cldt.equals("java.lang.integer")){
			try {
				num = Integer.parseInt(strClusteringKeyValue);
			}
			catch(Exception e) {
				throw new DBAppException("Wrong input data type of cluster");
			}
		}
		else {
			try {
				num = Double.parseDouble(strClusteringKeyValue);
			}
			catch(Exception e) {
				throw new DBAppException("Wrong input data type of cluster");
			}
		}
    	
        /*if (cldt.equals("java.lang.String") || cldt.equals("java.lang.string")) {
        	num=strClusteringKeyValue;
        	}
        else if (cldt.equals("java.lang.Integer") ||cldt.equals("java.lang.integer")){
        	num = Integer.parseInt(strClusteringKeyValue);
        }
        else {
        	num = Double.parseDouble(strClusteringKeyValue);
        }*/
        
        /*boolean btreeOnCluster=false;//true if there's a btree index on the cluster key
        for (int i=0;i<columnsWithIndexAndLocation.size(); i++) {
        	if ((int)columnsWithIndexAndLocation.get(i).get(1)==indexOfCluster) {
        		btreeOnCluster=true;
        	}
        } */
        
        if (btreeOnCluster==true) {
        	BTree<String,String> bTree=SerialBTree.deser(SerialCName.deser(strTableName), strTableName);
        	if (bTree.search(""+strClusteringKeyValue)==null) {
        		throw new DBAppException("Trying to update a tuple that doesn't exist in the table");
        	}
        	String m= bTree.search(""+strClusteringKeyValue).get(0);
        	//System.out.println(m);
        	String mm = m.split("-")[0];
        	int mmm=Integer.parseInt(mm);
        	SerialBTree.ser(bTree, SerialCName.deser(strTableName), strTableName);
        	Page page=Serial.deser(mmm, strTableName);
        	ArrayList<Object> tuplePostUpdateComponents= new ArrayList<Object>();
        	BinarySearchToUpdate2.binarySearchForTuple2(page,0,page.getSize()-1,num,SerialCId.deser(strTableName),keyAndValue,mmm,strTableName,tuplePostUpdateComponents,columnsWithIndexAndLocation, testing);
        }
        else {
        	int s=0;
        	int e=getSize(strTableName)-1;
        	int m=(s+e)/2;        	
        	Object[] minMaxCount=SerialMinMaxCount.deser(m, strTableName);
        	BinarySearchToUpdate2.binarySearchForPage2(minMaxCount,table,0,getSize(strTableName)-1,num,keyAndValue, strTableName, columnsWithIndexAndLocation, testing);
        }
    	

    	////////////////////////////////
    	
    	/*Table table= DBApp.retTable(strTableName);
 
    	String cldt=SerialCType.deser(strTableName);
    	Comparable num;
        if (cldt.equals("java.lang.String") || cldt.equals("java.lang.string")) {
        	num=strClusteringKeyValue;
        	}
        else if (cldt.equals("java.lang.Integer") ||cldt.equals("java.lang.integer")){
        	num = Integer.parseInt(strClusteringKeyValue);
        }
        else {
        	num = Double.parseDouble(strClusteringKeyValue);
        }
        
    	int s=0;
    	int e=getSize(strTableName)-1;
    	int m=(s+e)/2;
    	Page page=Serial.deser(m,strTableName);
    	BinarySearchToUpdate2.binarySearchForPage2(page,table,0,getSize(strTableName)-1,num,aValues, strTableName);*/

    	//BinarySearchToUpdate.binarySearchForPage(table,0,table.getSize()-1,num,aValues);
    	
	}
	
	
	public static void insertTable(String strTableName,Hashtable<String, Object> htblColNameValue) throws IOException, DBAppException {//check if table already has pages if yes
	    
		ArrayList<String []> meta= metaReader();
		Stack<Object> sV= new Stack<>();
		Stack<Object> sK= new Stack<>();
    	for (Enumeration<String> keys = htblColNameValue.keys(); keys.hasMoreElements();) {
    		String key = keys.nextElement();
            Object value = htblColNameValue.get(key);
            sV.push(value);
            sK.push(key);
        }
    	
    	ArrayList<Object> aValues=new ArrayList<>();
    	ArrayList<Object> aKeys=new ArrayList<>();
    	while(sV.isEmpty()!=true) {
    		aKeys.add(sK.pop());
    		aValues.add(sV.pop());
    	}
    	
    	ArrayList<ArrayList<Object>> keyAndValue= new ArrayList<ArrayList<Object>>();

    	Set<String> keys= htblColNameValue.keySet();
    	List<String> keysList = new ArrayList<>(keys); 
    	
    	Collection<Object> values = htblColNameValue.values();
    	List<Object> valuesList = new ArrayList<>(values);
    	
    	ArrayList<String> testing= new ArrayList<String>();
    	for(int g=0; g < meta.size(); g++) {
    		if(meta.get(g)[0].equals(strTableName)){
        		testing.add(meta.get(g)[1]);
    		}
    	}
    	
    	for(int x=0; x < testing.size(); x++) { //creates an arrayList of arrayLists where each arrayList inside the large arrayList contains the name of the aValue at index 0, index of it in tuple at index 1 and clusterName at index 2
    		ArrayList<Object> temp= new ArrayList<Object>();
    		for(int y=0; y < htblColNameValue.size(); y++) {
    			if((testing.get(x)).equals(keysList.get(y))) {
    				temp.add(valuesList.get(y)); //example: "mohamed"
    				temp.add(x); //example: index
    				temp.add(testing.get(x)); //example: "name"
    				temp.add(valuesList.get(y).getClass().getName()); //example: java.lang.Integer
    				keyAndValue.add(temp);
    			}
    		}
    	}
    	
    	ArrayList<String> metaDataTypes= new ArrayList<String>();
    	for(int g=0; g < meta.size(); g++) {
    		if(meta.get(g)[0].equals(strTableName)) {
        		metaDataTypes.add(meta.get(g)[2]);
    		}
    	}
    	
    	ArrayList<String> usedColumnsInDelete= new ArrayList<String>();
    	for(int x=0; x < testing.size(); x++) {
    		for(int y=0; y < keyAndValue.size(); y++) {
    			if((testing.get(x)).equals((String)keyAndValue.get(y).get(2))) {
    				usedColumnsInDelete.add(testing.get(x));
    			}
    		}
    	}
    	
    	ArrayList<Integer> indexOfUsedColumnsInDelete= new ArrayList<Integer>();
    	for(int o=0; o < usedColumnsInDelete.size(); o++) {
    		for(int j=0; j < testing.size(); j++) {
    			if((usedColumnsInDelete.get(o)).equals(testing.get(j))) {
    				indexOfUsedColumnsInDelete.add(j);
    			}
    		}
    	}
    	
    	ArrayList<ArrayList<Object>> columnsWithIndexAndLocation= new ArrayList<ArrayList<Object>>();
    	int counter= 0;
    	for(int k=0; k < meta.size(); k++) {
    		ArrayList<Object> helper= new ArrayList<Object>();
    		if(meta.get(k)[0].equals(strTableName)) {
    			if(meta.get(k)[5].equals("B+tree")) {
    				helper.add(meta.get(k)[1]);
    				helper.add(counter);
    				columnsWithIndexAndLocation.add(helper);
    			}
    			counter++;
    		}		
    	}
    	
    	//System.out.println("htblColNameValue.size() "+htblColNameValue.size());
    	//System.out.println("testing.size() "+testing.size());
    	
    	if (htblColNameValue.size()!=testing.size()) {
    		throw new DBAppException("you're trying to insert a tuple with a missing or an extra column");
    	}
    	
    	ArrayList<Object> hashtableKeys= new ArrayList<Object>();
    	ArrayList<Object> hashtableValues= new ArrayList<Object>();
    	for (Entry<String, Object> entry : htblColNameValue.entrySet()) {
    		hashtableKeys.add(entry.getKey());
    	    hashtableValues.add(entry.getValue());
    	}

    	boolean foundColName=false;
    	boolean correctColType=false;
    	for(int i=0; i < testing.size(); i++) {
    		foundColName=false;
    		correctColType=false;
    		for(int j=0; j < testing.size(); j++) {
    			if (((hashtableKeys.get(j))).equals((String)testing.get(i))){
    				foundColName= true;
    				if ((hashtableValues.get(j).getClass().getTypeName().equals(metaDataTypes.get(i)))) {
        				correctColType=true;
    				}
    			}	
    		}
			if (foundColName==false) {
				throw new DBAppException("you are trying to insert a value to a column that doesn't exist in the table");
			}
			if (foundColName==true && correctColType==false) {
				throw new DBAppException("you are trying to insert the wrong data type of a column of the table");
			}
    	}
    		
    	//System.out.println(indexOfUsedColumnsInDelete);
    
    	/*boolean wrongDataType= true;
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		wrongDataType= true;
    		for(int j=0; j < keyAndValue.size(); j++) {
    			if(((usedColumnsInDelete.get(i)).equals((String)keyAndValue.get(j).get(2))) && (indexOfUsedColumnsInDelete.get(i) == (int)keyAndValue.get(j).get(1)) && ((metaDataTypes.get(indexOfUsedColumnsInDelete.get(i))).equals(keyAndValue.get(j).get(3)))) {
        			wrongDataType= false;
    			}
    		}
    		if(wrongDataType == true) {
    			System.out.println("You are trying to insert wrong datatypes");
    			return;
    		}
    	}*/
    	
    	ArrayList<Object> aValuesNew= new ArrayList<Object>();
    	
    	//System.out.println("indexOfUsedColumnsInDelete "+indexOfUsedColumnsInDelete);
    	
    	/*for(int i=0; i < indexOfUsedColumnsInDelete.size(); i++) {
    		for(int x=0; x < keyAndValue.size(); x++) {
    			if(indexOfUsedColumnsInDelete.get(i) == keyAndValue.get(x).get(1)) {
    				aValuesNew.add(keyAndValue.get(x).get(0));
    			}
    		}
    	}*/
    	
       	//System.out.println("aValuesNew= "+aValuesNew);
    	
    	/////////////////////////
    	
    	String cluster=getCluster(strTableName,meta);//i got the name of the cluster key 
    	//System.out.println("cluster= "+cluster);
    	
    	//System.out.println(cluster);
    	int clId=0;
    	for (int j=0;j<aKeys.size();j++) {//getting cluster id (position) in akeys and avalues
    		if (aKeys.get(j).equals(cluster)) {
    			clId=j;
    		
    		}
    	}
    	
        Object clusterValue=aValues.get(clId);//i'm assuming that all cluster keys are integers?
        //System.out.println("clusterValue= "+clusterValue);
        

       
    	//System.out.println(clusterValue);    	
    	
    	clusterId=clId;
    	//System.out.println(clusterId+"what");
    	
    	String clusterDT=getClusterDataType(strTableName,meta);
    	clusterDataType=clusterDT;
    	//System.out.println(clusterDataType+"is");
    	
    	//System.out.println(clusterValue);
    	Table table= DBApp.retTable(strTableName);
    	
    	//System.out.println(keyAndValue);
    	
    	//System.out.println("Insering: "+clusterValue);
    	insertPageIntoTable(table,clusterValue,aValues,strTableName, columnsWithIndexAndLocation);
	}
		
	public Page getPage(int index) {
        if (index >= 0 && index < table.size()) {
        	//System.out.println("here "+table.get(index)+" till here");            
        	return table.get(index);
        } else {
            return null; // or throw an exception indicating index out of bounds
        }
    }
	
	public static void insertPageIntoTable(Table t, Object clusterValue, ArrayList<Object> aValues, String tName, ArrayList<ArrayList<Object>> btreeCols) throws DBAppException { //check 9.4 case of size==0
		
		String fileName = "./src/DB/"+tName+"Count"+".ser"; // Assuming your file naming convention
	    File file = new File(fileName);
		int size=0;
	    if (file.exists()) {
	    	size= SerialCount.deser(tName);
	    }
		if(size==0) {//no pages
			Page p= new Page(tName+ " " + size); //set page size? //check
			
			//t.addPage(p);
			p.insertIntoPage(aValues);
			SerialCount.ser(1,tName);//SER COUNT
			
			//System.out.println(tableSize);
			//System.out.println(table.size());
			Object[] tupleMinMaxCount= {p.getTuple(0),p.getTuple(p.getSize()-1),1};
			//System.out.println(p.getTuple(0));
			//System.out.println(p.getTuple(p.getSize()-1));
			
			//System.out.println(btreeCols);
			
			for (int i=0;i<btreeCols.size();i++) {
				BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tName);
				int indexOfColumn= (int)btreeCols.get(i).get(1);
				int indexOfCluster= SerialCId.deser(tName);
				Comparable theCluster= (Comparable) aValues.get(indexOfCluster);
				bTree.insert(""+aValues.get(indexOfColumn), "0-"+theCluster); //0- alashan di awal page
				SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tName);
			}
			
			SerialMinMaxCount.ser( tupleMinMaxCount, 0, tName);
			Serial.ser(p, 0, tName);//commhana add
			
			//Serial.ser(p,0,t.getTableName());
		}
		else {
			//System.out.println(t.getSize());
			int mid= (size-1) / 2;
			//Page page= t.getPage(mid);//commhana rem			
			//Page page=Serial.deser(mid, tName);//commhana add
			Object[] tupleMinMaxCount= SerialMinMaxCount.deser( mid, tName);
			String tableName= tName;
			
			
			Insert.getPageToInsert(tupleMinMaxCount, 0, size-1, aValues, tableName, clusterId, btreeCols);
			//BinarySearchPage.BinarySearchPage(t,0,size-1, aValues, clusterId);
		}
		SerialTable.ser(t, tName);
	}
	
	public int getIndexLast(Page p, String tableName) { //4.4 recheck if a page has a max size of 5 but has 3 elements only it returns 2,the indexof the last used elem
		 //int countIndex=0;
		 //Serial.deser(indexOfPage, tableName);
		 //Page p= getPage(indexOfPage);
		 /*for(int i=0; i<p.size; i++) { //200 is the pre-set size of a page
			if(!(pageVectors.get(indexOfPage).getTuple(i)==null)) {
				countIndex++;
			}			
			else
				break;
		}
		 return countIndex;*/
		 return p.getSize()-1;
	}
	
	public void addPage(Page p) {
		table.add(p);
		//tableSize=table.size(); //10.4 recheck
	}

	public static void deleteInTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException, DBAppException {

		if (htblColNameValue.size()==0) {//I HAVETO DELETE FROM THE BTREES AS WELL
	//System.out.println("enter");
			ArrayList<String[]> meta2= metaReader();
			ArrayList<ArrayList<Object>> columnsWithIndex= new ArrayList<ArrayList<Object>>();
	    	int counter2= 0;
	    	for(int k=0; k < meta2.size(); k++) {
	    		ArrayList<Object> helper2= new ArrayList<Object>();
	    		if(meta2.get(k)[0].equals(strTableName)) {
	    			if(meta2.get(k)[5].equals("B+tree")) {
	    				helper2.add(meta2.get(k)[1]);
	    				helper2.add(meta2.get(k)[4]);
	    				columnsWithIndex.add(helper2);
	    			}
	    			counter2++;
	    		}
	    	}
	    	
	    	for (int i=0;i<columnsWithIndex.size();i++) {
	    		Serial.deleteFile("./src/DB/"+strTableName+columnsWithIndex.get(i).get(0)+"B+Tree"+".ser");
	    		BTree<String,String> bTree = new BTree<String,String>((String)columnsWithIndex.get(i).get(1));
	    		SerialBTree.ser(bTree, (String)columnsWithIndex.get(i).get(0) , strTableName);	
	    	}
	    		
			int size=SerialCount.deser(strTableName);
			for (int i=0;i<size;i++) {
				Serial.deleteFile("./src/DB/"+strTableName+i+"MinMaxCount"+".ser");
				Serial.deleteFile("./src/DB/"+strTableName+i+".ser");
			}
			SerialCount.ser(0,strTableName);
			return; 
		}
		
		ArrayList<String []> meta= metaReader();
		Stack<Object> sV= new Stack<>();
		Stack<Object> sK= new Stack<>();
    	for (Enumeration<String> keys = htblColNameValue.keys(); keys.hasMoreElements();) {
    		String key = keys.nextElement();
            Object value = htblColNameValue.get(key);
            sV.push(value);
            sK.push(key);
        }
    	ArrayList<Object> aValues=new ArrayList<>();
    	ArrayList<Object> aKeys=new ArrayList<>();
    	while(sV.isEmpty()!=true) {
    		aKeys.add(sK.pop());
    		aValues.add(sV.pop());
    	}//unhashing values aValues stores the values and aKeys stores the keys
    	
    	/////////////////////
    	
    	ArrayList<ArrayList<Object>> keyAndValue= new ArrayList<ArrayList<Object>>();
        
    	Set<String> keys= htblColNameValue.keySet();
    	List<String> keysList = new ArrayList<>(keys); // Convert the Set to a List
    	
    	Collection<Object> values = htblColNameValue.values();
    	List<Object> valuesList = new ArrayList<>(values);
    	
    	ArrayList<String> testing= new ArrayList<String>();
    	for(int g=0; g < meta.size(); g++) {
    		if(meta.get(g)[0].equals(strTableName)) {
        		testing.add(meta.get(g)[1]);
    		}
    	}
    	
    	for(int x=0; x < testing.size(); x++) { //creates an arrayList of arrayLists where each arrayList inside the large arrayList contains the name of the aValue at index 0, index of it in tuple at index 1 and clusterName at index 2
    		ArrayList<Object> temp= new ArrayList<Object>();
    		for(int y=0; y < htblColNameValue.size(); y++) {
    			if((testing.get(x)).equals(keysList.get(y))) {
    				temp.add(valuesList.get(y)); //example: "mohamed"
    				temp.add(x); //example: index
    				temp.add(testing.get(x)); //example: "name"
    				temp.add(valuesList.get(y).getClass().getName()); //example: java.lang.Integer
    				keyAndValue.add(temp);
    			}
    		}
    	}
    	//System.out.println(keyAndValue);
    	/////////////////////////////////checks correct datatypes
    	ArrayList<String> metaDataTypes= new ArrayList<String>();
    	for(int g=0; g < meta.size(); g++) {
    		if(meta.get(g)[0].equals(strTableName)) {
        		metaDataTypes.add(meta.get(g)[2]);
    		}
    	}
    	//System.out.println(metaDataTypes);
    	
    	ArrayList<String> usedColumnsInDelete= new ArrayList<String>();
    	for(int x=0; x < testing.size(); x++) {
    		for(int y=0; y < keyAndValue.size(); y++) {
    			if((testing.get(x)).equals((String)keyAndValue.get(y).get(2))) {
    				usedColumnsInDelete.add(testing.get(x));
    			}
    		}
    	}
    	//System.out.println(usedColumnsInDelete);
    	
    	ArrayList<Integer> indexOfUsedColumnsInDelete= new ArrayList<Integer>();
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		for(int j=0; j < testing.size(); j++) {
    			if((usedColumnsInDelete.get(i)).equals(testing.get(j))) {
    				indexOfUsedColumnsInDelete.add(j);
    			}
    		}
    	}
    	//System.out.println(indexOfUsedColumnsInDelete);
    	
    	if (htblColNameValue.size()>testing.size()) {
    		throw new DBAppException("you're trying to delete a tuple with an extra column");
    	}
    	
    	ArrayList<Object> hashtableKeys= new ArrayList<Object>();
    	ArrayList<Object> hashtableValues= new ArrayList<Object>();
    	for (Entry<String, Object> entry : htblColNameValue.entrySet()) {
    		hashtableKeys.add(entry.getKey());
    	    hashtableValues.add(entry.getValue());
    	}

    	boolean foundColName=false;
    	boolean correctColType=false;
    	for(int i=0; i < hashtableKeys.size(); i++) {
    		foundColName=false;
    		correctColType=false;
    		for(int j=0; j < testing.size(); j++) {
    			if (((hashtableKeys.get(i))).equals((String)testing.get(j))){
    				foundColName= true;
    				if ((hashtableValues.get(i).getClass().getTypeName().equals(metaDataTypes.get(j)))) {
        				correctColType=true;
    				}
    			}	
    		}
			if (foundColName==false) {
				throw new DBAppException("you are trying to delete a value of a column that doesn't exist in the table");
			}
			if (foundColName==true && correctColType==false) {
				throw new DBAppException("you are trying to delete the wrong data type of a column of the table");
			}
    	}
    
    	boolean wrongDataType= true;
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		wrongDataType= true;
    		for(int j=0; j < keyAndValue.size(); j++) {
    			if(((usedColumnsInDelete.get(i)).equals((String)keyAndValue.get(j).get(2))) && (indexOfUsedColumnsInDelete.get(i) == (int)keyAndValue.get(j).get(1)) && ((metaDataTypes.get(indexOfUsedColumnsInDelete.get(i))).equals(keyAndValue.get(j).get(3)))) {
        			wrongDataType= false;
    			}
    		}
    		if(wrongDataType == true) {
    			System.out.println("You are trying to delete wrong datatypes");
    			return;
    		}
    	}
    	/////////////////////////////////
    	
    	int indexOfCluster= SerialCId.deser(strTableName);
    	String nameOfCluster= SerialCName.deser(strTableName); //omar new delete
    	
    	
    	
    	boolean clusterFound= false;
    	//checks if the input hashtable goes in with the clusterId
    	for(int x=0; x < keyAndValue.size(); x++) {
    		if(((int)keyAndValue.get(x).get(1) == indexOfCluster) && (keyAndValue.get(x).get(2).equals(nameOfCluster))) { //indexOfCluster is always int therefore can type cast to int
    			clusterFound= true;
    		}
    	}
    	
    	//////////////////////////////15.4
    	
    	ArrayList<ArrayList<Object>> columnsWithIndexAndLocation= new ArrayList<ArrayList<Object>>();
    	int counter= 0;
    	for(int k=0; k < meta.size(); k++) {
    		ArrayList<Object> helper= new ArrayList<Object>();
    		if(meta.get(k)[0].equals(strTableName)) {
    			if(meta.get(k)[5].equals("B+tree")) {
    				helper.add(meta.get(k)[1]);
    				helper.add(counter);
    				columnsWithIndexAndLocation.add(helper);
    			}
    			counter++;
    		}
    	}
    	
    	ArrayList<ArrayList<Object>> givenInDeleteAndHaveBTreeWithLocation= new ArrayList<ArrayList<Object>>();
    	ArrayList<Object> helperArrayList= new ArrayList<Object>();
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		helperArrayList= new ArrayList<Object>() ;
    		for(int x=0; x < columnsWithIndexAndLocation.size(); x++) {
    			if((usedColumnsInDelete.get(i)).equals(columnsWithIndexAndLocation.get(x).get(0))) { //check if .get(0) or .get(1)
    				//System.out.println("hereeeeeeeeeeeeee: "+columnsWithIndexAndLocation.get(x).get(0));
    				//System.out.println("hereeeeeeeeeeeeee: "+columnsWithIndexAndLocation.get(x).get(1));
    				helperArrayList.add(columnsWithIndexAndLocation.get(x).get(0));
    				helperArrayList.add(columnsWithIndexAndLocation.get(x).get(1));
    				givenInDeleteAndHaveBTreeWithLocation.add(helperArrayList);
    				//System.out.println("hereeeeeeeeeeeeee: "+givenInDeleteAndHaveBTreeWithLocation);
    			}
    		}
    	}
    	
    	///////////////////////////////15.4
    	if(columnsWithIndexAndLocation.size() == 0) { //no indices ala ay columns aslan
    		if(clusterFound == true) {
        		//call old delete
        		Table table= DBApp.retTable(strTableName);
            	
            	Comparable delVal= (Comparable) aValues.get(SerialCId.deser(strTableName));
            	
            	int s=0;
            	//int e=Serial.deser(i, strTableName);
            	int e= SerialCount.deser(table.getTableName())-1;
            	//System.out.println("e:"+e);
            	int m=(s+e)/2;
            	//System.out.println("m: "+m);
            	
            	Object[] minMaxCount=SerialMinMaxCount.deser(m, strTableName);
            	//Page page=Serial.deser(m,table.getTableName());
            	
            	//System.out.println("PAGE&&&&&&&&&& "+page);
            	//System.out.println("delval: "+delVal);
            	//System.out.println(aValues);
            	//System.out.println(strTableName);
            	
            	BinarySearchToDelete.binarySearchForPage2(minMaxCount,table,0,e,delVal,aValues, strTableName, indexOfUsedColumnsInDelete, keyAndValue);
        	}
        	else {
        		//System.out.println("i am here omar");
        		
        		Page firstPage= Serial.deser(0, strTableName);
        		Tuple firstTuple= firstPage.getTuple(0);
        		newDelete.deleteWithAnd(firstPage, firstTuple, keyAndValue, 0, strTableName, 0);
        	}
    	}
    	else { //there are indices
    		String clusterName= SerialCName.deser(strTableName);
    		boolean iFoundCluster= false;
    		int i=0;
    		for( ;i < givenInDeleteAndHaveBTreeWithLocation.size(); i++) {
    			if(givenInDeleteAndHaveBTreeWithLocation.get(i).get(0).equals(clusterName)) {
    				iFoundCluster= true; 
    				break;
    			}
    		}
    		
    		if(iFoundCluster == true) {
    			//System.out.println("i will enter here");
    			BTree<String,String> bTree=SerialBTree.deser(""+clusterName, strTableName);
    			Comparable clusterValue=null;
    			for(int x=0; x < keyAndValue.size(); x++) {
    				if(keyAndValue.get(x).get(2).equals(clusterName)) {
    					clusterValue= (Comparable)keyAndValue.get(x).get(0);
    				}
    			}
    			//System.out.println("clusterValue "+clusterValue);
    			Vector<String> pageAndIndex= bTree.search(""+clusterValue);
    			
    			if (pageAndIndex==null){
    				return;
    			}
    			
    			int thePage= Integer.parseInt(pageAndIndex.get(0).split("-")[0]);
    			SerialBTree.ser(bTree, (String)clusterName, strTableName);
    			
    			Page page2=Serial.deser(thePage, strTableName);
				//ArrayList<Object> a= null;
				//Comparable val,int clId,ArrayList<Object> a,Table t, int pageIndex, String tName, ArrayList<Integer> indexOfUsedColumnsInDelete,  ArrayList<ArrayList<Object>> keyAndValue, ArrayList<ArrayList<Object>> btreeCols
				BinarySearchToDelete2.binarySearchForTuple(page2, 0, page2.getSize()-1, /*check*/clusterValue, SerialCId.deser(strTableName), aValues, SerialTable.deser(strTableName), thePage, strTableName, indexOfUsedColumnsInDelete, keyAndValue, columnsWithIndexAndLocation);//call the binarysearch for tuple and remove from all btrees if have index AND defragment check that rest of tuple correct 
    			return;
   			}
   			else if(givenInDeleteAndHaveBTreeWithLocation.size() != 0) {
   				//System.out.println("i will not enter here");
   				//System.out.println(givenInDeleteAndHaveBTreeWithLocation);
   				BTree<String,String> bTree=SerialBTree.deser(""+givenInDeleteAndHaveBTreeWithLocation.get(0).get(0), strTableName);
   				//System.out.println("name of col with btree i will use: "+givenInDeleteAndHaveBTreeWithLocation.get(0).get(0));
   				Comparable num=null;
   				//int thePage= -1;
   				Comparable Value=null;
   				
    			for(int x=0; x < keyAndValue.size(); x++) {
    				if(keyAndValue.get(x).get(2).equals(givenInDeleteAndHaveBTreeWithLocation.get(0).get(0))) {
    					//System.out.println("name of input: "+keyAndValue.get(x).get(2));
    					Value= (Comparable)keyAndValue.get(x).get(0);
    					//System.out.println("omar: "+Value);
    				}
    			}
    			
   				Vector<String> pageAndIndex= bTree.search(""+Value);
   				//System.out.println("1st page and index: "+pageAndIndex);
   				Vector<String> normalArray= new Vector<String>(); //change to object
   				
   				SerialBTree.ser(bTree, ""+givenInDeleteAndHaveBTreeWithLocation.get(0).get(0), strTableName);
   				
   				/*if (pageAndIndex==null) {
   					throw new DBAppException()
   				}*/
   				
   				if(pageAndIndex==null) {//i added htis here hana 
   					return;
   				}
   				
   				for(int y=0; y<pageAndIndex.size(); y++) { //normal array contains all the clusters
   					normalArray.add(pageAndIndex.get(y).split("-")[1]);
   				}
   				
   				
   				//System.out.println("1st normal array "+normalArray);
   				
   				boolean iHaveClusterInInput=false;
   				Comparable cl=null;
					//System.out.println("hereeee"+keyAndValue);
				for (int j=0;j<keyAndValue.size();j++) {
					if (((keyAndValue.get(j)).get(2)).equals(SerialCName.deser(strTableName))) {
						iHaveClusterInInput=true;
						cl=(Comparable) keyAndValue.get(j).get(0);
					}
				}
   				
   				int indexOfPage= -1;
   				int sizeIWILLNEED= pageAndIndex.size();//no pages i will try
   				int newI=0;
   				String temp="";
   				for(int z=0; z<normalArray.size();z++) { //must update each loop
   					//for(int k=0; k<normalArray.size(); k++) {
   					BTree<String,String> bt=SerialBTree.deser(""+givenInDeleteAndHaveBTreeWithLocation.get(0).get(0), strTableName);
   					pageAndIndex= bt.search(""+Value);
   					if ( !temp.equals("") && temp.equals(pageAndIndex.get(newI))) {
   						newI=newI+1;
   					}
   					//if((pageAndIndex.get(newI).split("-")[1]).equals(normalArray.get(z))){
   						indexOfPage= Integer.parseInt(pageAndIndex.get(newI).split("-")[0]);
   						
   						//System.out.println("indexOfPage "+indexOfPage);
   						
   						String cldt=SerialCType.deser(strTableName);
   						if (cldt.equals("java.lang.String") || cldt.equals("java.lang.string")) {
   							num = normalArray.get(z);
   			    		   }
   						else if (cldt.equals("java.lang.Integer") ||cldt.equals("java.lang.integer")){
   							num = Integer.parseInt(normalArray.get(z));
   						}
   						else {
   							num = Double.parseDouble(normalArray.get(z));
   						}
   			   							
   						if (iHaveClusterInInput==false) {
   							//System.out.println("i will always enter here");
   							Page p= Serial.deser(indexOfPage, strTableName);
   	   						Table table= DBApp.retTable(strTableName);
   	   						BinarySearchToDelete2.binarySearchForTuple(p, 0, p.getSize()-1, num, SerialCId.deser(strTableName), aValues, table, indexOfPage, strTableName, indexOfUsedColumnsInDelete, keyAndValue, columnsWithIndexAndLocation);
   						}
   						else {
   							//System.out.println("i will not enter here");
   							Object[] minMax=SerialMinMaxCount.deser(indexOfPage, strTableName);
   							Tuple mini=(Tuple)minMax[0];
   							Tuple maxi=(Tuple)minMax[1];
   							if (cl.compareTo((Comparable) mini.getTupleVectors().get(SerialCId.deser(strTableName))) >=0 &&  cl.compareTo((Comparable) maxi.getTupleVectors().get(SerialCId.deser(strTableName))) <=0) {
   								Page p= Serial.deser(indexOfPage, strTableName);
   	   	   						Table table= DBApp.retTable(strTableName);
   	   	   						BinarySearchToDelete2.binarySearchForTuple(p, 0, p.getSize()-1, num, SerialCId.deser(strTableName), aValues, table, indexOfPage, strTableName, indexOfUsedColumnsInDelete, keyAndValue, columnsWithIndexAndLocation);
   							}
   							else {
   								mini=mini;
   							}
   						}
   						
   						temp=pageAndIndex.get(newI);
   						
   						
   						
   						
   					//}
   					
   					
   				}
   					
   					
   				
   				
   					
    		}
   			else {
   				boolean flagElse= false;
   				//System.out.println("usedColsInDelete "+usedColumnsInDelete);
   				for(int b=0; b < usedColumnsInDelete.size(); b++) {
   					if(usedColumnsInDelete.get(b).equals(SerialCName.deser(strTableName))) {
   						flagElse= true;
   						break;
   					}
   				}
   				if(flagElse == true) {
   					//System.out.println("i have cols that have trees not in the input an i have cluster in input");
		        	Table table= DBApp.retTable(strTableName);
		            	
		        	//System.out.println(aValues);
		        	/*int z=0;
		        	for (int j=0;j<aKeys.size();j++) {
		        		if (aKeys.equals(SerialCName.deser(strTableName))) {
		        			z=i;
		        		}
		        	}*/
		        	Comparable delVal= null;
		        	//System.out.println(keyAndValue);
		        	for(int g=0; g<keyAndValue.size(); g++) {
		        		if(keyAndValue.get(g).get(2).equals(SerialCName.deser(strTableName))) {
		        			delVal= (Comparable) keyAndValue.get(g).get(0);
		        		}
		        	}
		           	//Comparable delVal= (Comparable) aValues.get(0);
		        	//System.out.println(delVal);
		           	int s=0;
		           	int e= SerialCount.deser(table.getTableName())-1;
		           	int m=(s+e)/2;
		            	
		           	Object[] minMaxCount=SerialMinMaxCount.deser(m, strTableName);
		           	BinarySearchToDelete2.binarySearchForPage(minMaxCount,table,0,e,delVal,aValues, strTableName, indexOfUsedColumnsInDelete, keyAndValue, columnsWithIndexAndLocation); //recheck
   				}
   				else {
   					//System.out.println("i have cols that have trees not in the input an i dont have cluster in input");
   					Page firstPage= Serial.deser(0, strTableName);
   	        		Tuple firstTuple= firstPage.getTuple(0);
   	        		//System.out.println("pass eh "+columnsWithIndexAndLocation);
   	        		Linear2.deleteWithAnd(firstPage, firstTuple, keyAndValue, 0, strTableName, 0, columnsWithIndexAndLocation);
   				}
   			}
    	}
    	
    	
    	/////////////////////
    	
    	
    	/*Table table= DBApp.retTable(strTableName);
    	
    	Comparable delVal= (Comparable) aValues.get(SerialCId.deser(strTableName));
    	
    	int s=0;
    	//int e=Serial.deser(i, strTableName);
    	int e= SerialCount.deser(table.getTableName())-1;
    	System.out.println("e:"+e);
    	int m=(s+e)/2;
    	System.out.println("m: "+m);
    	Page page=Serial.deser(m,table.getTableName());
    	
    	System.out.println("PAGE&&&&&&&&&& "+page);
    	System.out.println("delval: "+delVal);
    	System.out.println(aValues);
    	System.out.println(strTableName);
    	
    	BinarySearchToDelete.binarySearchForPage2(page,table,0,e,delVal,aValues, strTableName);*/
	}
	
	public static void main(String[] args) throws IOException {
    }
}
	


