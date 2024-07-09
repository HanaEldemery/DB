package DB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Vector;



public class select {
	
	private static Vector<Tuple> getNotEqualKeys(Comparable value, String tName) throws DBAppException {
		return Not(tabletoVector(tName), getEqualKeys(value,tName), tName);
		
	}
	private static Vector<Tuple> getEqualKeys(Comparable value, String tName) throws DBAppException {
		Vector<Tuple> t2=new Vector<Tuple>();
		int pageNum= binarySearchSelect.findPageContainingValue(value, tName);//-1
		if(pageNum==-1) {
			Object[] midMinMax = SerialMinMaxCount.deser(0, tName); // Deserialize min/max for the mid page
	        Tuple minTuple = (Tuple) midMinMax[0];
	        Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
	        midMinMax = SerialMinMaxCount.deser(SerialCount.deser(tName)-1, tName);
	        Tuple maxTuple = (Tuple) midMinMax[1];
	        Comparable maxVal = (Comparable) maxTuple.getTupleVectors().get(SerialCId.deser(tName));
	        if(value.compareTo(maxVal) > 0 || value.compareTo(minVal) < 0) {
	        	return t2;//is empty
	        }
	        else {
	        	return t2;
	        }
		}
		else {
			Page currentPage = Serial.deser(pageNum, tName);
			//int tupleNum =binarySearchSelect.binarySearchTupleInPageEqual(currentPage, value, SerialCount.deser(tName)-1, tName);
			//System.out.println("tupleNum "+tupleNum);
			int tupleNum =binarySearchSelect.binarySearchTupleInPageEqual(currentPage, 0, currentPage.getSize(), value, SerialCId.deser(tName));
			if(tupleNum!=-1) {
				Tuple c =currentPage.getTuple(tupleNum);
				t2.add(c);
			}
		}
		return t2;
	}
	
	
	
	public static Vector<Tuple> selectusing(String tableName, Comparable value,String operator,int k) {//add the index i 1
		Vector<Tuple> t2 = new Vector<>();
		int tableSize= SerialCount.deser(tableName);
		
		for(int i=0;i<tableSize;i++) {
			Page currentPage = /*t.getPage(i);*/ Serial.deser(i, tableName);
			for(int j=0;j<currentPage.getSize();j++) {
				Tuple currentTuple = currentPage.getTuple(j);
	            Comparable element = (Comparable) currentTuple.getElement(k);//make it general 1
	            
	            // Ensure the element is not null and compare
	            if (element != null && operator.equals("=") && element.compareTo(value)==0) {//
					t2.add(currentPage.getTuple(j));
				}
	            if (element != null && operator.equals(">") && element.compareTo(value)>0) {//
					t2.add(currentPage.getTuple(j));
					//System.out.println("t2 "+t2);
				}
	            if (element != null && operator.equals("<") && element.compareTo(value)<0) {//
					t2.add(currentPage.getTuple(j));
					//System.out.println("t2 "+t2);
				}
	            if (element != null && operator.equals(">=") && element.compareTo(value)>=0) {//
					t2.add(currentPage.getTuple(j));
				}
	            if (element != null && operator.equals("<=") && element.compareTo(value)<=0) {//
					t2.add(currentPage.getTuple(j));
				}
	            if (currentTuple != null && operator.equals("!=") && element.compareTo(value)!=0) {//
		        	t2.add(currentTuple);	
		        }
	            
			}
		}
		//System.out.println("final t2 "+t2);
		return t2;
	}
	public static Vector<Tuple> And(Vector<Tuple> t,Comparable value,String operator,int k) {
		Vector<Tuple> t2 = new Vector<>();
		if(t!=null)
		for(int i=0;i<t.size();i++) {
			Tuple currentTuple = t.get(i);
	        Comparable element = (Comparable) currentTuple.getElement(k);
	        // Ensure the element is not null and compare
	        if (currentTuple != null && operator.equals("=") && element.compareTo(value)==0) {//
	        	t2.add(currentTuple);	
	        }
	        if (element != null && operator.equals(">") && element.compareTo(value)>0) {//
				t2.add(currentTuple);
			}
	        if (element != null && operator.equals("<") && element.compareTo(value)<0) {//
	        	t2.add(currentTuple);		
	        }
	        if (element != null && operator.equals(">=") && element.compareTo(value)>=0) {//
	        	t2.add(currentTuple);		
	        }
            if (element != null && operator.equals("<=") && element.compareTo(value)<=0) {//
            	t2.add(currentTuple);
            }
            if (currentTuple != null && operator.equals("!=") && element.compareTo(value)!=0) {//
	        	t2.add(currentTuple);	
	        }
	         
		}
		return t2;
	}
	public static Vector<Tuple> OR(Vector<Tuple> P1,Vector<Tuple> P2,String tName){//remove intersection 
		//System.out.println("P1: "+P1);
		//System.out.println("P2: "+P2);
		Vector<Tuple> t3 = new Vector<Tuple>();
		t3.addAll(P1);t3.addAll(P2);
		//System.out.println("t3: "+t3);
		return removeDuplicates(t3,tName);
	}
	public static Vector<Tuple> AndNot(Vector<Tuple> t,Comparable value,String operator) {
		Vector<Tuple> t2 = new Vector<>();
		for(int i=0;i<t.size();i++) {
			Tuple currentTuple = t.get(i);
	        Comparable element = (Comparable) currentTuple.getElement(0);
	        // Ensure the element is not null and compare
	        if (currentTuple != null && operator.equals("=") && element.compareTo(value)!=0) {//
	        	t2.add(currentTuple);	
	        }
	        if (element != null && operator.equals(">") && element.compareTo(value)<=0) {//
				t2.add(currentTuple);
			}
	        if (element != null && operator.equals("<") && element.compareTo(value)>=0) {//
	        	t2.add(currentTuple);		
	        }
	        if (element != null && operator.equals(">=") && element.compareTo(value)<0) {//
	        	t2.add(currentTuple);		
	        }
            if (element != null && operator.equals("<=") && element.compareTo(value)>0) {//
            	t2.add(currentTuple);
            }
	         
		}
		return t2;
	}

	
//	public static Vector<Tuple> Xor(Vector<Tuple> P1,Comparable value1,String operator1,Comparable value2,String operator2){
//		return OR(AndNot(And(P1,value1,operator1),value2,operator2),And(AndNot(P1,value1,operator1),value2,operator2));
//	}
	public static Vector<Tuple> Xor2(Vector<Tuple> P1,Vector<Tuple> P2,Vector<Tuple> Table ,String tName ){
		//System.out.println("P1 "+P1);
		//System.out.println("P2 "+P2);
		//System.out.println(Table);
		return OR(filterCommonElementsManually(Not(Table,P1,tName),P2, tName), filterCommonElementsManually(Not(Table,P2,tName),P1, tName), tName);
	}
	

	
	
	
	public static void printF(Vector<Tuple> t) {
		//System.out.println(t);
		for(int i=0;i<t.size();i++) {
			System.out.println(t.get(i));
			
		}
	}
	public static void printF(String tableName) {
		int tableSize= SerialCount.deser(tableName);
		for(int i=0;i<tableSize;i++) {
			Page currentPage = /*t.getPage(i);*/Serial.deser(i, tableName);
			for(int j=0;j<currentPage.getSize();j++) {
				Tuple currentTuple = currentPage.getTuple(j);
	            //System.out.print(currentTuple.getElement(1));
			}
		}
	}	

	
	public static Vector<Tuple> Not(Vector<Tuple> table,Vector<Tuple> smallTable, String tName){
		Vector<Tuple> filtered = new Vector<Tuple>();

		//System.out.println("tableBefore "+table);
		//System.out.println("smallTableBefore "+smallTable);
		
		boolean flag= false;
	    for(int i=0;i<table.size();i++) {
	    	//filtered.remove(smallTable.get(i));
	    	flag=false;
	    	for(int x=0; x<smallTable.size(); x++) {
	    		if(((Comparable) table.get(i).getElement(SerialCId.deser(tName))).compareTo(smallTable.get(x).getElement(SerialCId.deser(tName)))==0) {
	    			flag= true;
	    		}
	    	}
	    	if(flag== false) {
	    		filtered.add(table.get(i));
	    	}
	    }
		
		//filtered.removeAll(smallTable);

	    //System.out.println("filtered "+filtered);
	    return filtered;
	}


	public static Vector<Tuple> filterCommonElementsManually(Vector<Tuple> largeVector, Vector<Tuple> smallVector, String tName) {
    Vector<Tuple> commonElements = new Vector<>();//like the and gets the intersection
    
    // System.out.println("largeTuplebeforefor "+largeVector);
    // System.out.println("smallVectorbeforefor "+smallVector);
    for (Tuple largeTuple : largeVector) {
    	//System.out
        for (Tuple smallTuple : smallVector) {
        	//System.out.println("SerialCId.deser(tName) "+ SerialCId.deser(tName));
            if (((Comparable) largeTuple.getElement(0)).compareTo(smallTuple.getElement(0))==0) {
            	//System.out.println("largeTuple 0 "+largeTuple.getElement(0));
            	//System.out.println("smallTuple 0 "+smallTuple.getElement(0));
                commonElements.add(largeTuple);
                //System.out.println("commomElements "+commonElements);
                break; // Break to avoid adding duplicates if smallVector has duplicates
            }
        }
    }
    //System.out.println("commomElementsbeforereturn "+commonElements);
    return commonElements;
}
	


//(id>5 and id>2 and id<20) or (id<5 and id>2 and id!=3) 
	
	
	
	public static Vector<Tuple> removeDuplicates(Vector<Tuple> original, String tableName) {
	    Vector<Tuple> uniqueTuples = new Vector<>();
	    
	    //System.out.println("original "+original);
	    //System.out.println()

	    // Iterate through each tuple in the original vector
	    for (int i = 0; i < original.size(); i++) {
	        boolean isDuplicate = false;
	        Tuple currentTuple = original.get(i);

	        // Compare with all tuples added to the unique list
	        for (Tuple uniqueTuple : uniqueTuples) {
	            if (((Comparable) currentTuple.getElement(SerialCId.deser(tableName))).compareTo((uniqueTuple.getElement(SerialCId.deser(tableName))))==0) {
	                isDuplicate = true;
	                break;
	            }
	        }

	        // If it was not found in the unique list, add it
	        if (!isDuplicate) {
	            uniqueTuples.add(currentTuple);
	        }
	    }

	    return uniqueTuples;
	}

	public static Vector<Tuple> tabletoVector(String tableName){
		Vector<Tuple> v=new Vector<Tuple>();
		int tableSize= SerialCount.deser(tableName);
		for(int i=0;i<tableSize;i++) {
			Page currentPage = /*t.getPage(i);*/Serial.deser(i, tableName);			
			for(int j=0;j<currentPage.getSize();j++) {
				Tuple currentTuple = currentPage.getTuple(j);
	            Comparable element = (Comparable) currentTuple.getElement(0);
	            v.add(currentTuple);
			}
		}
		
		return v;
	}
	public static Vector<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[]  strarrOperators) throws DBAppException{
		Vector<Tuple> current=null;
		Vector<Tuple> tmp=null;
		Vector<Tuple> previous=null;
		int j=0;
        String tableName = arrSQLTerms[0]._strTableName;
        Vector<Tuple> Vtable=tabletoVector(tableName);
        String columnName = arrSQLTerms[0]._strColumnName;
        String operator = arrSQLTerms[0]._strOperator;
        Object value = arrSQLTerms[0]._objValue;	
        int tableSize= /*table.getSize();*/SerialCount.deser(tableName);
        int f = getI(tableName,columnName); 
           tmp=selectusing(tableName,(Comparable) value,operator,f);// id=5 and id=10 getI(tableName,columnName,table)
           
           for (int i = 1; i < arrSQLTerms.length; i++) { 
           if(j<strarrOperators.length&&strarrOperators[j].equals("and")) {
        	   tableName = arrSQLTerms[i]._strTableName;
        	   columnName = arrSQLTerms[i]._strColumnName;
        	   operator = arrSQLTerms[i]._strOperator;
        	   value = arrSQLTerms[i]._objValue;
        	   f = getI(tableName,columnName); 
        	   previous =And(tmp,(Comparable) value,operator,f); // id=5 and id=10
           }else if(j<strarrOperators.length&&strarrOperators[j].equals("or")) {
        	   tableName = arrSQLTerms[i]._strTableName;
            columnName = arrSQLTerms[i]._strColumnName;
            operator = arrSQLTerms[i]._strOperator;
            value = arrSQLTerms[i]._objValue;	
            f = getI(tableName,columnName); 
        	   previous =OR(tmp,selectusing(tableName,(Comparable) value,operator,f),tableName);//,getI(tableName,columnName,table) 
        	   
           }
           else if(j<strarrOperators.length&&strarrOperators[j].equals("xor")) {
            columnName = arrSQLTerms[i]._strColumnName;
            operator = arrSQLTerms[i]._strOperator;
            value = arrSQLTerms[i]._objValue;		
        	//previous =Xor(tmp,(Comparable) value,operator,(Comparable)value2,operator2);
            f = getI(tableName,columnName); 
        	previous =Xor2(tmp,selectusing(tableName,(Comparable) value,operator,f),Vtable, tableName);  //getI(tableName,columnName2,table)
           }//else throw exception
           tmp=previous;
           j++;
		}   
		return tmp;
	}
	public static int getI(String tableName,String columnName) {
		ArrayList<String[]> meta;
		int j = 0;
		try {
			meta = Table.metaReader();
			int i = 0;
	        for (; i < meta.size(); i++) {
	            if (meta.get(i)[0].equals(tableName)) {
	                if (meta.get(i)[1].equals(columnName)) {
	                	 return j;
	                }
	                j++;
	            }     
	       }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return -1;
       
	}
	public static Tuple getmin(Vector<Tuple> t) {
		int result=(int) t.get(0).getElement(0);
		Tuple tt=t.get(0);
		for(int i=1;i<t.size();i++) {
			if((int)t.get(i).getElement(0)<result) {
				result=(int)t.get(i).getElement(0);
				tt=t.get(i);
			}
		}
		return tt;
	}
	public static Vector<Tuple> sort(Vector<Tuple> t) {
		return t;
		/*Vector<Tuple> t2 = new Vector<>();
		Tuple e =null;
		int size =t.size();
		for(int i=0;i<size;i++) {
			e=getmin(t);
			t2.add(e);
			t.remove(e);
		}
		return t2;*/
	}
	
	public static Vector<Tuple> selectFromTable2(SQLTerm[] arrSQLTerms, String[]  strarrOperators) throws DBAppException{
		ArrayList<String[]> meta=null;
		try {
			meta = Table.metaReader();
		BTree b=null;
		Vector<Tuple> current=null;
		Vector<Tuple> tmp=null;
		Vector<Tuple> previous=null;
		int j=0;
        String tableName = arrSQLTerms[0]._strTableName;
        Vector<Tuple> Vtable=tabletoVector(tableName);
        //System.out.println(Vtable+" vtable");
        String columnName = arrSQLTerms[0]._strColumnName;
        String operator = arrSQLTerms[0]._strOperator;
        Object value = arrSQLTerms[0]._objValue;	
        int tableSize= /*table.getSize();*/SerialCount.deser(tableName);
        int f = getI(tableName,columnName); 
        if(!meta.get(f)[4].equals("null")) {//there is index
        	//get the bplustree
        	//System.out.print(false);
        	//System.out.println("colName: "+columnName);
        	//System.out.println("tableName: "+tableName);
        	b= /*table.btree.get(f);*/SerialBTree.deser(columnName, tableName);
        	//System.out.println("value :"+value);
        	//System.out.println("operator :"+operator);
            tmp=selectusingB(tableName,(Comparable) value,operator,0, b);
        }
        else if(meta.get(f)[3].equals("TRUE")){
        	tmp= computeOperator(operator,(Comparable) value, tableName);
        }
        else {
           tmp=selectusing(tableName,(Comparable) value,operator,f);
           //System.out.println("tmp final "+tmp);
        }
        //System.out.println("tmp13131 "+tmp);
        for (int i = 1; i < arrSQLTerms.length; i++) { 
           if(j<strarrOperators.length&&strarrOperators[j].equals("and")) {
        	   tableName = arrSQLTerms[i]._strTableName;
        	   columnName = arrSQLTerms[i]._strColumnName;
        	   operator = arrSQLTerms[i]._strOperator;
        	   value = arrSQLTerms[i]._objValue;
        	   f = getI(tableName,columnName); 
        	   //System.out.println("tmp before AND "+j+"   "+tmp);
        	   previous =And(tmp,(Comparable) value,operator,f); // id=5 and id=10
        	   //System.out.println("tmp "+tmp);
           }else if(j<strarrOperators.length&&strarrOperators[j].equals("or")) {
	        	tableName = arrSQLTerms[i]._strTableName;
	            columnName = arrSQLTerms[i]._strColumnName;
	            operator = arrSQLTerms[i]._strOperator;
	            value = arrSQLTerms[i]._objValue;	
	            f = getI(tableName,columnName); 
	            //System.out.println("f: "+f);
	            if(!meta.get(f)[4].equals("null")) {//there is index
	            	//get the bplustree
	            	b= /*table.btree.get(f);*/SerialBTree.deser(columnName, tableName);
	            	previous=OR(tmp,selectusingB(tableName,(Comparable) value,operator,f, b),tableName);
	            }else if(meta.get(f)[3].equals("TRUE")){
	            	previous=OR(tmp,computeOperator(operator,(Comparable) value, tableName),tableName);
	            }else {
	        	   previous =OR(tmp,selectusing(tableName,(Comparable) value,operator,f), tableName);//,getI(tableName,columnName,table) 
	        	   //System.out.println("previous "+previous);
	            }
           }
           else if(j<strarrOperators.length&&strarrOperators[j].equals("xor")) {
	            columnName = arrSQLTerms[i]._strColumnName;
	            operator = arrSQLTerms[i]._strOperator;
	            value = arrSQLTerms[i]._objValue;		
	        	
	            f = getI(tableName,columnName); 
	            if(!meta.get(f)[4].equals("null")) {//there is index
	            	//get the bplustree
	            	b= /*table.btree.get(f);*/SerialBTree.deser(columnName, tableName);
	                previous=Xor2(tmp,selectusingB(tableName,(Comparable) value,operator,1, b),Vtable, tableName);
	            }else if(meta.get(f)[3].equals("TRUE")){
	                previous=Xor2(tmp,computeOperator(operator,(Comparable) value, tableName),Vtable, tableName);

	            }else {
	            	//System.out.println("temp wiudoisdoaojidw "+tmp);
	            	previous =Xor2(tmp,selectusing(tableName,(Comparable) value,operator,f),Vtable, tableName);  //getI(tableName,columnName2,table)
	            	//System.out.println("diwijodjoasodaoijwd "+ previous);
	            }
            }
           tmp=previous;
           //System.out.println("13131 "+j+tmp);
           j++;
		}  
		return sort(tmp);
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public static Vector<Tuple> selectusingB(String tableName,Comparable value,String operator,int k,BTree b) throws DBAppException {//add the index i 1
		Vector<Tuple> t2 = new Vector<>();
		//computeOperator("hana","=")
		//LinkedList<Pointer<String, String>> pointers=b.computeOperator(value,operator);
		//System.out.println(b.computeOperator(value,operator));
		//System.out.println("operator :"+operator);
		LinkedList<Pointer<?, String>> pointers=b.computeOperator(value,operator);
		
		for (Pointer<?, String> pointer : pointers) {
		//for (Pointer<String, String> pointer : pointers) {     //looping over the returned linklist
			String page = pointer.value().split("-")[0];      //get the page number 
			String cluster =pointer.value().split("-")[1];   //get the cluster key to use in binarysearch
			Page myPage = /*t.getPage(Integer.parseInt(page));*/ Serial.deser(Integer.parseInt(page), tableName);// i have the page im looking for
	        //System.out.println(page+" "+cluster);
			
			
			
			//int clusterKey = Integer.parseInt(cluster);    // i have now the cluster key
			
			
			String cldt=SerialCType.deser(tableName);
			Comparable num="";
			
			if (cldt.equals("java.lang.String") || cldt.equals("java.lang.string")) {
				try {
					num = cluster;
				}
				catch(Exception e) {
					throw new DBAppException("Wrong input data type of cluster");
				}
			}
			else if (cldt.equals("java.lang.Integer") ||cldt.equals("java.lang.integer")){
				try {
					num = Integer.parseInt(cluster);
				}
				catch(Exception e) {
					throw new DBAppException("Wrong input data type of cluster");
				}
			}
			else {
				try {
					num = Double.parseDouble(cluster);
				}
				catch(Exception e) {
					throw new DBAppException("Wrong input data type of cluster");
				}
			}
				
			Tuple currentTuple= binarySearchForTuple(myPage, 0, myPage.getSize()-1, num , SerialCId.deser(tableName));
			
			t2.add(currentTuple);
//			int x = 15;
//			Tuple t12 = t.getPage(0).binarySearch((Comparable)(x));
//			if(t12!=null)
//				t2.add(t12);///go to the page and do a binary search using cluster
		}
		return removeDuplicates(t2, tableName);
		
	}
	public static Vector<Tuple> selectusingB2(String tableName,Comparable value,String operator,int k,BTree b) {//add the index i 1
		Vector<Tuple> t2 = new Vector<>();
		//computeOperator("hana","=")
		LinkedList<Pointer<String, String>> pointers=b.computeOperator(value,operator);
		for (Pointer<String, String> pointer : pointers) {     //looping over the returned linklist
			String page = pointer.value().split("-")[0];      //get the page number 
			String cluster =pointer.value().split("-")[1];   //get the cluster key to use in binarysearch
			Page myPage = /*t.getPage(Integer.parseInt(page));*/ Serial.deser(Integer.parseInt(page), tableName);// i have the page im looking for
	        // System.out.println(page+" "+cluster);
			
			int clusterKey = Integer.parseInt(cluster);    // i have now the cluster key
			for (int i = 0; i < myPage.getSize(); i++) {  // 
				Tuple currentTuple = myPage.getTuple(i); // Get the tuple from the page
			    if (currentTuple.getElement(0) != null) {
			        Object element = currentTuple.getElement(0); // Get the first element of the tuple  id
			        if (clusterKey == Integer.parseInt(element+"")) {
		                t2.add(currentTuple); // Add the tuple to the vector if it matches
		            } 
			    }
	            
	        
		}
//			int x = 15;
//			Tuple t12 = t.getPage(0).binarySearch((Comparable)(x));
//			if(t12!=null)
//				t2.add(t12);///go to the page and do a binary search using cluster
		}
		return removeDuplicates(t2, tableName);
		
	}
	
	public static Tuple binarySearchForTuple(Page p, int min, int max, Comparable val,int clId) {
		Tuple tempTuple = null;
        while (min <= max) {
            int mid = (min + max) / 2;
            if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(val)==0) {           	
            	return p.getTuple(mid);
            } 
            else if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(val) > 0) {
                max = mid - 1;
                }
            else if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(val) < 0) {
            	min = mid + 1;
            } 
        }
        //System.out.println("trying to update a tuple that doesn't exist in the table");
        return tempTuple;
    }
	
	public static void main(String[] args) throws DBAppException {
		    Table table = new Table("Student");
		
		    // Create pages and add tuples
		    Page page1 = new Page(null);
		    Tuple t2=new Tuple();t2.addElement(5);t2.addElement("ahmed");t2.addElement(0.9);
		    Tuple t3=new Tuple();t3.addElement(10);t3.addElement("mohsen");t3.addElement(1.5);
		    Tuple t4=new Tuple();t4.addElement(15);t4.addElement("hana");t4.addElement(2.0);
		    Tuple t5=new Tuple();t5.addElement(20);t5.addElement("david");t5.addElement(3.0);
		    Tuple t6=new Tuple();t6.addElement(25);t6.addElement("omar");t6.addElement(3.1);
		    Tuple t7=new Tuple();t7.addElement(30);t7.addElement("adham");t7.addElement(0.7);
		    Tuple t8=new Tuple();t8.addElement(35);t8.addElement("biko");t8.addElement(5.0);
		    page1.addElement(t2);page1.addElement(t3);page1.addElement(t4);
		    
		    Page page2 = new Page("student");
		    page2.addElement(t5);page2.addElement(t6);page2.addElement(t7);page2.addElement(t8);
		    DBApp D= new DBApp();
		
		    table.addPage(page1);
		    table.addPage(page2);
		    //D.add(table);
		    SQLTerm[] a =new SQLTerm[4];
		    String[] b= {"xor"};
		    a[1]=new SQLTerm("Student","id",">", 20);
		    //a[0]=new SQLTerm("Student","name","<","Ahmed");
		    //a[2]=new SQLTerm("Student","gpa",">",1.5);
		    //a[3]=new SQLTerm("Student","gpa","<",3.0);
		    a[0]=new SQLTerm("Student","id",">",35);
		
			//printF(computeOperator("=",40,"Student"));
		
		
		    
		    
		   
	//	   try {
	//			D.createIndex("Student","gpa","gpaIndex");
	//		} catch (DBAppException e1) {
	//			// TODO Auto-generated catch block
	//			e1.printStackTrace();
	//		} 
		    
		   //createIndex(String   strTableName,String   strColName,String   strIndexName)
		    
	 	   
	 	    //System.out.println(DBApp.test(table));
	 	    //System.out.println(getI("Student","id",table));
		}
	public static int[] splitAndConvert(String value) {
        String[] parts = value.split("-");
        int[] numbers = new int[2];
        numbers[0] = Integer.parseInt(parts[0]);
        numbers[1] = Integer.parseInt(parts[1]);
        return numbers;
    }
	public static Vector<Tuple> computeOperator(String operator,Comparable value,String tName) throws DBAppException{
		return switch (operator) {
			case "<" -> getLessThanKeys(value,tName);
			case "<=" -> getLessThanOrEqualKeys(value,tName);
			case ">" -> getMoreThanKeys(value,tName);
			case ">=" -> getMoreThanOrEqualKeys(value,tName);
			case "!=" -> getNotEqualKeys(value,tName);
			case "=" -> getEqualKeys(value,tName);
			default -> new Vector<Tuple>();//throw exception i supposedly handled this case
		};
	}
	
	private static  Vector<Tuple> getMoreThanKeys(Comparable value, String tName) throws DBAppException {
		Vector<Tuple> t2 = new Vector<Tuple>();
		int pageNum= binarySearchSelect.findPageContainingValue(value, tName);//-1
		//System.out.println("pageNum "+pageNum);
		//Page currentPage = Serial.deser(pageNum, tName);
		//System.out.println("currentPage "+currentPage);
		
		if(pageNum==-1) {
			Object[] midMinMax = SerialMinMaxCount.deser(0, tName); // Deserialize min/max for the mid page
	        Tuple minTuple = (Tuple) midMinMax[0];
	        Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
	        midMinMax = SerialMinMaxCount.deser(SerialCount.deser(tName)-1, tName);
	        Tuple maxTuple = (Tuple) midMinMax[1];
	        Comparable maxVal = (Comparable) maxTuple.getTupleVectors().get(SerialCId.deser(tName));
	        if(value.compareTo(maxVal)>0) {
	        	return t2;//is empty
	        }
	        else {
	        	return tabletoVector(tName);
	        }

			
		}
		else {
			int tableSize= SerialCount.deser(tName);
			int j=-1;
			Page currentPage = Serial.deser(pageNum, tName);
			int tupleNum =binarySearchSelect.binarySearchTupleInPageGreater(currentPage, value, SerialCId.deser(tName), tName) ;
			//System.out.println("tupleNum "+tupleNum);
			for(int i=pageNum;i<tableSize;i++) {//looping over the rest of the pages starting from page num
				//System.out.println("tableSize "+tableSize);
				Page currentPage2 = Serial.deser(i, tName);
				//System.out.println("currentPage2 "+currentPage2);
				if(i==pageNum) {
					
					j=tupleNum;
					//System.out.println(j);
				}
				else {
					j=0;
				}
				for(;j<currentPage2.getSize();j++) {//looping over a single page 
					
					Comparable c =(Comparable)currentPage2.getTuple(j).getElement(SerialCId.deser(tName));
					Tuple t= currentPage2.getTuple(j);
					if(c.compareTo(value)!=0 || c.compareTo(value)>0) {
						//System.out.println("t "+t);
						//System.out.println("t2 BEFORE "+t2 +"\n");
						t2.add(t);//add the tuple im on
						//System.out.println("t2 AFTER "+t2);
					}
				}
			}
			
		}
		
		
		
		return t2;
	}
	private static  Vector<Tuple> getMoreThanOrEqualKeys(Comparable value, String tName) throws DBAppException {
		Vector<Tuple> t2 = new Vector<Tuple>();
		int pageNum= binarySearchSelect.findPageContainingValue(value, tName);//-1
		//Page currentPage = Serial.deser(pageNum, tName);
		
		if(pageNum==-1) {
			Object[] midMinMax = SerialMinMaxCount.deser(0, tName); // Deserialize min/max for the mid page
	        Tuple minTuple = (Tuple) midMinMax[0];
	        Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
	        midMinMax = SerialMinMaxCount.deser(SerialCount.deser(tName)-1, tName);
	        Tuple maxTuple = (Tuple) midMinMax[1];
	        Comparable maxVal = (Comparable) maxTuple.getTupleVectors().get(SerialCId.deser(tName));
	        if(value.compareTo(maxVal)>0) {
	        	return t2;//is empty
	        }
	        else {
	        	return tabletoVector(tName);
	        }

			
		}
		else {
			Page currentPage = Serial.deser(pageNum, tName);
			int tableSize= SerialCount.deser(tName);
			int j=-1;
			int tupleNum =binarySearchSelect.binarySearchTupleInPageGreater(currentPage, value, SerialCId.deser(tName), tName) ;
			for(int i=pageNum;i<tableSize;i++) {//looping over the rest of the pages starting from page num
				Page currentPage2 = Serial.deser(i, tName);
				if(i==pageNum) {
					
					j=tupleNum;
					//System.out.println(j);
				}
				else {
					j=0;
				}
				for(;j<currentPage2.getSize();j++) {//looping over a single page 
					Comparable c =(Comparable)currentPage2.getTuple(j).getElement(SerialCId.deser(tName));
					Tuple t= currentPage2.getTuple(j);
					if(c.compareTo(value)>=0) {
						//System.out.println("t "+t);
						//System.out.println("t2 BEFORE "+t2 +"\n");
						t2.add(t);//add the tuple im on
						//System.out.println("t2 AFTER "+t2);
					}
				}
			}
		}			
		return t2;
	}
	
	private static  Vector<Tuple> getLessThanOrEqualKeys(Comparable value, String tName) throws DBAppException {
		Vector<Tuple> t2 = new Vector<Tuple>();
		int pageNum= binarySearchSelect.findPageContainingValue(value, tName);//-1
		//Page currentPage = Serial.deser(pageNum, tName);
		
		if(pageNum==-1) {
			Object[] midMinMax = SerialMinMaxCount.deser(0, tName); // Deserialize min/max for the mid page
	        Tuple minTuple = (Tuple) midMinMax[0];
	        Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
	        midMinMax = SerialMinMaxCount.deser(SerialCount.deser(tName)-1, tName);
	        Tuple maxTuple = (Tuple) midMinMax[1];
	        Comparable maxVal = (Comparable) maxTuple.getTupleVectors().get(SerialCId.deser(tName));
	        if(value.compareTo(maxVal)<0) {
	        	return t2;//is empty
	        }
	        else {
	        	return tabletoVector(tName);
	        }

			
		}
		else {
			int tableSize= SerialCount.deser(tName);
			int j=-1;
			Page currentPage = Serial.deser(pageNum, tName);
			//System.out.println("currentPage "+currentPage);
			int tupleNum =binarySearchSelect.binarySearchTupleInPageSmaller(currentPage, value, SerialCId.deser(tName), tName) ;
			//System.out.println("tupleNum 1 "+tupleNum);
			for(int i=0;i<=pageNum;i++) {//looping over the rest of the pages starting from page num
				Page currentPage2 = Serial.deser(i, tName);
				//System.out.println("currentPage2 "+currentPage2);
				if(i==pageNum) {
					//System.out.println(j);
					for(j=0 ;j<=tupleNum;j++) {//looping over a single page 
						Comparable c =(Comparable)currentPage2.getTuple(j).getElement(SerialCId.deser(tName));
						Tuple t= currentPage2.getTuple(j);
						if(c.compareTo(value)<=0)
							t2.add(t);//add the tuple im on
					}
				}
				else {
					for(j=0;j<currentPage2.getSize();j++) {//looping over a single page 
						Comparable c =(Comparable)currentPage2.getTuple(j).getElement(SerialCId.deser(tName));
						Tuple t= currentPage2.getTuple(j);
						if(c.compareTo(value)<=0)
							t2.add(t);//add the tuple im on
					}
				}
				
			}
		}
		return t2;
	}
	
	private static  Vector<Tuple> getLessThanKeys(Comparable value, String tName) throws DBAppException {
		Vector<Tuple> t2 = new Vector<Tuple>();
		int pageNum= binarySearchSelect.findPageContainingValue(value, tName);//-1
		//Page currentPage = Serial.deser(pageNum, tName);
		
		if(pageNum==-1) {
			Object[] midMinMax = SerialMinMaxCount.deser(0, tName); // Deserialize min/max for the mid page
	        Tuple minTuple = (Tuple) midMinMax[0];
	        Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
	        midMinMax = SerialMinMaxCount.deser(SerialCount.deser(tName)-1, tName);
	        Tuple maxTuple = (Tuple) midMinMax[1];
	        Comparable maxVal = (Comparable) maxTuple.getTupleVectors().get(SerialCId.deser(tName));
	        if(value.compareTo(maxVal)<0) {
	        	return t2;//is empty
	        }
	        else {
	        	return tabletoVector(tName);
	        }

			
		}
		else {
			int tableSize= SerialCount.deser(tName);
			int j=-1;
			Page currentPage = Serial.deser(pageNum, tName);
			//System.out.println("currentPage "+currentPage);
			int tupleNum =binarySearchSelect.binarySearchTupleInPageSmaller(currentPage, value, SerialCId.deser(tName), tName) ;
			//System.out.println("tupleNum "+tupleNum);
			for(int i=0;i<=pageNum;i++) {//looping over the rest of the pages starting from page num
				Page currentPage2 = Serial.deser(i, tName);
				//System.out.println("currentPage2 "+currentPage2);
				if(i==pageNum) {
					//System.out.println(j);
					for(j=0 ;j<=tupleNum;j++) {//looping over a single page 
						Comparable c =(Comparable)currentPage2.getTuple(j).getElement(SerialCId.deser(tName));
						Tuple t= currentPage2.getTuple(j);
						if(c.compareTo(value)<0)
							t2.add(t);//add the tuple im on
					}
				}
				else {
					for(j=0;j<currentPage2.getSize();j++) {//looping over a single page 
						Comparable c =(Comparable)currentPage2.getTuple(j).getElement(SerialCId.deser(tName));
						Tuple t= currentPage2.getTuple(j);
						if(c.compareTo(value)<0)
							t2.add(t);//add the tuple im on
					}
				}
				
			}
		}
		
		
		
		return t2;
	}

	
	
	
	
	
	
	
	
	
	
	public static boolean validOpperator(String operator) {
		if (operator.equals("=")|| operator.equals(">")|| operator.equals("<")   ||  operator.equals(">=")  || operator.equals("<=")  || operator.equals("!=")) {//
			return true;
		}
		return false;
	}

	
	
}
/*
*/	            
   