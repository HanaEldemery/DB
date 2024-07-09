package DB;

import java.util.ArrayList;

public class BinarySearchToUpdate2 {
	
	static void binarySearchForPage2(Object[] minMax, Table t, int min, int max, Comparable num,ArrayList<ArrayList<Object>> keyAndValue, String tName,ArrayList<ArrayList<Object>> btreeCols, ArrayList<String> metaColumnNames) throws DBAppException{
		while (min <= max) {
			Tuple mini=(Tuple)minMax[0];
			Tuple maxi=(Tuple)minMax[1];
            int mid = (min + max) / 2;       
            if ( ((Comparable) mini.getTupleVectors().get(SerialCId.deser(tName))).compareTo(num) <=0 && ((Comparable) maxi.getTupleVectors().get(SerialCId.deser(tName))).compareTo(num) >= 0) {//in this page
            	ArrayList<Object> tuplePostUpdateComponents= new ArrayList<Object>();
            	Page page=Serial.deser(mid, tName);
            	binarySearchForTuple2(page,0,page.getSize()-1,num,SerialCId.deser(tName),keyAndValue,mid,tName,tuplePostUpdateComponents, btreeCols, metaColumnNames);
            	return;
            }
            else if (((Comparable) mini.getTupleVectors().get(SerialCId.deser(tName))).compareTo(num) > 0) {
            	//System.out.println("2nd here");
            	SerialMinMaxCount.ser(minMax, mid, tName);
            	//Serial.ser(page, mid, tName);
            	max = mid - 1;
            	int m=(min+max)/2;
            	//page=Serial.deser(m, tName);
            	minMax=SerialMinMaxCount.deser(m, tName);
            }  
            else if (((Comparable) maxi.getTupleVectors().get(SerialCId.deser(tName))).compareTo(num) < 0) {
            	SerialMinMaxCount.ser(minMax, mid, tName);
            	//Serial.ser(page, mid, tName);
            	min = mid + 1;
            	int m=(min+max)/2;
            	minMax=SerialMinMaxCount.deser(m, tName);
            }   
        }
		throw new DBAppException ("you are trying to update a tuple that doesn't exist in the table");
    }
	
	static void binarySearchForTuple2(Page p, int min, int max, Comparable num,int clId,ArrayList<ArrayList<Object>> keyAndValue, int mmm, String tName, ArrayList<Object> tuplePostUpdateComponents, ArrayList<ArrayList<Object>> btreeCols, ArrayList<String> metaColumnNames) throws DBAppException{
        while (min <= max) {
            int mid = (min + max) / 2;
            if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(num)==0) {
            	//////////////////////
            	Tuple needForUpdate= p.getTuple(mid);
            	int indexOfCluster= SerialCId.deser(tName);
            	
            	Boolean found= false;
            	for(int x = 0; x < needForUpdate.getSize(); x++) {
            		found= false;
            		for(int y = 0; y < keyAndValue.size(); y++) {
            			if(x == (int)keyAndValue.get(y).get(1)) { //can add && flag == false;
            				found= true;
            				tuplePostUpdateComponents.add(keyAndValue.get(y).get(0)); //value is in input hashtable, therefore it is updated value
            				
            				for (int i=0;i<btreeCols.size();i++) {
            					if ((""+btreeCols.get(i).get(0)).equals(keyAndValue.get(y).get(2))) {
	            					BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tName);
	            					if (!(btreeCols.get(i).get(0).equals(SerialCName.deser(tName)))) {
		            					bTree.delete(""+needForUpdate.getElement(x), mmm+"-"+num);
		            					bTree.insert(""+keyAndValue.get(y).get(0), mmm+"-"+num);}
	            					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tName);
            					}	
            				}
            				
            			}
            		}
            		if(found == false) { //value not in input hashtable, therefore get from original (will not update)
            			tuplePostUpdateComponents.add(needForUpdate.getElement(x));
            			for (int i=0;i<btreeCols.size();i++) {
        					if ((""+btreeCols.get(i).get(0)).equals(metaColumnNames.get(x))) {//i need the column name
        						BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tName);
            					bTree.delete(""+needForUpdate.getElement(x), mmm+"-"+num);
            					bTree.insert(""+needForUpdate.getElement(x), mmm+"-"+num);
            					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tName);
        					}	
        				}
            			
        			}
            	}
            	//////////////////////
            	for (int i=0;i<p.getTuple(mid).getTupleVectors().size();i++) {
            		p.getTuple(mid).getTupleVectors().set(i, /*a.get(i)*/tuplePostUpdateComponents.get(i));
            		Object[] tupleMinMax= {p.getTuple(0),p.getTuple(p.getSize()-1), p.getSize()};
        			SerialMinMaxCount.ser( tupleMinMax, mmm, tName);
            		Serial.ser(p, mmm, tName);
                }
            	return;
            } 
            else if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(num) > 0) {
                max = mid - 1;
                }
            else if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(num) < 0) {
            	min = mid + 1;
            } 
        }
        throw new DBAppException ("you are trying to update a tuple that doesn't exist in the table");
    }

}
