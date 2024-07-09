package DB;

import java.util.ArrayList;

public class BinarySearchToDelete2 {
	static void binarySearchForPage(Object[] minMax, Table t, int min, int max, Comparable val,ArrayList<Object> a, String tName, ArrayList<Integer> indexOfUsedColumnsInDelete,  ArrayList<ArrayList<Object>> keyAndValue, ArrayList<ArrayList<Object>> btreeCols){
		//System.out.println("here binary");
		//System.out.println("bla"+tName);
		//System.out.println(SerialCId.deser(tName));
		//System.out.println(SerialCType.deser(tName));
        while (min <= max) {
        	Tuple mini=(Tuple)minMax[0];
			Tuple maxi=(Tuple)minMax[1];
            int mid = (min + max) / 2;   
            //System.out.println("middd"+mid);
            //System.out.println("is this null"+page.getTuple(0));
            if ( ((Comparable) mini.getTupleVectors().get(SerialCId.deser(tName))).compareTo(val) <=0 && ((Comparable) maxi.getTupleVectors().get(SerialCId.deser(tName))).compareTo(val) >= 0) {
            	//System.out.println("hhhhhhhhhhhhhhh");
            	Page page=Serial.deser(mid, tName);
            	binarySearchForTuple(page,0,page.getSize()-1,val,SerialCId.deser(tName),a,t,mid,tName, indexOfUsedColumnsInDelete, keyAndValue, btreeCols);
            	return;
            }
            else if (((Comparable) mini.getTupleVectors().get(SerialCId.deser(tName))).compareTo(val) > 0) {
            	//System.out.println("2nd here");
            	SerialMinMaxCount.ser(minMax, mid, tName);
            	//Serial.ser(page, mid, tName);
            	max = mid - 1;
            	int m=(min+max)/2;
            	//page=Serial.deser(m, tName);
            	minMax=SerialMinMaxCount.deser(m, tName);
            }  
            else if (((Comparable)maxi.getTupleVectors().get(SerialCId.deser(tName))).compareTo(val) < 0) {
            	SerialMinMaxCount.ser(minMax, mid, tName);
            	//Serial.ser(page, mid, tName);
            	min = mid + 1;
            	int m=(min+max)/2;
            	minMax=SerialMinMaxCount.deser(m, tName);
            }   
        }
        System.out.println("trying to update a tuple that doesn't exist in the table");
    }
	
	static Tuple binarySearchForTuple2(Page p, int min, int max, Comparable val,int clId) {
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
        System.out.println("trying to update a tuple that doesn't exist in the table");
        return tempTuple;
    }
	
	static void binarySearchForTuple(Page p, int min, int max, Comparable val,int clId,ArrayList<Object> a,Table t, int pageIndex, String tName, ArrayList<Integer> indexOfUsedColumnsInDelete,  ArrayList<ArrayList<Object>> keyAndValue, ArrayList<ArrayList<Object>> btreeCols){
        while (min <= max) {
            int mid = (min + max) / 2;
            System.out.println("p.getTuple(mid).getTupleVectors().get(clId) "+p.getTuple(mid).getTupleVectors().get(clId));
            System.out.println("val "+val);
            if (((Comparable)(p.getTuple(mid).getTupleVectors().get(clId))).compareTo((Comparable)val)==0) {           	
            	for(int i=0; i < keyAndValue.size(); i++) {
            		Comparable element= (Comparable)p.getTuple(mid).getElement(indexOfUsedColumnsInDelete.get(i));
            		for(int j=0; j < keyAndValue.size(); j++) {
            			if(keyAndValue.get(j).get(1)==indexOfUsedColumnsInDelete.get(i)) {
            				if(!(((Comparable) keyAndValue.get(j).get(0)).compareTo(element) == 0)) {
            					System.out.println("not same");
            					return;
            				}
            			}
            		}
            	}
            	for(int i=0;i<btreeCols.size();i++) {
            		System.out.println(mid);
					BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tName);
					Comparable value= (Comparable)p.getTuple(mid).getElement((int)btreeCols.get(i).get(1));
					Comparable theCluster= (Comparable) p.getTuple(mid).getElement(clId);
					//bTree.delete(""+needForUpdate.getElement(x), mmm+"-"+num);
					
					
					System.out.println("what i delete "+p.getTuple(mid).getElement((int)btreeCols.get(i).get(1))+","+ pageIndex+"-"+theCluster+"");
					bTree.delete(""+p.getTuple(mid).getElement((int)btreeCols.get(i).get(1)), pageIndex+"-"+theCluster); //0- alashan di awal page
					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tName);
				}
            	p.removeElement(p.getTuple(mid));
            	Object[] tupleMinMax= {p.getTuple(0),p.getTuple(p.getSize()-1), p.getSize()};
            	SerialMinMaxCount.ser( tupleMinMax, pageIndex, tName);
            	Serial.ser(p, pageIndex, tName);
            	
            	Defragment2.defragment(t,pageIndex, tName, btreeCols);
            	return;
            } 
            else if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(val) > 0) {
                max = mid - 1;
                }
            else if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(val) < 0) {
            	min = mid + 1;
            } 
        }
        System.out.println("trying to update a tuple that doesn't exist in the table");
    }
	
}
