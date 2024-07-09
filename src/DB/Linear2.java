package DB;

import java.util.ArrayList;

public class Linear2 {
	public static void deleteWithAnd(Page p, Tuple tuple, ArrayList<ArrayList<Object>> keyAndValue, int indexOfTupleInPage, String tableName, int indexOfPage, ArrayList<ArrayList<Object>> btreeCols) {
		//System.out.println("delete linear");
		//System.out.println(tuple);
		if(tuple == null) {
			return;
		}
		boolean DELETE= true;
		for(int i=0; i < keyAndValue.size(); i++) {
			int index= (int) keyAndValue.get(i).get(1); //type cast because it's an index therefore it's always an int
			Comparable value= (Comparable) keyAndValue.get(i).get(0);
			if(!(((Comparable) tuple.getElement(index)).compareTo(value) == 0)) { //if value being searched for is not in current tuple
				DELETE= false;
			}
		}
		if(DELETE == false) { //not delete
			//(2)	->check if was last tuple in current page
			//			->(2) check if next page exists
			//				->YES
			//					->recurse with nextPage and first Tuple in nextPage
			//				->NO
			//					->serialize and return
			//	 	->check if not last tuple in current page
			//			-> recurse with same page and next tuple
			if(indexOfTupleInPage == (p.size-1)) { //was last tuple in current page
				int tableSize= SerialCount.deser(tableName);
				if(indexOfPage < (tableSize-1)) { //next page exists
					Serial.ser(p, indexOfPage, tableName);
					Page nextPage= Serial.deser((indexOfPage + 1), tableName);
					Tuple firstTupleNextPage= nextPage.getTuple(0);
					deleteWithAnd(nextPage, firstTupleNextPage, keyAndValue, 0, tableName, (indexOfPage + 1),btreeCols);
				}
				else { //no next page
					Serial.ser(p, indexOfPage, tableName);
					return;
				}
			}
			else { //was not last tuple in current page
				Tuple nextTuple= p.getTuple((indexOfTupleInPage+1));
				deleteWithAnd(p, nextTuple, keyAndValue, (indexOfTupleInPage+1), tableName, indexOfPage,btreeCols);
			}
		}
		else { //delete
			//	->delete
			//	->ser
			//	(2)->check if next page exists
			//		->YES
			//			->defragment
			//			->recurse on same page with same tupleIndex
			//		->NO
			//			->serialize
			//			->return
			for(int i=0;i<btreeCols.size();i++) {
				BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
				System.out.println("hererrrer");
				Comparable value= (Comparable)p.getTuple(indexOfTupleInPage).getElement((int)btreeCols.get(i).get(1)); //check
				System.out.println("value "+value);
				Comparable theCluster= (Comparable) p.getTuple(indexOfTupleInPage).getElement(SerialCId.deser(tableName)); //check
				bTree.delete(""+value, indexOfPage+"-"+theCluster); //check
				SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
			}
			p.removeElement(tuple);
			Object[] tupleMinMax= {p.getTuple(0),p.getTuple(p.getSize()-1), p.getSize()}; //check 
        	SerialMinMaxCount.ser( tupleMinMax, indexOfPage, tableName); //check
			Serial.ser(p, indexOfPage, tableName);
			System.out.println("btree "+btreeCols);
			
			Table tempTable= SerialTable.deser(tableName);
			Defragment2.defragment(tempTable, indexOfPage, tableName, btreeCols);
			
			int tableSize= SerialCount.deser(tableName);
			if(indexOfPage >= tableSize) {
				return;
			}
			else {
				Page page= Serial.deser(indexOfPage, tableName);
				Tuple t= page.getTuple(indexOfTupleInPage);
				deleteWithAnd(page, t, keyAndValue, indexOfTupleInPage, tableName, indexOfPage,btreeCols);
			}
		}
	}
}
