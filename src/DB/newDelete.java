package DB;

import java.util.ArrayList;

public class newDelete {
	public static void deleteWithAnd(Page p, Tuple tuple, ArrayList<ArrayList<Object>> keyAndValue, int indexOfTupleInPage, String tableName, int indexOfPage) {
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
					deleteWithAnd(nextPage, firstTupleNextPage, keyAndValue, 0, tableName, (indexOfPage + 1));
				}
				else { //no next page
					Serial.ser(p, indexOfPage, tableName);
					return;
				}
			}
			else { //was not last tuple in current page
				Tuple nextTuple= p.getTuple((indexOfTupleInPage+1));
				deleteWithAnd(p, nextTuple, keyAndValue, (indexOfTupleInPage+1), tableName, indexOfPage);
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
			
			p.removeElement(tuple);
			Object[] tupleMinMax= {p.getTuple(0),p.getTuple(p.getSize()-1), p.getSize()}; //check 
        	SerialMinMaxCount.ser( tupleMinMax, indexOfPage, tableName); //check
			Serial.ser(p, indexOfPage, tableName);
			
			Table tempTable= SerialTable.deser(tableName);
			Defragment.defragment(tempTable, indexOfPage, tableName);
			
			int tableSize= SerialCount.deser(tableName);
			if(indexOfPage >= tableSize) {
				return;
			}
			else {
				Page page= Serial.deser(indexOfPage, tableName);
				Tuple t= page.getTuple(indexOfTupleInPage);
				deleteWithAnd(page, t, keyAndValue, indexOfTupleInPage, tableName, indexOfPage);
			}

			/*p.removeElement(tuple);
			Serial.ser(p, indexOfPage, tableName);
			Table tablePassParamToDefrag= SerialTable.deser(tableName);
			
			System.out.println("pre defrag "+p);
			
			Defragment.defragment(tablePassParamToDefrag, indexOfPage+1, tableName);
			
			System.out.println("post defrag "+p);
			
			int tableSize= SerialCount.deser(tableName);
			if(tableSize == indexOfPage) {
				Page page= Serial.deser(indexOfPage, tableName);
				Tuple t= page.getTuple(indexOfTupleInPage);
				deleteWithAnd(page, t, keyAndValue, indexOfTupleInPage, tableName, indexOfPage);
			}
			else {
				Page page= Serial.deser(indexOfPage, tableName);
				Tuple t= page.getTuple(indexOfTupleInPage);
				deleteWithAnd(page, t, keyAndValue, indexOfTupleInPage, tableName, indexOfPage);
			}
			
			
			System.out.println("&&&&&&&&&&&&&&&&& "+p);*/
			
			/////////////////////////////////
			/*if((p.getSize()) == 0) {
				Defragment.defragment(tablePassParamToDefrag, indexOfPage, tableName);
				int tableSize=  SerialCount.deser(tableName);
				if(indexOfPage == tableSize-1) {
					return;
				}
				else {
					deleteWithAnd(p, p.getTuple(0), keyAndValue, 0, tableName, indexOfPage);
				}
			}
			else {
				Tuple tupleToUse= p.getTuple(0); //post deletion
				
				System.out.println("wodoasdaw ind: "+ indexOfPage);
				
				Defragment.defragment(tablePassParamToDefrag, indexOfPage, tableName);
				
				System.out.println("&&&&&&&&&&&&&&&&& post defrag "+ p);
				
				deleteWithAnd(p, tupleToUse, keyAndValue, 0, tableName, indexOfPage);
			}*/
			
			//Defragment.defragment(tablePassParamToDefrag, indexOfPage, tableName);
			
			/*int tableSize= SerialCount.deser(tableName);
			if(indexOfPage < (tableSize-1)) { //next page exists
				System.out.println("************tuple "+ tuple);
				System.out.println("************tableSize "+tableSize);
				System.out.println("************indexOfPage "+indexOfPage);
				//Table tablePassParamToDefrag= SerialTable.deser(tableName);
				//Defragment.defragment(tablePassParamToDefrag, indexOfPage, tableName);
				
				/*Page willNotNeedPage= Serial.deser((indexOfPage + 1), tableName);
				Tuple tupleThatWillMoveFromBelowPage= willNotNeedPage.getTuple(0);
				
				System.out.println("************willNotNeedPage "+willNotNeedPage);
				System.out.println("************tupleThatWillMoveFromBelowPage "+tupleThatWillMoveFromBelowPage);
				
				Serial.ser(willNotNeedPage, (indexOfPage + 1), tableName);*/
				
				
				//deleteWithAnd(p, tuple, keyAndValue, indexOfTupleInPage, tableName, indexOfPage);
			//}
			//else { //no next page
				//Serial.ser(p, indexOfPage, tableName);
				//return;
			//}
		}
	}
}
