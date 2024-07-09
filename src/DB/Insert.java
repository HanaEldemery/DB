package DB;

import java.util.ArrayList;

public class Insert {
	
	public static Tuple arrayListToTuple(ArrayList<Object> tupleToAdd) { //4.4 recheck
		Tuple vector = new Tuple();
		
        for(int i=0;i<tupleToAdd.size();i++) {
            vector.addElement(tupleToAdd.get(i));
        }
        return vector;
	}
	
	public static ArrayList<Object> tupleToArrayList(Tuple tupleToAdd){ //4.4 recheck
		ArrayList<Object> AL= new ArrayList<Object>();
		
		for(int i=0;i<tupleToAdd.getSize();i++) {
            AL.add(tupleToAdd.getElement(i));
        }
        return AL;
	}
	
	public static void over(Page page, int pageIndex, Tuple overflowingTuple, Table table, int indexOfCluster, String tName,ArrayList<ArrayList<Object>> btreeCols) {
		String tableName=tName;
		
		//if no next page
		//new page 
		//insert in first tuple in new page
		//t.add(new page)
		//serialize page and new page
		//return 
		
		if (pageIndex==SerialCount.deser(tableName)-1) {//if no next page
			Page newPage=new Page("./src/DB/"+tableName+pageIndex+".ser");
			newPage.addElement(overflowingTuple);
			Object[] tupleMinMax= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
			SerialMinMaxCount.ser( tupleMinMax, pageIndex, tableName);
			Object[] tupleMinMaxNext= {newPage.getTuple(0),newPage.getTuple(newPage.getSize()-1), newPage.getSize()};
			SerialMinMaxCount.ser( tupleMinMaxNext, (pageIndex+1), tableName);
			Serial.ser(page,pageIndex,tableName);
			Serial.ser(newPage, pageIndex+1 , tableName);
			int oldC=SerialCount.deser(tableName);
			SerialCount.ser((oldC+1),tableName);
			for(int i=0;i<btreeCols.size();i++) {
				BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
				Comparable value= (Comparable)overflowingTuple.getElement((int)btreeCols.get(i).get(1));
				Comparable theCluster= (Comparable) overflowingTuple.getElement(indexOfCluster);
				bTree.delete(""+overflowingTuple.getElement((int)btreeCols.get(i).get(1)), pageIndex+"-"+theCluster); //check
				bTree.insert(""+overflowingTuple.getElement((int)btreeCols.get(i).get(1)), (pageIndex+1)+"-"+theCluster); //0- alashan di awal page
				SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
			}
			return;
		}
		
		//if next page 
		//1st case->not full
		//insert and shift in next page 
		//int placement=BSTtest.BSTtest(page, 0, page.getSize()-1, tupleToAdd, indexOfCluster);//this gets the supposed index of tuple to be added 
		//page.insertElementAt(vector,placement);//this inserts and shifts
		
		else {//if next page not full
			//System.out.println("fine");
			Page pnext=Serial.deser(pageIndex+1, tableName);
			if (pnext.getSize()<pnext.size) {
				System.out.println("fine");
				ArrayList<Object> array= tupleToArrayList(overflowingTuple);
				int placement=0;//this gets the supposed index of tuple to be added 
				pnext.insertElementAt(overflowingTuple,placement);//this inserts and shifts
				Object[] tupleMinMax= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
				SerialMinMaxCount.ser( tupleMinMax, pageIndex, tableName);
				Object[] tupleMinMaxNext= {pnext.getTuple(0),pnext.getTuple(pnext.getSize()-1), pnext.getSize()};
				SerialMinMaxCount.ser( tupleMinMaxNext, (pageIndex+1), tableName);
				Serial.ser(page, pageIndex, tableName);
				Serial.ser(pnext, pageIndex+1 , tableName);
				for(int i=0;i<btreeCols.size();i++) {
					BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
					Comparable value= (Comparable)overflowingTuple.getElement((int)btreeCols.get(i).get(1));
					Comparable theCluster= (Comparable) overflowingTuple.getElement(indexOfCluster);
					bTree.delete(""+overflowingTuple.getElement((int)btreeCols.get(i).get(1)), (pageIndex)+"-"+theCluster); //check
					bTree.insert(""+overflowingTuple.getElement((int)btreeCols.get(i).get(1)), (pageIndex+1)+"-"+theCluster); //0- alashan di awal page
					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
				}
				return;
			}
			else {//if next page full
				Object[] tupleMinMax= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
				SerialMinMaxCount.ser( tupleMinMax, pageIndex, tableName);
				Serial.ser(page, pageIndex, tableName);
				int placement=0;
				pnext.insertElementAt(overflowingTuple, placement);//insert at first slot in next page
				for(int i=0;i<btreeCols.size();i++) {
					BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
					Comparable value= (Comparable)overflowingTuple.getElement((int)btreeCols.get(i).get(1));
					Comparable theCluster= (Comparable) overflowingTuple.getElement(indexOfCluster);
					bTree.delete(""+overflowingTuple.getElement((int)btreeCols.get(i).get(1)), pageIndex+"-"+theCluster); //check
					bTree.insert(""+overflowingTuple.getElement((int)btreeCols.get(i).get(1)), (pageIndex+1)+"-"+theCluster); //0- alashan di awal page
					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
				}
				Tuple overflow=pnext.relocateExtra();
				over(pnext,pageIndex+1,overflow,table,indexOfCluster, tableName, btreeCols);//should i add a return?

			}
			
		}
	}
	
	
	public static void getPageToInsert(Object[] minMax, int start, int end, ArrayList<Object> tupleToAdd, String tableName, int indexOfCluster, ArrayList<ArrayList<Object>> btreeCols) throws DBAppException {
		Tuple min=(Tuple)minMax[0];
		Tuple max=(Tuple)minMax[1];
		int ps=(int)minMax[2];
		Tuple vector= arrayListToTuple(tupleToAdd);
		Comparable valueClusterToAdd= (Comparable)tupleToAdd.get(indexOfCluster);
		Table table= Table.getTableByName(tableName);
		int mid=(start+end)/2;
		
		//in this page
		//bigger than first 
		//smaller than last 
		 
		//there are 2 cases
		//page not full->insert and shift
		//page full->insert and handle overflow
		
		/////////////////////////
		if (valueClusterToAdd.compareTo(min.getElement(indexOfCluster)) == 0 || valueClusterToAdd.compareTo(max.getElement(indexOfCluster)) == 0) {
			throw new DBAppException ("you are trying to insert a duplicate cluster key");
		}
		/////////////////////////
		
		if (valueClusterToAdd.compareTo(min.getElement(indexOfCluster)) >0 && valueClusterToAdd.compareTo(max.getElement(indexOfCluster))<0) {//ana fel page el sah fa ha deserialize el page
			Page page=Serial.deser(mid, tableName);
			if (page.getSize()<page.size) { 
				int placement=BSTtest2.BSTtest2(page, 0, page.getSize()-1, tupleToAdd, indexOfCluster);//this gets the supposed index of tuple to be added 
				page.insertElementAt(vector,placement);//this inserts and shifts
				Object[] tupleMinMax= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
				SerialMinMaxCount.ser( tupleMinMax, mid, tableName);
				Serial.ser(page,mid,tableName);
				for(int i=0;i<btreeCols.size();i++) {
					BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
					Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
					Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
					bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), mid+"-"+theCluster); //0- alashan di awal page
					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
				}
				return;
			}
			else {//page.getSize()==page.size
				int placement=BSTtest2.BSTtest2(page, 0, page.getSize()-1, tupleToAdd, indexOfCluster);////this gets the supposed index of tuple to be added if the page will overflow
				page.insertElementAt(vector, placement);
				Tuple overflowFromPrev=page.relocateExtra();
				for(int i=0;i<btreeCols.size();i++) {
					BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
					Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
					Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
					bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), mid+"-"+theCluster); //0- alashan di awal page
					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
				}
				over(page,mid,overflowFromPrev,table,indexOfCluster, tableName, btreeCols);//i give over the page with the overflow
				return;
			}	
		}
		
		//if smaller than 1st
		//3 cases
		//1- mid==0(hahot fe awel page)
		//2- bigger than last in mid-1
		//3- smaller than last in mid-1
		
		if (valueClusterToAdd.compareTo(min.getElement(indexOfCluster)) < 0 ) {
			if (mid==0) {//asghar men awel haga fe awel page
				Page page=Serial.deser(mid, tableName);
				if (page.getSize()<page.size) {//not full -> insert and shift
					int placement=0;
					page.insertElementAt(vector,placement);
					Object[] tupleMinMax= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
					SerialMinMaxCount.ser( tupleMinMax, mid, tableName);
					Serial.ser(page, mid, tableName);
					for(int i=0;i<btreeCols.size();i++) {
						BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
						Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
						Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
						bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), mid+"-"+theCluster); //0- alashan di awal page
						SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
					}

					return;
				}
				else {//full -> insert and overflow
					int placement=0;////this gets the supposed index of tuple to be added if the page will overflow
					page.insertElementAt(vector, placement);
					Tuple overflowFromPrev=page.relocateExtra();
					for(int i=0;i<btreeCols.size();i++) {
						BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
						Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
						Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
						bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), mid+"-"+theCluster); //0- alashan di awal page
						SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
					}
					over(page,mid,overflowFromPrev,table,indexOfCluster, tableName, btreeCols);//i give over the page with the overflow
					return;
				}				
			}
			else {//ana mesh fe awel page
				Object[] minMaxPrev=SerialMinMaxCount.deser((mid-1), tableName);
				Tuple minPrev=(Tuple)minMaxPrev[0];
				Tuple maxPrev=(Tuple)minMaxPrev[1];
				if (valueClusterToAdd.compareTo(maxPrev.getElement(indexOfCluster)) > 0) {//if bigger than last in prev
					SerialMinMaxCount.ser( minMaxPrev, (mid-1), tableName);
					Page page=Serial.deser(mid,tableName);
					if (page.getSize()<page.size) {//page not full
						int placement=0;
						page.insertElementAt(vector,placement);
						Object[] tupleMinMax= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
						SerialMinMaxCount.ser( tupleMinMax, mid, tableName);
						Serial.ser(page, mid, tableName);
						for(int i=0;i<btreeCols.size();i++) {
							BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
							Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
							Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
							bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), mid+"-"+theCluster); //0- alashan di awal page
							SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
						}
						return;
					}
					else {//page full
						int placement=0;////this gets the supposed index of tuple to be added if the page will overflow
						page.insertElementAt(vector, placement);
						Tuple overflowFromPrev=page.relocateExtra();
						for(int i=0;i<btreeCols.size();i++) {
							BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
							Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
							Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
							bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), mid+"-"+theCluster); //0- alashan di awal page
							SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
						}
						over(page,mid,overflowFromPrev,table,indexOfCluster, tableName, btreeCols);//i give over the page with the overflow
						return;
					}
				}
				else {//smaller than last in prev
					SerialMinMaxCount.ser(minMax, mid, tableName);
					int s=start;
					int e=mid-1;
					int m= (s+e)/2;
					Object[] newMinMax=SerialMinMaxCount.deser(m, tableName);
					getPageToInsert(newMinMax,s,e,tupleToAdd,tableName,indexOfCluster, btreeCols);
				}	
			}
		}
		
		//larger than last 
		//2 cases
		//1-not full->insert and shift
		//2-full->no next page->new page
		//      ->next page->recursive call
		
		if (valueClusterToAdd.compareTo(max.getElement(indexOfCluster)) > 0) {//larger than last
			Page useless=new Page("uselesss");
			if (ps < useless.size) {
				Page page=Serial.deser(mid, tableName);
				int placement=page.getSize();
				page.insertElementAt(vector,placement);
				Object[] tupleMinMax= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
				SerialMinMaxCount.ser( tupleMinMax, mid, tableName);
				Serial.ser(page, mid, tableName);
				for(int i=0;i<btreeCols.size();i++) {
					BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
					Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
					Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
					bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), mid+"-"+theCluster); //0- alashan di awal page
					SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
				}
				return;
			}
			else {//mafeesh space
				if (mid==SerialCount.deser(tableName)-1) {//ana kont fe akher page
					Page newPage=new Page("./src/DB/"+tableName+(mid+1)+".ser");
					newPage.addElement(vector);
					SerialMinMaxCount.ser(minMax, mid, tableName);
					Object[] newtupleMinMax= {newPage.getTuple(0),newPage.getTuple(newPage.getSize()-1), newPage.getSize()};
					SerialMinMaxCount.ser( newtupleMinMax, (mid+1), tableName);
					Serial.ser(newPage, mid+1 , tableName);
					for(int i=0;i<btreeCols.size();i++) {
						BTree<String,String> bTree=SerialBTree.deser(""+btreeCols.get(i).get(0), tableName);
						Comparable value= (Comparable)tupleToAdd.get((int)btreeCols.get(i).get(1));
						Comparable theCluster= (Comparable) tupleToAdd.get(indexOfCluster);
						bTree.insert(""+tupleToAdd.get((int)btreeCols.get(i).get(1)), (mid+1)+"-"+theCluster); //0- alashan di awal page
						SerialBTree.ser(bTree,""+btreeCols.get(i).get(0) , tableName);
					}

					int oldC=SerialCount.deser(tableName);
					SerialCount.ser((oldC+1),tableName);
					return;
				}
				else {//lessa fee pages taht
					SerialMinMaxCount.ser(minMax, mid, tableName);
					int s=mid+1;
					int e=end;
					int m= (s+e)/2;
					Object[] newMinMax=SerialMinMaxCount.deser(m, tableName);
					getPageToInsert(newMinMax,s,e,tupleToAdd,tableName,indexOfCluster, btreeCols);
				}
			}
		}	
	}
}
