package DB;

public class Defragment {
	public static void defragment(Table table, int indexPage, String tName){
		int size= SerialCount.deser(tName);
		if (indexPage==size-1) {//deleting men akher page
			Page p=Serial.deser(indexPage, tName);
			if (p.getSize()==0) {
				//table.removePage(indexPage);
				int oldC=(SerialCount.deser(tName));
				SerialCount.ser((oldC-1), tName);
				Serial.deleteFile("./src/DB/"+tName+indexPage+"MinMaxCount"+".ser");
				Serial.deleteFile("./src/DB/"+tName+indexPage+".ser");
				return;
			}
			else {
				Object[] tupleMinMax= {p.getTuple(0),p.getTuple(p.getSize()-1), p.getSize()};
            	SerialMinMaxCount.ser( tupleMinMax, indexPage, tName);
				Serial.ser(p, indexPage, tName);
				return;
			}
		}		
		else {
			if(indexPage < (size-1)) {//fee page taht
				Page pnext=Serial.deser(indexPage+1, tName);
				Tuple temp= pnext.getTuple(0);
				pnext.removeElement(pnext.getTuple(0));
				Object[] tupleMinMax= {pnext.getTuple(0),pnext.getTuple(pnext.getSize()-1), pnext.getSize()};
            	SerialMinMaxCount.ser( tupleMinMax, (indexPage+1), tName);
				Serial.ser(pnext, indexPage+1, tName);
				
				Page page=Serial.deser(indexPage, tName);
				page.addElement(temp);
				Object[] tupleMinMax2= {page.getTuple(0),page.getTuple(page.getSize()-1), page.getSize()};
            	SerialMinMaxCount.ser( tupleMinMax2, indexPage, tName);
				Serial.ser(page, indexPage, tName);
				
				Object[] pnext2=SerialMinMaxCount.deser((indexPage+1), tName);
				int pnextSize = (int)pnext2[2];
				//Page pnext2=Serial.deser((indexPage+1), tName);
				if(pnextSize==0) {
					//table.removePage(indexPage+1);
					int oldC=(SerialCount.deser(tName));
					SerialCount.ser((oldC-1), tName);
					Serial.deleteFile("./src/DB/"+tName+(indexPage+1)+"MinMaxCount"+".ser");
					Serial.deleteFile("./src/DB/"+tName+(indexPage+1)+".ser");
					return;
				}
				else {
					SerialMinMaxCount.ser(pnext2, (indexPage+1), tName);
					//Serial.ser(pnext2, (indexPage+1), tName);
					defragment(table,(indexPage+1), tName);
				}
			}
		}
	}
}
