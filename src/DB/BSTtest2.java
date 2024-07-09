package DB;

import java.util.ArrayList;

public class BSTtest2 {
	 public static int BSTtest2(Page p, int start, int end, ArrayList<Object> tupleToAdd, int clusteringIndex) throws DBAppException{
	        while (start <= end) {
	            int mid = (start + end) / 2;
	            
	            ///////////////////////////
	            if(((Comparable)p.getTuple(start).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) == 0 || ((Comparable)p.getTuple(end).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) == 0 || ((Comparable)p.getTuple(mid).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) == 0){
	            	throw new DBAppException ("you are trying to insert a duplicate tuple");
	            }
	            ///////////////////////////
	            
	            if (((Comparable)p.getTuple(start).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) < 0) { //assuming en el cluster int
	            	if (end-start==1 && ((Comparable)p.getTuple(end).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) > 0){
	            		return end;
	            	}
	            }
	            if (((Comparable)p.getTuple(start).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) > 0) { //assuming en el cluster int
	            	if (end-start==1 && ((Comparable)p.getTuple(end).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) > 0) {
	            		return start; //check 9.4
	            	}
	            }
	            if (((Comparable)p.getTuple(start).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) < 0) { //assuming en el cluster int
	            	if (end-start==0 /*&& ((Comparable)table.getPage(startPageIndex).getTuple(end).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) < 0*/) {
	            		if(end+1 <= p.size && ((Comparable)p.getTuple(end+1).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) > 0){
	            			return end+1;
	            		}
	          
	            	}
	            }
	            
	            if (((Comparable)p.getTuple(mid).getElement(clusteringIndex)).compareTo((Comparable)tupleToAdd.get(clusteringIndex)) > 0) {
	                end = mid - 1;
	            }
	            
	            else {
	            	start = mid + 1;
	            } 
	            
	        }
	        return -1; //should never return -1
	    }
}
