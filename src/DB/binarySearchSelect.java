package DB;

import java.util.ArrayList;

public class binarySearchSelect {
	
	/*static int binarySearchTupleInPageEqual(Page page, Comparable value, int keyIndex, String tName) throws DBAppException {
		Object[] midMinMax = SerialMinMaxCount.deser(keyIndex, tName); // Deserialize min/max for the mid page
		Tuple minTuple = (Tuple) midMinMax[0];
		Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
		if(value.compareTo(minVal)<0)
			return 0;
		int low = 0;
	    int high = page.getSize() - 1;
	    int mid2=0;
	    while (low <= high) {
	        int mid = low + (high - low) / 2;
	        Tuple midTuple = page.getTuple(mid);
	        if (midTuple == null) {
	            throw new DBAppException("Null tuple found at index: " + mid);
	        }
	        
	        Comparable midValue = (Comparable) midTuple.getElement(keyIndex);
	        int cmp = midValue.compareTo(value);
	        
	        if (cmp == 0) {
	            return mid; // Value found
	        } else if (cmp < 0) {
	            low = mid + 1;
	        } else {
	            high = mid - 1;
	        }
	    }

	    return -1; // Value not found
	}*/
	static int binarySearchTupleInPageEqual(Page p, int min, int max, Comparable val,int clId) {
		int tempTuple = -1;
        while (min <= max) {
            int mid = (min + max) / 2;
            if (((Comparable) p.getTuple(mid).getTupleVectors().get(clId)).compareTo(val)==0) {           	
            	return mid;
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

	static int findPageContainingValue(Comparable value, String tName) throws DBAppException {
	    int left = 0;
	    int right = SerialCount.deser(tName) - 1; // Get the count of serialized pages

	    while (left <= right) {
	        int mid = left + (right - left) / 2;
	        Object[] midMinMax = SerialMinMaxCount.deser(mid, tName); // Deserialize min/max for the mid page
	        Tuple minTuple = (Tuple) midMinMax[0];
	        Tuple maxTuple = (Tuple) midMinMax[1];
	        Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
	        Comparable maxVal = (Comparable) maxTuple.getTupleVectors().get(SerialCId.deser(tName));

	        if (minVal.compareTo(value) <= 0 && maxVal.compareTo(value) >= 0) {
	            return mid; // The value fits within the range of the current mid page
	        }

	        if (value.compareTo(minVal) < 0) {
	            right = mid - 1; // Move to the left half
	        } 
	        else {
	            left = mid + 1; // Move to the right half
	        }
	    }

	    // Special handling to return the index of the next page if value is between the max of one page and the min of the next
	    if (left < SerialCount.deser(tName) && right >= 0) {
	        // When value is greater than max of 'right' and less than min of 'left'
	        Object[] leftMinMax = SerialMinMaxCount.deser(left, tName);
	        Tuple leftMinTuple = (Tuple) leftMinMax[0];
	        Comparable leftMinVal = (Comparable) leftMinTuple.getTupleVectors().get(SerialCId.deser(tName));
	        if (value.compareTo(leftMinVal) < 0) {
	            return left; // Return the index of the page that has the minimum value greater than the searched value
	        }
	    }

	    return -1; // If no suitable page was found
	}
	static int binarySearchTupleInPageGreater(Page page, Comparable value, int keyIndex, String tName) throws DBAppException {
		Object[] midMinMax = SerialMinMaxCount.deser(keyIndex, tName); // Deserialize min/max for the mid page
		Tuple minTuple = (Tuple) midMinMax[0];
		Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
		if(value.compareTo(minVal)<0)
			return 0;
		int low = 0;
	    int high = page.getSize() - 1;
	    int mid2=0;
	    while (low <= high) {
	        int mid = low + (high - low) / 2;
	        Tuple midTuple = page.getTuple(mid);
	        if (midTuple == null) {
	            throw new DBAppException("Null tuple found at index: " + mid);
	        }
	        
	        //System.out.println("keyIndex "+ keyIndex);
	        
	        Comparable midValue = (Comparable)midTuple.getElement(keyIndex);
	        
	        /*Comparable num="";
	        String cldt=SerialCType.deser(tName);
			
	        System.out.println("midValue "+midValue);
	        
			if (cldt.equals("java.lang.String") || cldt.equals("java.lang.string")) {
				num = midValue;
			}
			else if (cldt.equals("java.lang.Integer") ||cldt.equals("java.lang.integer")){
				num = Integer.parseInt((String) midValue);
			}
			else {
				num = Double.parseDouble((String) midValue);
			}*/
	        
	        int cmp = midValue.compareTo(value);
	        
	        if (cmp == 0) {
	            return mid; // Value found
	        } else if (cmp < 0) {
	            low = mid + 1;
	            mid2 = mid+1;
	        } else {
	            high = mid - 1;
	            mid2 = mid;
	        }
	    }

	    return mid2; // Value not found
	}
	static int binarySearchTupleInPageSmaller(Page page, Comparable value, int keyIndex, String tName) throws DBAppException {
		Object[] midMinMax = SerialMinMaxCount.deser(keyIndex, tName); // Deserialize min/max for the mid page
		Tuple minTuple = (Tuple) midMinMax[0];
		Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
		if(value.compareTo(minVal)<0)
			return 0;
		int low = 0;
	    int high = page.getSize() - 1;
	    int mid2=0;
	    while (low <= high) {
	        int mid = low + (high - low) / 2;
	        Tuple midTuple = page.getTuple(mid);
	        if (midTuple == null) {
	            throw new DBAppException("Null tuple found at index: " + mid);
	        }
	        
	        Comparable midValue = (Comparable) midTuple.getElement(keyIndex);
	        int cmp = midValue.compareTo(value);
	        
	        if (cmp == 0) {
	            return mid; // Value found
	        } else if (cmp < 0) {
	            low = mid + 1;
	            mid2 = mid;
	        } else {
	            high = mid - 1;
	            mid2 = mid;
	        }
	    }

	    return mid2; // Value not found
	}
	
	
	
	
	
	
	
	
}
