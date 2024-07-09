package DB;

import java.util.Vector;

public class MoreThanKeysSearch {

    static int findPageContainingValue(Comparable value, String tName) throws DBAppException {
        // Assuming SerialCount and other utilities are correctly implemented
        int left = 0;
        int right = SerialCount.deser(tName) - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            Object[] midMinMax = SerialMinMaxCount.deser(mid, tName);
            Tuple minTuple = (Tuple) midMinMax[0];
	        Tuple maxTuple = (Tuple) midMinMax[1];
	        Comparable minVal = (Comparable) minTuple.getTupleVectors().get(SerialCId.deser(tName));
	        Comparable maxVal = (Comparable) maxTuple.getTupleVectors().get(SerialCId.deser(tName));
            if (minVal.compareTo(value) <= 0 && maxVal.compareTo(value) >= 0) {
                return mid;
            }
            if (value.compareTo(minVal) < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return left < SerialCount.deser(tName) ? left : -1;
    }

    static int binarySearchTupleInPage(Page page, Comparable value, int pageIndex, String tName) throws DBAppException {
        int low = 0;
        int high = page.getSize() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = page.getTuple(mid);
            Comparable midValue = (Comparable) midTuple.getElement(0); // Assuming the value to compare is at index 0
            int cmp = midValue.compareTo(value);
            if (cmp == 0) {
                return mid + 1; // Start from next tuple if exact match
            } else if (cmp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return low; // Next tuple position after the last less than the value
    }

    public static Vector<Tuple> getMoreThanKeys(String value, String tName) throws DBAppException {
        Vector<Tuple> results = new Vector<>();
        int pageNum = findPageContainingValue(value, tName);
        if (pageNum == -1) return results; // No suitable page found
        Page currentPage = Serial.deser(pageNum, tName);
        int tupleIndex = binarySearchTupleInPage(currentPage, value, pageNum, tName);

        // Start from the found tuple index and continue to the end of the dataset
        while (pageNum < SerialCount.deser(tName)) {
            for (int j = tupleIndex; j < currentPage.getSize(); j++) {
                results.add(currentPage.getTuple(j));
            }
            pageNum++;
            if (pageNum < SerialCount.deser(tName)) {
                currentPage = Serial.deser(pageNum, tName);
                tupleIndex = 0;  // Reset tuple index for new page
            }
        }
        return results;
    }
}
