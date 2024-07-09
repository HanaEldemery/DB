package DB;

import java.util.Vector;

public class LessThanKeysSearch {

    static int binarySearchTupleInPageForLessThan(Page page, Comparable value, int keyIndex, String tName) throws DBAppException {
        int low = 0;
        int high = page.getSize() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = page.getTuple(mid);
            Comparable midValue = (Comparable) midTuple.getElement(keyIndex);
            if (midValue.compareTo(value) >= 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return high; // Return the highest index where the value is less than the search value
    }

    public static Vector<Tuple> getLessThanKeys(Comparable value, String tName) throws DBAppException {
        Vector<Tuple> results = new Vector<>();
        int pageNum = 0;
        Page currentPage;
        boolean continueSearch = true;

        // Continue processing pages until the condition fails
        while (pageNum < SerialCount.deser(tName) && continueSearch) {
            currentPage = Serial.deser(pageNum, tName);
            int tupleIndex = binarySearchTupleInPageForLessThan(currentPage, value, SerialCId.deser(tName), tName); // Assuming key is at index 0

            for (int i = 0; i <= tupleIndex; i++) {
                results.add(currentPage.getTuple(i));
                if (((Comparable) currentPage.getTuple(i).getElement(SerialCId.deser(tName))).compareTo(value) >= 0) {
                    continueSearch = false;
                    break;
                }
            }
            pageNum++;
        }

        return results;
    }
}
