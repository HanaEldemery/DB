package DB;

import java.util.Vector;

public class getLessThanOrEqualKeys {

    // Method to perform binary search on the page to find the boundary index for "less than or equal"
    static int binarySearchTupleInPageForLessThanOrEqual(Page page, Comparable value, int keyIndex, String tName) throws DBAppException {
        int low = 0;
        int high = page.getSize() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = page.getTuple(mid);
            Comparable midValue = (Comparable) midTuple.getElement(keyIndex);
            if (midValue.compareTo(value) > 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return high; // Return the highest index where the value is less than or equal to the search value
    }

    // Modified method to collect tuples that are "less than or equal to" the search key
    public static Vector<Tuple> getLessThanOrEqualKeys(Comparable value, String tName) throws DBAppException {
        Vector<Tuple> results = new Vector<>();
        int pageNum = 0;
        Page currentPage;
        boolean continueSearch = true;

        // Iterate over pages and collect tuples that meet the condition
        while (pageNum < SerialCount.deser(tName) && continueSearch) {
            currentPage = Serial.deser(pageNum, tName);
            int tupleIndex = binarySearchTupleInPageForLessThanOrEqual(currentPage, value, 0, tName); // Assuming key is at index 0

            for (int i = 0; i <= tupleIndex; i++) {
                results.add(currentPage.getTuple(i));
            }
            // If we're at the last tuple within boundary, stop further page processing
            if (tupleIndex < currentPage.getSize() - 1) {
                continueSearch = false;
            }
            pageNum++;
        }

        return results;
    }
}
