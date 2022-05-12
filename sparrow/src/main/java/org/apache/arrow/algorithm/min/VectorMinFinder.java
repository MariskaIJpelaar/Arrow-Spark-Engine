package org.apache.arrow.algorithm.min;

import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.ValueVector;

/**
 * 2022-05-12
 * Inspiration from: <a href="https://github.com/apache/arrow/blob/master/java/algorithm/src/main/java/org/apache/arrow/algorithm/search/VectorSearcher.java">https://github.com/apache/arrow/blob/master/java/algorithm/src/main/java/org/apache/arrow/algorithm/search/VectorSearcher.java</a>
 */

public final class VectorMinFinder {
    /**
     * Result returned when finding fails
     */
    public static final int FIND_FAIL_RESULT = -1;

    /**
     * Search for the minimum value in a vector by traversing the vector in sequence
     * @param targetVector the vector from which to perform the search
     * @param comparator the criterion for element (in)equality
     * @return the index of the minimum value, if there is any value, else -1
     * @param <V> the vector type
     */
    public static <V extends ValueVector> int linearSearch(V targetVector, VectorValueComparator<V> comparator) {
        if (targetVector.getValueCount() < 1)
            return FIND_FAIL_RESULT;

        comparator.attachVector(targetVector);
        int keyIndex = 0;
        for (int i = 1; i < targetVector.getValueCount(); ++i) {
            if (comparator.compare(keyIndex, i) > 0)
                keyIndex = i;
        }
        return keyIndex;
    }
}
