/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.values;

import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import java.util.List;

public class NebulaRecord {
    private final List<String>       keys;
    private final List<ValueWrapper> values;

    public NebulaRecord(List<String> keys, List<ValueWrapper> values) {
        this.keys = keys;
        this.values = values;
    }

    /**
     * if the record columns contain the given key
     *
     * @param key the given column name
     * @return true if record contains the given key
     */
    public boolean containsKey(String key) {
        return keys.contains(key);
    }

    /**
     * get the column size
     *
     * @return size of the columns
     */
    public int size() {
        return keys.size();
    }

    /**
     * Retrieve the value at the given index
     *
     * @param index the index for which to retrieve the value
     * @return the value at then given index
     * @throws IndexOutOfBoundsException – if the index is out of range (index < 0 || index >= size())
     */
    public ValueWrapper get(int index) {
        return values.get(index);
    }

    /**
     * Retrieve the value of the given column name
     *
     * @param columnName the column name
     * @return the value of the given column name
     */
    public ValueWrapper get(String columnName) {
        int index = keys.indexOf(columnName);
        return get(index);
    }
}
