/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula5.jdbc.values;

import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import java.util.List;

public class NebulaRecord {
    private final List<String>       keys;
    private final List<ValueWrapper> values;

    public NebulaRecord(List<String> keys, List<ValueWrapper> values) {
        this.keys = keys;
        this.values = values;
    }

    public boolean containsKey(String key) {
        return keys.contains(key);
    }

    public int size() {
        return keys.size();
    }

    public ValueWrapper get(int index) {
        return values.get(index);
    }

    public ValueWrapper get(String columnName) {
        int index = keys.indexOf(columnName);
        return get(index);
    }
}
