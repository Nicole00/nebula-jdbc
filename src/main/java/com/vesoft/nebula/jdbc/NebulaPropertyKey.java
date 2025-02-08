/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

public enum NebulaPropertyKey {
    USER("user", false),
    PASSWORD("password", false),
    ADDRESS("address", false),
    HOST("host", false),
    PORT("port", false),
    PATH("path", false),

    SCHEMA("schema", false),
    TIMEZONE("timezone", false),
    DBNAME("graphName", false),

    MAXCLIENTSIZE("maxClientSize", false),
    MINCLIENTSIZE("minClientSize", false),
    CONNECTTIMEOUT("connectTimeout", false),
    REQUESTTIMEOUT("requestTimeout", false),
    MAXWAITTIME("maxWaitTime", false),
    ;

    private String  keyName;
    private boolean isCaseSensitive;

    private NebulaPropertyKey(String keyName, boolean isCaseSensitive) {
        this.keyName = keyName;
        this.isCaseSensitive = isCaseSensitive;
    }

    public String getKeyName() {
        return this.keyName;
    }
}
