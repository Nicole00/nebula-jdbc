/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula5.jdbc;

import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class NebulaDriver implements Driver {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String DRIVER_NAME = "Nebula JDBC Driver";

    static {
        try {
            DriverManager.registerDriver(new NebulaDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register NebulaDriver", e);
        }
    }


    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException(String.format("url: %s is not accepted, example format:jdbc:nebula://ip1:port1,ip2:port2/graphName"));
        }
        return new NebulaConnection(url, properties);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }
        String[] pieces = url.split("//");
        return url.startsWith("jdbc:nebula:") && pieces.length == 2;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
