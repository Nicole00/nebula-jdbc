/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.net.NebulaClient;
import com.vesoft.nebula.jdbc.statement.NebulaPreparedStatementImpl;
import com.vesoft.nebula.jdbc.statement.NebulaStatementImpl;
import org.slf4j.LoggerFactory;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class NebulaConnection implements Connection {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass());

    private Properties properties;

    private NebulaClient client;
    private boolean      closed = false;

    public NebulaConnection(String url, Properties props) throws SQLException {
        try {
            this.properties = UrlParser.parse(url, props);
        } catch (Exception e) {
            throw new SQLException(e);
        }

        NebulaClient.Builder builder = NebulaClient.builder(properties.getProperty(NebulaPropertyKey.ADDRESS.getKeyName()),
                                                            properties.getProperty(NebulaPropertyKey.USER.getKeyName()),
                                                            properties.getProperty(NebulaPropertyKey.PASSWORD.getKeyName()));
        builder.withConnectTimeoutMills((int) properties.getOrDefault(NebulaPropertyKey.CONNECTTIMEOUT.getKeyName(), 3000));
        builder.withRequestTimeoutMills((int) properties.getOrDefault(NebulaPropertyKey.REQUESTTIMEOUT.getKeyName(), 5000));
        try {
            client = builder.build();
            if (properties.containsKey(NebulaPropertyKey.SCHEMA.getKeyName())) {
                ResultSet res = client.execute(String.format("SESSION SET SCHEMA \"%s\"", properties.getProperty(NebulaPropertyKey.SCHEMA.getKeyName())));
                if (!res.isSucceeded()) {
                    throw new RuntimeException("SESSION SET SCHEMA failed: " + res.getErrorMessage());
                }
            }
            if (properties.containsKey(NebulaPropertyKey.DBNAME.getKeyName())) {
                ResultSet res = client.execute("SESSION SET GRAPH " + properties.getProperty(NebulaPropertyKey.DBNAME.getKeyName()));
                if (!res.isSucceeded()) {
                    throw new RuntimeException("SESSION SET GRAPH failed: " + res.getErrorMessage());
                }
            }
            if (properties.containsKey(NebulaPropertyKey.TIMEZONE.getKeyName())) {
                ResultSet res = client.execute(String.format("SESSION SET TIME ZONE \"%s\"", properties.getProperty(NebulaPropertyKey.TIMEZONE.getKeyName())));
                if (!res.isSucceeded()) {
                    throw new RuntimeException("SESSION SET TIME ZONE failed: " + res.getErrorMessage());
                }
            }
        } catch (Exception e) {
            if (client != null) {
                client.close();
            }
            throw new SQLException(e);
        }
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection already closed.");
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new NebulaStatementImpl(this);
    }

    @Override
    public PreparedStatement prepareStatement(String gql) throws SQLException {
        checkClosed();
        return new NebulaPreparedStatementImpl(this, gql);
    }

    public ResultSet execute(String gql) throws SQLException {
        checkClosed();
        try {
            return client.execute(gql);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        client.close();
        logger.info("JDBCConnection closed.");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String gql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(gql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String gql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(gql);
    }

    @Override
    public CallableStatement prepareCall(String gql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String gql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(gql);
    }

    @Override
    public PreparedStatement prepareStatement(String gql, int[] columnIndexes) throws SQLException {
        return prepareStatement(gql);
    }

    @Override
    public PreparedStatement prepareStatement(String gql, String[] columnNames) throws SQLException {
        return prepareStatement(gql);
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return client.ping();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return (String) properties.getOrDefault(name, "null");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return properties.getProperty(NebulaPropertyKey.DBNAME.getKeyName());
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public long getQueryTimeout() {
        return client.getRequestTimeoutMills();
    }
}
