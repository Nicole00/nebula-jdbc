/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula5.jdbc.statement;

import com.sun.java.swing.plaf.windows.WindowsTabbedPaneUI;
import com.vesoft.nebula5.jdbc.NebulaConnection;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NebulaPreparedStatementImpl extends NebulaStatementImpl implements PreparedStatement {

    private static final Pattern             GQL_PLACEHOLDER_PATTERN = Pattern.compile("\\?(?=[^\"]*(?:\"[^\"]*\"[^\"]*)*$)");
    private              String              rawGql;
    private              Map<Object, Object> parameters;
    private              int                 parameterNumber;

    public NebulaPreparedStatementImpl(NebulaConnection connection, String rawGql) {
        super(connection);
        this.rawGql = rawGql;
        this.parameterNumber = parameterCount(rawGql);
        this.parameters = new HashMap<>();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        return currentResultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        execute();
        return currentAffectNum;
    }

    @Override
    public boolean execute() throws SQLException {
        String gql = replacePlaceHolderWithParam(rawGql);
        return execute(gql);
    }

    @Override
    public int executeUpdate(String gql, int autoGeneratedKeys) throws SQLException {
        execute(gql);
        return currentAffectNum;
    }

    @Override
    public int executeUpdate(String gql, int[] columnIndexes) throws SQLException {
        execute(gql);
        return currentAffectNum;
    }

    @Override
    public int executeUpdate(String gql, String[] columnNames) throws SQLException {
        execute(gql);
        return currentAffectNum;
    }

    @Override
    public boolean execute(String gql, int autoGeneratedKeys) throws SQLException {
        return execute(gql);
    }

    @Override
    public boolean execute(String gql, int[] columnIndexes) throws SQLException {
        return execute(gql);
    }

    @Override
    public boolean execute(String gql, String[] columnNames) throws SQLException {
        return execute(gql);
    }


    public Map<Object, Object> getParameters() {
        return parameters;
    }

    public int getParametersNumber() {
        return parameterNumber;
    }

    /**
     * set params
     **/
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        insertParameter(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        insertParameter(parameterIndex, x);
    }


    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        insertParameter(parameterIndex, null);
    }


    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        insertParameter(parameterIndex, value);
    }


    @Override
    public ResultSet executeQuery(String gql) throws SQLException {
        execute(gql);
        return currentResultSet;
    }

    @Override
    public int executeUpdate(String gql) throws SQLException {
        execute(gql);
        return currentAffectNum;
    }

    @Override
    public void close() throws SQLException {

    }


    @Override
    public int getQueryTimeout() throws SQLException {
        return getQueryTimeout();
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return currentAffectNum;
    }


    @Override
    public int getFetchSize() throws SQLException {
        return currentResultSet.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return currentResultSet.getConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return currentResultSet.getType();
    }


    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }


    @Override
    public int getResultSetHoldability() throws SQLException {
        return currentResultSet.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }


    private int parameterCount(String rawGql) {
        int     count   = 0;
        Matcher matcher = GQL_PLACEHOLDER_PATTERN.matcher(rawGql);
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    protected String replacePlaceHolderWithParam(String rawNGQL) throws SQLException {
        Integer index    = 1;
        String  digested = rawNGQL;

        Matcher matcher = GQL_PLACEHOLDER_PATTERN.matcher(digested);

        while (matcher.find()) {
            if (!parameters.containsKey(index)) {
                throw new SQLException(String.format("Can not get param in index [%d], please check your nGql.", index));
            }
            Object param = parameters.get(index);
            if (param != null) {
                String paramTypeName = param.getClass().getTypeName();
                switch (paramTypeName) {
                    case ("java.lang.String"):
                        param = String.format("\"%s\"", param);
                        break;
                    case ("java.sql.Date"):
                        param = String.format("date(\"%s\")", param);
                        break;
                    case ("java.util.Date"):
                    case ("java.time.LocalDateTime"):
                        String datetimeString = datetimeFormatter.format(param);
                        param = String.format("local_datetime(\"%s\")", datetimeString);
                        break;
                    case ("java.sql.Time"):
                    case ("java.time.LocalTime"):
                        String localTimeString = timeFormatter.format(param);
                        param = String.format("local_time(\"%s\")", localTimeString);
                        break;
                    case (" java.time.OffsetTime"):
                        String zonedTimeString = zonedTimeFormatter.format(param);
                        param = String.format("zoned_time(\"%s\")", zonedTimeString);
                        break;
                    case (" java.time.OffsetDateTime"):
                        String zonedDatetimeString = zonedDatetimeFormatter.format(param);
                        param = String.format("zoned_datetime(\"%s\")", zonedDatetimeString);
                        break;
                    case ("java.time.Duration"):
                        param = String.format("duration(\"%s\")", (Duration) param);
                        break;
                    default:
                        break;
                }
            }
            digested = GQL_PLACEHOLDER_PATTERN.matcher(digested).replaceFirst(Objects.toString(param));
            index++;
        }

        return digested;
    }

    public void insertParameter(int parameterIndex, Object obj) throws SQLException {
        this.checkParamIndex(parameterIndex);
        this.parameters.put(parameterIndex, obj);
    }

    private void checkParamIndex(int paramIndex) throws SQLException {
        if (paramIndex > parameterNumber) {
            throw new SQLException("param index out of bounds, index(start from 1) "
                                           + paramIndex + " out of bound for size "
                                           + parameterNumber);
        }
    }

    private static SimpleDateFormat datetimeFormatter      = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ssssss");
    private static SimpleDateFormat zonedDatetimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ssssssZ");
    private static SimpleDateFormat timeFormatter          = new SimpleDateFormat("HH:mm:ss.sssssss");
    private static SimpleDateFormat zonedTimeFormatter     = new SimpleDateFormat("HH:mm:ss.sssssssZ");


    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }
}
