/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import com.vesoft.nebula.jdbc.statement.NebulaStatementImpl;
import com.vesoft.nebula.jdbc.values.NebulaRecord;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class NebulaResultSet implements java.sql.ResultSet {

    static final int                 SUPPORTED_FETCH_DIRECTION = java.sql.ResultSet.FETCH_FORWARD;
    static final int                 SUPPORTED_HOLDABILITY     = java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    private      ResultSet           resultSet;
    private      NebulaStatementImpl statement;

    private NebulaRecord currentRecord;
    private ValueWrapper value = null;

    private       int                      rowNumber   = 0;
    private       boolean                  closed      = false;
    private final AtomicBoolean            beforeFirst = new AtomicBoolean(true);
    private final AtomicReference<Boolean> first       = new AtomicReference<>();
    private final AtomicBoolean            last        = new AtomicBoolean(false);
    private final AtomicBoolean            afterLast   = new AtomicBoolean(false);


    public NebulaResultSet(ResultSet resultSet, NebulaStatementImpl statement) {
        this.resultSet = resultSet;
        this.statement = statement;
    }

    @Override
    public boolean next() throws SQLException {
        if (beforeFirst.compareAndSet(true, false)) {
            first.compareAndSet(false, true);
        } else {
            first.compareAndSet(true, false);
        }
        assertIsOpen();
        if (this.resultSet.hasNext()) {
            ResultSet.Record rowRecord = resultSet.next();
            this.currentRecord = new NebulaRecord(resultSet.getColumnNames(), rowRecord.values());
            rowNumber++;
            return true;
        }
        this.currentRecord = null;
        afterLast.compareAndSet(false, true);
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        assertIsOpen();
        if (this.value == null) {
            throw new SQLException("No column has been read prior to this call");
        }
        return value.isNull();
    }

    /**
     * get the {@link ValueWrapper} at given column index
     *
     * @param columnIndex the given column index
     * @return the ValueWrapper at given column index
     */
    private ValueWrapper getValueByColumnIndex(int columnIndex) throws SQLException {
        assertIsOpen();
        assertColumnIndexIsPresent(columnIndex);
        columnIndex--;
        this.value = this.currentRecord.get(columnIndex);
        return this.value;
    }

    /**
     * get the {@link ValueWrapper} pf given column name
     *
     * @param columnName the given column name
     * @return the ValueWrapper of given column name
     */
    private ValueWrapper getValueByColumnName(String columnName) throws SQLException {
        assertIsOpen();
        assertColumnNameIsPresent(columnName);
        this.value = this.currentRecord.get(columnName);
        return this.value;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getValueByColumnIndex(columnIndex).asString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getValueByColumnIndex(columnIndex).asBoolean();
    }


    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) getValueByColumnIndex(columnIndex).asInt();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getValueByColumnIndex(columnIndex).asInt();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getValueByColumnIndex(columnIndex).asLong();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getValueByColumnIndex(columnIndex).asFloat();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getValueByColumnIndex(columnIndex).asDouble();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getValueByColumnIndex(columnIndex).asDecimal().setScale(scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return Date.valueOf(getValueByColumnIndex(columnIndex).asDate());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return Time.valueOf(getValueByColumnIndex(columnIndex).asLocalTime());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ValueWrapper value = getValueByColumnIndex(columnIndex);
        if (value.isLocalDateTime()) {
            return Timestamp.valueOf(value.asLocalDateTime());
        } else if (value.isZonedDateTime()) {
            return Timestamp.valueOf(value.asZonedDateTime().toLocalDateTime());
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " cannot be cast to timestamp");
        }
    }


    @Override
    public String getString(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).asString();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).asBoolean();
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return (short) getValueByColumnName(columnLabel).asInt();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).asInt();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).asLong();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).asFloat();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).asDouble();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getValueByColumnName(columnLabel).asDecimal().setScale(scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return Date.valueOf(getValueByColumnName(columnLabel).asDate());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return Time.valueOf(getValueByColumnName(columnLabel).asLocalTime());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        ValueWrapper value = getValueByColumnName(columnLabel);
        if (value.isLocalDateTime()) {
            return Timestamp.valueOf(value.asLocalDateTime());
        } else if (value.isZonedDateTime()) {
            return Timestamp.valueOf(value.asZonedDateTime().toLocalDateTime());
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " cannot be cast to timestamp");
        }
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValueByColumnIndex(columnIndex).getValue();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).getValue();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        assertIsOpen();
        int index = resultSet.getColumnNames().indexOf(columnLabel);
        if (index == -1) {
            throw new SQLException("No such column is present");
        }
        return ++index;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getValueByColumnName(columnLabel).asDecimal();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.beforeFirst.get();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.afterLast.get();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return first.get();
    }

    @Override
    public boolean isLast() throws SQLException {
        return !resultSet.hasNext();
    }


    @Override
    public int getRow() throws SQLException {
        return rowNumber;
    }


    @Override
    public void setFetchDirection(int direction) throws SQLException {
        assertIsOpen();
        if (direction != SUPPORTED_FETCH_DIRECTION) {
            throw new SQLException("Only forward fetching is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return SUPPORTED_FETCH_DIRECTION;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }


    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        ValueWrapper value = getValueByColumnIndex(columnIndex);
        if (value.isNull()) {
            return null;
        }
        ZoneId        targetZone = cal.getTimeZone().toZoneId();
        ZonedDateTime zonedDateTime;
        if (value.isDate()) {
            zonedDateTime = value.asDate().atStartOfDay(targetZone);
        } else if (value.isLocalDateTime()) {
            zonedDateTime = value.asLocalDateTime().atZone(targetZone);
        } else if (value.isZonedDateTime()) {
            zonedDateTime = value.asZonedDateTime().withZoneSameInstant(targetZone);
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " can not cast to date");
        }
        return Date.valueOf(zonedDateTime.toLocalDate());
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        ValueWrapper value = getValueByColumnName(columnLabel);
        if (value.isNull()) {
            return null;
        }
        ZoneId        targetZone = cal.getTimeZone().toZoneId();
        ZonedDateTime zonedDateTime;
        if (value.isDate()) {
            zonedDateTime = value.asDate().atStartOfDay(targetZone);
        } else if (value.isLocalDateTime()) {
            zonedDateTime = value.asLocalDateTime().atZone(targetZone);
        } else if (value.isZonedDateTime()) {
            zonedDateTime = value.asZonedDateTime().withZoneSameInstant(targetZone);
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " can not cast to date");
        }
        return Date.valueOf(zonedDateTime.toLocalDate());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        ValueWrapper value = getValueByColumnIndex(columnIndex);
        if (value.isNull()) {
            return null;
        }
        ZoneOffset targetOffset = getZoneOffsetFrom(cal);
        OffsetTime offsetTime;
        if (value.isLocalTime()) {
            offsetTime = value.asLocalTime().atOffset(targetOffset);
        } else if (value.isZonedTime()) {
            offsetTime = value.asZonedTime().withOffsetSameInstant(targetOffset);
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " can not cast to Time");
        }
        return Time.valueOf(offsetTime.toLocalTime());
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        ValueWrapper value        = getValueByColumnName(columnLabel);
        ZoneOffset   targetOffset = getZoneOffsetFrom(cal);
        OffsetTime   offsetTime;
        if (value.isLocalTime()) {
            offsetTime = value.asLocalTime().atOffset(targetOffset);
        } else if (value.isZonedTime()) {
            offsetTime = value.asZonedTime().withOffsetSameInstant(targetOffset);
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " can not cast to Time");
        }
        return Time.valueOf(offsetTime.toLocalTime());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        ValueWrapper value = getValueByColumnIndex(columnIndex);
        if (value.isNull()) {
            return null;
        }
        ZonedDateTime zonedDateTime;
        ZoneId        zoneId = cal.getTimeZone().toZoneId();
        if (value.isLocalDateTime()) {
            zonedDateTime = value.asLocalDateTime().atZone(zoneId);
        } else if (value.isZonedDateTime()) {
            zonedDateTime = value.asZonedDateTime().withZoneSameInstant(zoneId);
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " can not cast to Timestamp");
        }
        return Timestamp.valueOf(zonedDateTime.toLocalDateTime());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        ValueWrapper value = getValueByColumnName(columnLabel);
        if (value.isNull()) {
            return null;
        }
        ZonedDateTime zonedDateTime;
        ZoneId        zoneId = cal.getTimeZone().toZoneId();
        if (value.isLocalDateTime()) {
            zonedDateTime = value.asLocalDateTime().atZone(zoneId);
        } else if (value.isZonedDateTime()) {
            zonedDateTime = value.asZonedDateTime().withZoneSameInstant(zoneId);
        } else {
            throw new SQLException("value type " + value.getDataTypeString() + " can not cast to Timestamp");
        }
        return Timestamp.valueOf(zonedDateTime.toLocalDateTime());
    }


    @Override
    public int getHoldability() throws SQLException {
        return SUPPORTED_HOLDABILITY;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }


    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        } else {
            throw new SQLException("This object does not implement the given interface");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }


    private void assertIsOpen() throws SQLException {
        if (this.closed) {
            throw new SQLException("The result set is closed");
        }
    }


    private void assertColumnIndexIsPresent(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > this.currentRecord.size()) {
            throw new SQLException("Invalid column index value");
        }
    }

    private void assertColumnNameIsPresent(String columnName) throws SQLException {
        if (!this.currentRecord.containsKey(columnName)) {
            throw new SQLException("Invalid column name");
        }
    }


    private static ZoneOffset getZoneOffsetFrom(Calendar cal) {
        Calendar calendar = cal == null ? Calendar.getInstance() : cal;
        return calendar
                .getTimeZone()
                .toZoneId()
                .getRules()
                .getOffset(calendar.toInstant());
    }


    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }


    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }


    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }
}
