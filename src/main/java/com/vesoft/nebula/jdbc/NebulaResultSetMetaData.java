/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.driver.graph.data.Edge;
import com.vesoft.nebula.driver.graph.data.EmbeddingVector;
import com.vesoft.nebula.driver.graph.data.NRecord;
import com.vesoft.nebula.driver.graph.data.Node;
import com.vesoft.nebula.driver.graph.data.Path;
import com.vesoft.nebula.driver.graph.decode.ColumnType;
import com.vesoft.nebula.jdbc.values.NebulaRecord;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

public class NebulaResultSetMetaData implements ResultSetMetaData {

    private final String       schemaName;
    private final String       catalogName;
    private final String       tableName;
    private final List<String> keys;
    private final NebulaRecord firstRecord;

    public NebulaResultSetMetaData(String schemaName, String catalogName, List<String> keys, NebulaRecord firstRecord) {
        this.schemaName = schemaName == null ? "" : schemaName;
        this.catalogName = catalogName == null ? "" : catalogName;
        this.keys = keys;
        this.firstRecord = firstRecord;
        this.tableName = "";
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.keys.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        int adjustedIndex = column - 1;
        return keys.get(adjustedIndex);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return this.schemaName;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return this.tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return this.catalogName;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        if (this.firstRecord == null) {
            return Types.NULL;
        }
        int        adjustedIndex = column - 1;
        ColumnType type          = this.firstRecord.get(adjustedIndex).getDataType();
        return toSqlType(type);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if (this.firstRecord == null) {
            return "";
        }
        int adjustedIndex = column - 1;
        return this.firstRecord.get(adjustedIndex).getDataTypeString();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true; // you cannot write back using nebula ResultSet
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false; // you cannot write back using nebula ResultSet
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false; // you cannot write back using nebula ResultSet
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        if (this.firstRecord == null) {
            return Object.class.getName();
        }
        ColumnType type = this.firstRecord.get(column - 1).getDataType();
        switch (type) {
            case COLUMN_TYPE_BOOL:
                return Boolean.class.getName();
            case COLUMN_TYPE_DURATION:
                return Duration.class.getName();

            case COLUMN_TYPE_STRING:
                return String.class.getName();
            case COLUMN_TYPE_INT8:
            case COLUMN_TYPE_UINT8:
            case COLUMN_TYPE_INT16:
            case COLUMN_TYPE_UINT16:
            case COLUMN_TYPE_INT32:
            case COLUMN_TYPE_UINT32:
                return Integer.class.getName();
            case COLUMN_TYPE_INT64:
            case COLUMN_TYPE_UINT64:
                return Long.class.getName();
            case COLUMN_TYPE_FLOAT32:
                return Float.class.getName();
            case COLUMN_TYPE_FLOAT64:
                return Double.class.getName();
            case COLUMN_TYPE_DATE:
                return LocalDate.class.getName();
            case COLUMN_TYPE_ZONEDTIME:
                return OffsetTime.class.getName();
            case COLUMN_TYPE_LOCALTIME:
                return LocalTime.class.getName();
            case COLUMN_TYPE_LOCALDATETIME:
                return LocalDateTime.class.getName();
            case COLUMN_TYPE_ZONEDDATETIME:
                return OffsetDateTime.class.getName();
            case COLUMN_TYPE_NULL:
                return Object.class.getName();
            case COLUMN_TYPE_DECIMAL:
                return BigDecimal.class.getName();
            case COLUMN_TYPE_NODE:
                return Node.class.getName();
            case COLUMN_TYPE_EDGE:
                return Edge.class.getName();
            case COLUMN_TYPE_PATH:
                return Path.class.getName();
            case COLUMN_TYPE_LIST:
                return List.class.getName();
            case COLUMN_TYPE_RECORD:
                return NRecord.class.getName();
            case COLUMN_TYPE_EMBEDDINGVECTOR:
                return EmbeddingVector.class.getName();
            default:
                throw new SQLException("not support column type: " + type.name());
        }


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

    private int toSqlType(ColumnType type) throws SQLException {
        switch (type) {
            case COLUMN_TYPE_ANY:
            case COLUMN_TYPE_DURATION:
                return Types.OTHER;
            case COLUMN_TYPE_BOOL:
                return Types.BOOLEAN;
            case COLUMN_TYPE_STRING:
                return Types.VARCHAR;
            case COLUMN_TYPE_INT8:
            case COLUMN_TYPE_UINT8:
            case COLUMN_TYPE_INT16:
            case COLUMN_TYPE_UINT16:
            case COLUMN_TYPE_INT32:
            case COLUMN_TYPE_UINT32:
            case COLUMN_TYPE_INT64:
            case COLUMN_TYPE_UINT64:
                return Types.INTEGER;
            case COLUMN_TYPE_FLOAT32:
                return Types.FLOAT;
            case COLUMN_TYPE_FLOAT64:
                return Types.DOUBLE;
            case COLUMN_TYPE_DATE:
                return Types.DATE;
            case COLUMN_TYPE_ZONEDTIME:
                return Types.TIME;
            case COLUMN_TYPE_LOCALTIME:
            case COLUMN_TYPE_LOCALDATETIME:
            case COLUMN_TYPE_ZONEDDATETIME:
                return Types.TIMESTAMP;
            case COLUMN_TYPE_NULL:
                return Types.NULL;
            case COLUMN_TYPE_DECIMAL:
                return Types.DECIMAL;
            case COLUMN_TYPE_NODE:
            case COLUMN_TYPE_EDGE:
            case COLUMN_TYPE_PATH:
            case COLUMN_TYPE_LIST:
            case COLUMN_TYPE_RECORD:
            case COLUMN_TYPE_EMBEDDINGVECTOR:
                return Types.STRUCT;
            default:
                throw new SQLException("not support column type: " + type.name());

        }
    }
}
