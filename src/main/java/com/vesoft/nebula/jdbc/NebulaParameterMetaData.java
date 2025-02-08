/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula5.jdbc;

import com.vesoft.nebula5.jdbc.statement.NebulaPreparedStatementImpl;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class NebulaParameterMetaData implements ParameterMetaData {
    private static NebulaParameterMetaData     nebulaParameterMetaData = null;
    private        NebulaPreparedStatementImpl preparedStatement;

    private NebulaParameterMetaData(NebulaPreparedStatementImpl preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    public static ParameterMetaData getInstance(NebulaPreparedStatementImpl preparedStatement) {
        if (nebulaParameterMetaData == null) {
            nebulaParameterMetaData = new NebulaParameterMetaData(preparedStatement);
        }
        return nebulaParameterMetaData;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return preparedStatement.getParametersNumber();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int getScale(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported.");
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return null;
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        Object parameter =  preparedStatement.getParameters().get(param);
        if(parameter != null){
            return parameter.getClass().getName();
        }
        return String.format("No such param with index [%d]", param);
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
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
}
