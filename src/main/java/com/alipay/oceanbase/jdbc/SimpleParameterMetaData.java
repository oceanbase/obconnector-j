package com.alipay.oceanbase.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

/** Very basic info about the parameterized query, only reliable method is getParameterCount(). */
public class SimpleParameterMetaData implements ParameterMetaData {

    private final int parameterCount;

    public SimpleParameterMetaData(int parameterCount) {
        this.parameterCount = parameterCount;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    @Override
    public int isNullable(final int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Parameter metadata out of range : param was %s and must be in range 1 - %s",
                param, parameterCount), "07009");
        }
        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Parameter metadata out of range : param was %s and must be in range 1 - %s",
                param, parameterCount), "07009");
        }
        return true;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Parameter metadata out of range : param was %s and must be in range 1 - %s",
                param, parameterCount), "07009");
        }
        throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata precision");
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Parameter metadata out of range : param was %s and must be in range 1 - %s",
                param, parameterCount), "07009");
        }
        throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata scale");
    }

    /**
     * Parameter type are not sent by server. See https://jira.mariadb.org/browse/CONJ-568 and
     * https://jira.mariadb.org/browse/MDEV-15031
     *
     * @param param parameter number
     * @return SQL type from java.sql.Types
     * @throws SQLException a feature not supported, since server doesn't sent the right information
     */
    @Override
    public int getParameterType(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Parameter metadata out of range : param was %s and must be in range 1 - %s",
                param, parameterCount), "07009");
        }
        throw ExceptionFactory.INSTANCE
            .notSupported("Getting parameter type metadata are not supported");
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Parameter metadata out of range : param was %s and must be in range 1 - %s",
                param, parameterCount), "07009");
        }
        throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata type name");
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Parameter metadata out of range : param was %s and must be in range 1 - %s",
                param, parameterCount), "07009");
        }
        throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata class name");
    }

    @Override
    public int getParameterMode(int param) {
        return parameterModeIn;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException("The receiver is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException(
                "The receiver is not a wrapper and does not implement the interface");
        }
    }

    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
