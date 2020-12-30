package com.alipay.oceanbase.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

/** Very basic info about the parameterized query, only reliable method is getParameterCount(). */
public class OceanBaseParameterMetaData implements ParameterMetaData {

    private final ColumnDefinition[] parametersInformation;

    public OceanBaseParameterMetaData(ColumnDefinition[] parametersInformation) {
        this.parametersInformation = parametersInformation;
    }

    private void checkAvailable() throws SQLException {
        if (this.parametersInformation == null) {
            throw new SQLException("Parameter metadata not available for these statement", "S1C00");
        }
    }

    @Override
    public int getParameterCount() throws SQLException {
        checkAvailable();
        return parametersInformation.length;
    }

    private ColumnDefinition getParameterInformation(int param) throws SQLException {
        checkAvailable();
        if (param >= 1 && param <= parametersInformation.length) {
            return parametersInformation[param - 1];
        }
        throw new SQLException("Parameter metadata out of range : param was " + param
                               + " and must be 1 <= param <=" + parametersInformation.length,
            "07009");
    }

    @Override
    public int isNullable(final int param) throws SQLException {
        if (getParameterInformation(param).isNotNull()) {
            return ParameterMetaData.parameterNoNulls;
        } else {
            return ParameterMetaData.parameterNullable;
        }
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return getParameterInformation(param).isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        long length = getParameterInformation(param).getLength();
        return (length > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) length;
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (ColumnType.isNumeric(getParameterInformation(param).getColumnType())) {
            return getParameterInformation(param).getDecimals();
        }
        return 0;
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
        throw ExceptionFactory.INSTANCE
            .notSupported("Getting parameter type metadata are not supported");
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getParameterInformation(param).getColumnType().getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return getParameterInformation(param).getColumnType().getClassName();
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
