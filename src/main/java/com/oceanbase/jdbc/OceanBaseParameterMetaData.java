/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

/** Very basic info about the parameterized query, only reliable method is getParameterCount(). */
public class OceanBaseParameterMetaData implements ParameterMetaData {

    private final ColumnDefinition[] parametersInformation;
    private int parameterCount;
    private boolean returnSimpleMetadata;

    public OceanBaseParameterMetaData(ColumnDefinition[] parametersInformation) {
        this.parametersInformation = parametersInformation;
    }

    public OceanBaseParameterMetaData(ColumnDefinition[] parametersInformation, int parameterCount, boolean returnSimpleMetadata) {
        this.parametersInformation = parametersInformation;
        this.parameterCount = parameterCount;
        this.returnSimpleMetadata = returnSimpleMetadata;
    }

    private void checkAvailable() throws SQLException {
        if (this.parametersInformation == null) {
            throw new SQLException("Parameter metadata not available for these statement", "S1C00");
        }
    }

    @Override
    public int getParameterCount() throws SQLException {
        if (parametersInformation == null) {
            return parameterCount;
        }
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
        if (returnSimpleMetadata) {
            checkBounds(param);
            return false;
        }
        return getParameterInformation(param).isSigned();
    }
    @Override
    public int getPrecision(int param) throws SQLException {
        if (returnSimpleMetadata) {
            checkBounds(param);
            return 0;
        }
        long length = getParameterInformation(param).getLength();
        return (length > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) length;
    }
    @Override
    public int getScale(int param) throws SQLException {
        if (returnSimpleMetadata) {
            checkBounds(param);
            return 0;
        }
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
        if (returnSimpleMetadata) {
            checkBounds(param);
            return Types.VARCHAR;
        }
        //todo oracle mode temporarily maintain the original state , further research is needed
        if (getParameterInformation(param).isOracleMode()) {
            throw ExceptionFactory.INSTANCE
                    .notSupported("Getting parameter type metadata are not supported");
        }
        return getParameterInformation(param).getColumnType().getType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        if (returnSimpleMetadata) {
            checkBounds(param);
            return "VARCHAR";
        }
        return getParameterInformation(param).getColumnType().getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        if (returnSimpleMetadata) {
            checkBounds(param);
            return "java.lang.String";
        }
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
                "The receiver is not a wrapper and does not implement the interface " + iface.getName());
        }
    }

    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    public void checkBounds(int paramNumber) throws SQLException {
        if (paramNumber < 1) {
            throw new SQLException(
                    "Parameter index of '" + paramNumber + "' is invalid.");
        }
        if (paramNumber > this.parameterCount) {
            throw new SQLException(
                    "Parameter index of '" + paramNumber + "' is greater than number of parameters, which is '" + this.parameterCount + "'.");
        }
    }
}
